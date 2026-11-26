import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { expect, it } from "vitest";
import { launchVerificationServer, runControlOmb } from "../scripts/control-omb.ts";
import { readHandoffs } from "./testing/handoff-rows.ts";

it("stops at 150 steps, persists a checkpoint, and resumes the same task in a fresh provider session", async () => {
  const scratchDir = mkdtempSync(join(tmpdir(), "omb-step-cap-e2e-"));
  const launchLog = join(scratchDir, "claude-launches.jsonl");
  mkdirSync(scratchDir, { recursive: true });
  const fixture = await launchVerificationServer({
    ...process.env,
    FAKE_CLAUDE_MODE: "step-cap-recovery",
    FAKE_CLAUDE_STEP_COUNT: "160",
    FAKE_CLAUDE_LAUNCH_LOG: launchLog,
  });
  const { url, dataDir } = fixture.info;
  const api = async (method: string, path: string) => {
    const response = await fetch(url + path, {
      method,
      headers: { "content-type": "application/json", origin: url },
      signal: AbortSignal.timeout(30_000),
    });
    const body = await response.json() as any;
    expect(response.status, `${method} ${path}: ${JSON.stringify(body)}`).toBe(200);
    return body;
  };
  try {
    const { bot } = await runControlOmb(["new-bot", "--name", "Cap Recovery Bot", "--url", url]) as any;
    const threadId = bot.activeTaskId as string;
    const taskPrompt = "Complete the 160-step repository migration and report back.";
    await runControlOmb(["send", "--bot", bot.id, "--task", threadId, "--text", taskPrompt, "--url", url]);

    // Wait for both Turn 1 (capped at 150 steps) and automatic fresh-session continuation Turn 2 to finish.
    await expect.poll(async () => {
      const rows = readHandoffs(dataDir).filter((a) => a.threadId === threadId);
      return rows.at(-1)?.status;
    }, { timeout: 45_000, interval: 100 }).toBe("completed");

    await expect.poll(async () => {
      const { bots } = await api("GET", "/api/bots");
      const task = bots.find((b: any) => b.id === bot.id)?.tasks.find((t: any) => t.threadId === threadId);
      return task?.busy;
    }, { timeout: 20_000, interval: 100 }).toBe(false);

    // 1. Readback of checkpoint artifact on disk
    const handoffDir = join(dataDir, "handoffs");
    const handoffFiles = existsSync(handoffDir) ? readdirSync(handoffDir).filter((name) => name.endsWith(".md")) : [];
    expect(handoffFiles).toHaveLength(1);
    const checkpointPath = join(handoffDir, handoffFiles[0]!);
    const checkpointContent = readFileSync(checkpointPath, "utf8");
    expect(checkpointContent).toContain("Steps used: 150");
    expect(checkpointContent).toContain(taskPrompt);
    expect(checkpointContent).toContain("echo work-150");
    expect(checkpointContent).not.toContain("echo work-151");

    // 2. Readback of distinct provider session identities for the two turns
    const launches = readFileSync(launchLog, "utf8")
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line) as { sessionId: string; resumed: boolean; hasSessionIdFlag: boolean; prompt: string });
    expect(launches).toHaveLength(2);
    const [turn1, turn2] = launches;
    expect(turn1!.resumed).toBe(false);
    expect(turn1!.hasSessionIdFlag).toBe(true);
    expect(turn2!.resumed).toBe(false);
    expect(turn2!.hasSessionIdFlag).toBe(true);
    expect(turn2!.sessionId).not.toBe(turn1!.sessionId);
    expect(turn2!.prompt).toContain(checkpointPath);
    expect(turn2!.prompt).toContain(taskPrompt);

    // 3. Readback of the same task & root assignment completed on the hub
    const rootAssignments = readHandoffs(dataDir).filter((a) => a.threadId === threadId);
    expect(rootAssignments).toHaveLength(1);
    const root = rootAssignments[0]!;
    expect(root.executions).toBe(2);
    expect(root.status).toBe("completed");
    const db = new DatabaseSync(join(dataDir, "runtime.db"), { readOnly: true });
    let rootEvents: string[] = [];
    try {
      const rows = db.prepare("SELECT kind FROM assignment_events WHERE assignment_id = ? ORDER BY id").all(root.id) as Array<{ kind: string }>;
      rootEvents = rows.map((e) => e.kind);
    } finally {
      db.close();
    }
    expect(rootEvents).toContain("continued");
    expect(rootEvents).toContain("settled");

    // 4. Readback of persisted thread messages and updated session cursor in bots.json
    await expect.poll(() => {
      const botsFile = join(dataDir, "bots.json");
      if (!existsSync(botsFile)) return undefined;
      const saved = JSON.parse(readFileSync(botsFile, "utf8")) as any[];
      const savedBot = saved.find((b: any) => b.id === bot.id);
      const savedTask = savedBot?.tasks?.find((t: any) => t.threadId === threadId);
      const cursorId = savedTask?.lastInstanceId ?? bot.modelSelection.instanceId;
      return savedTask?.resumeCursors?.[cursorId];
    }, { timeout: 10_000, interval: 50 }).toBe(turn2!.sessionId);
    const { messages } = await api("GET", `/api/threads/${threadId}/messages?limit=400`);
    const finalReply = [...messages].reverse().find((m: any) => m.role === "bot" && m.kind === "text");
    expect(finalReply?.text).toContain(`Resumed in fresh session ${turn2!.sessionId} from ${checkpointPath} (steps150=true)`);
  } finally {
    await fixture.close();
    rmSync(scratchDir, { recursive: true, force: true });
  }
}, 60_000);

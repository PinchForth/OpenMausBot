import { writeFileSync } from "node:fs";
import { readUsage } from "./usage-ledger.ts";
import { daysBetween, readSpans } from "./usage-trace/spans.ts";
import { join } from "node:path";
import { expect, it } from "vitest";
import { launchVerificationServer, runControlOmb } from "../scripts/control-omb.ts";
import { request } from "../scripts/mcp-server.ts";
import type { TraceDetail, TraceList, TraceNode, TraceTool, TraceTurn } from "../shared/usage-trace.ts";

const turnsOf = (node: TraceNode): TraceTurn[] => node.kind === "turn" ? [node, ...node.children.flatMap(turnsOf)] : [];
const toolsOf = (turn: TraceTurn): TraceTool[] => turn.children.flatMap((child) =>
  child.kind === "tool" ? [child] : child.kind === "group" ? child.children.filter((inner): inner is TraceTool => inner.kind === "tool") : []);

it("traces a person's message to the Chief through the teammate it delegated to and the tools that teammate used", async () => {
  const session = await launchVerificationServer({ ...process.env }, undefined, undefined, undefined, undefined, { scripted: true });
  const cli = (...args: string[]) => runControlOmb(args, { env: { OPENMAUSBOT_URL: session.info.url } }) as Promise<any>;
  const api = (path: string, body?: unknown, method = "POST") => request(path, body === undefined ? {} : { method, body: JSON.stringify(body) }, session.info.url) as Promise<any>;
  try {
    const chief = (await cli("new-bot", "--name", "Clive", "--section", "Leadership")).bot;
    const researcher = (await cli("new-bot", "--name", "Researcher", "--section", "Engineering")).bot;
    await api(`/api/bots/${chief.id}`, { chiefOfStaff: true, managedSections: ["Engineering"], acknowledgePeerScope: true }, "PATCH");
    writeFileSync(join(session.info.dataDir, "room-plan.json"), JSON.stringify({
      [chief.id]: { steps: [{ arguments: { bot_ids: [researcher.id], message: "Compare the pricing pages" } }], reply: "Assigned", resumeReply: "Pricing compared" },
      [researcher.id]: { steps: [{ tool: "list_bots", arguments: {} }, { tool: "list_rooms", arguments: {} }], reply: "Two pages compared" },
    }));
    await cli("send", "--bot", chief.id, "--task", chief.activeTaskId, "--text", "Launch the new pricing page by Friday");
    expect((await cli("wait", "--bot", chief.id, "--task", chief.activeTaskId, "--timeout", "30")).status).toBe("settled");
    const today = new Date().toISOString().slice(0, 10);
    const settled = async () => {
      const list: TraceList = await api(`/api/usage/traces?from=${today}&to=${today}`, undefined, "GET");
      // A teammate's reply can cost its requester a one-word check (runtime/status-reply.ts), booked like any side job as a root
      // of its own: the person's message is the one root they started.
      const own = list.roots.filter((one) => one.source === "owner");
      const root = own.length === 1 && !own[0]!.running ? own[0]! : null;
      if (!root) return null;
      const detail: TraceDetail = await api(`/api/usage/traces/${encodeURIComponent(root.id)}?at=${root.startedAt}`, undefined, "GET");
      const turns = turnsOf(detail.turns[0]!);
      // Tool spans are written in batches, so the trace counts as settled once the teammate's last tool is in too.
      const research = turns.find((turn) => turn.botId === researcher.id);
      return turns.length === 3 && research && toolsOf(research).length === 3 ? { list, root, detail } : null;
    };
    await expect.poll(settled, { timeout: 20_000, interval: 250 }).toBeTruthy();
    const { list, root, detail } = (await settled())!;
    expect(root).toMatchObject({
      title: "Launch the new pricing page by Friday", source: "owner", timed: true, running: false, errors: 0,
      bots: [{ id: chief.id, name: "Clive" }, { id: researcher.id, name: "Researcher" }],
    });
    for (const other of list.roots.filter((one) => one.id !== root.id)) expect(other).toMatchObject({ source: "bot", tools: 0, bots: [{ id: chief.id, name: "Clive" }] });
    const sum = (key: "tools" | "steps" | "input" | "output") => list.roots.reduce((total, one) => total + one[key], 0);
    expect(list.totals).toMatchObject({ tools: sum("tools"), steps: sum("steps"), input: sum("input"), output: sum("output") });

    const [first] = detail.turns;
    expect(first).toMatchObject({ kind: "turn", botId: chief.id, threadId: chief.activeTaskId, timed: true, status: "ok" });
    const nested = first!.children.filter((child): child is TraceTurn => child.kind === "turn");
    expect(nested.map((turn) => turn.botId).sort()).toEqual([chief.id, researcher.id].sort());
    const research = nested.find((turn) => turn.botId === researcher.id)!;
    expect(research.threadId).not.toBe(researcher.activeTaskId);
    expect(toolsOf(research).map((tool) => tool.name)).toEqual(["list_room_targets", "list_bots", "list_rooms"]);
    for (const tool of toolsOf(research)) {
      expect(tool).toMatchObject({ timedBy: "omb", status: "ok", icon: "bots" });
      expect(tool.endedAt! - tool.startedAt).toBeGreaterThanOrEqual(0);
      expect(tool.startedAt).toBeGreaterThanOrEqual(research.startedAt);
      expect(tool.endedAt!).toBeLessThanOrEqual(research.endedAt!);
    }
    expect(toolsOf(first!).map((tool) => tool.name)).toEqual(["list_room_targets", "coordinate_bots"]);
    const all = turnsOf(first!);
    expect(all).toHaveLength(3);
    const toolTotal = all.reduce((sum, turn) => sum + toolsOf(turn).length, 0);
    expect(root.tools).toBe(toolTotal);
    expect(root.input).toBe(all.reduce((sum, turn) => sum + turn.tokens.input, 0));
    expect(root.output).toBe(all.reduce((sum, turn) => sum + turn.tokens.output, 0));
    expect(root.durationMs).toBe(Math.max(...all.map((turn) => turn.endedAt!)) - first!.startedAt);
    const byResearcher = await api(`/api/usage/traces/${encodeURIComponent(`${research.threadId}~${research.turnId}`)}?at=${research.startedAt}`, undefined, "GET");
    expect(byResearcher.root.id).toBe(root.id);
  } finally {
    await session.close();
  }
}, 90_000);

it("persists tool name and first sanitized error line in traces, spans, and usage ledger on failed tool steps", async () => {
  const secret = "sk-ant-live-secret9876543210abcd";
  const toolCalls = JSON.stringify([
    {
      name: "Bash",
      input: { command: "curl https://api.example.test/v1/fail" },
      ok: false,
      output: {
        ok: false,
        output: `Authorization failed with Bearer ${secret}\nstack line 2 should not persist`,
        exitCode: 22,
      },
    },
  ]);
  const session = await launchVerificationServer({ ...process.env, FAKE_CLAUDE_TOOL_CALLS: toolCalls });
  const cli = (...args: string[]) => runControlOmb(args, { env: { OPENMAUSBOT_URL: session.info.url } }) as Promise<any>;
  const api = (path: string, body?: unknown, method = "POST") => request(path, body === undefined ? {} : { method, body: JSON.stringify(body) }, session.info.url) as Promise<any>;
  try {
    const bot = (await cli("new-bot", "--name", "Fail Tracer")).bot;
    await cli("send", "--bot", bot.id, "--task", bot.activeTaskId, "--text", "Run the failing curl check");
    expect((await cli("wait", "--bot", bot.id, "--task", bot.activeTaskId, "--timeout", "30")).status).toBe("settled");

    const today = new Date().toISOString().slice(0, 10);
    const settled = async () => {
      const list: TraceList = await api(`/api/usage/traces?from=${today}&to=${today}`, undefined, "GET");
      const root = list.roots.find((one) => one.source === "owner" && !one.running);
      if (!root) return null;
      const detail: TraceDetail = await api(`/api/usage/traces/${encodeURIComponent(root.id)}?at=${root.startedAt}`, undefined, "GET");
      const tools = toolsOf(detail.turns[0]!);
      return tools.length === 1 && tools[0]!.status === "error" ? { root, detail, tools } : null;
    };
    await expect.poll(settled, { timeout: 20_000, interval: 250 }).toBeTruthy();
    const { detail, tools } = (await settled())!;
    const failedTool = tools[0]!;
    expect(failedTool.name).toBe("Bash");
    expect(failedTool.status).toBe("error");
    expect(failedTool.error).toContain("Authorization failed with Bearer «redacted");
    expect(failedTool.error).not.toContain(secret);
    expect(failedTool.error).not.toContain("stack line 2");
    expect(failedTool.error).not.toBe("{");

    // Readback directly from usage/<YYYY-MM>.jsonl and usage/spans/<YYYY-MM-DD>.jsonl
    const rows = readUsage(session.info.dataDir, { from: new Date(Date.now() - 86_400_000), to: new Date(Date.now() + 86_400_000) });
    const row = rows.find((r) => r.turnId === detail.turns[0]!.turnId)!;
    expect(row.toolErrors).toBe(1);
    expect(row.failedSteps).toEqual([
      { tool: "Bash", error: expect.stringContaining("Authorization failed with Bearer «redacted") },
    ]);
    expect(row.failedSteps![0]!.error).not.toContain(secret);

    const turnKey = `${detail.turns[0]!.threadId}:${detail.turns[0]!.turnId}`;
    const spans = (await readSpans(session.info.dataDir, daysBetween(detail.root.startedAt, Date.now()), new Set([turnKey]))).get(turnKey)!;
    const spanTool = spans.find((s) => s.kind === "tool")!;
    expect(spanTool.kind === "tool" && spanTool).toMatchObject({
      name: "Bash",
      status: "error",
      error: row.failedSteps![0]!.error,
    });
  } finally {
    await session.close();
  }
}, 60_000);

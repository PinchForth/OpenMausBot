import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";

import { removeTempDir } from "../testing/cleanup.ts";

const HELPER = join(dirname(fileURLToPath(import.meta.url)), "omb-hook.ts");
const dir = mkdtempSync(join(tmpdir(), "omb-hook-"));
const tokenFile = join(dir, "token");
writeFileSync(tokenFile, "secret-token-123\n", { mode: 0o600 });
const servers: Server[] = [];
afterAll(async () => {
  for (const s of servers) s.close();
  await removeTempDir(dir);
});

function run(stdin: string, env: Record<string, string>): Promise<{ code: number | null; stdout: string; stderr: string; ms: number }> {
  return new Promise((resolve) => {
    const started = Date.now();
    const child = spawn(process.execPath, [HELPER], { env: { ...process.env, ...env }, stdio: ["pipe", "pipe", "pipe"] });
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (c) => (stdout += c));
    child.stderr.on("data", (c) => (stderr += c));
    // Oversized input is intentionally rejected before stdin is drained.
    child.stdin.on("error", () => {});
    child.on("close", (code) => resolve({ code, stdout, stderr, ms: Date.now() - started }));
    child.stdin.end(stdin);
  });
}

function listen(handler: Parameters<typeof createServer>[1]): Promise<string> {
  return new Promise((resolve) => {
    const server = createServer(handler);
    servers.push(server);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address() as { port: number };
      resolve(`http://127.0.0.1:${address.port}`);
    });
  });
}

describe("omb-hook helper", () => {
  it.each(["context", "hookSpecificOutput"])("flushes large %s replies before exiting", async (field) => {
    const context = "A long restored note 🐭\n".repeat(20_000);
    const body = field === "context" ? { context } : { hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: context } };
    const url = await listen((_req, res) => res.end(JSON.stringify(body)));
    const result = await run(JSON.stringify({ hook_event_name: "SessionStart" }), { OMB_HOOK_URL: url, OMB_HOOK_TOKEN_FILE: tokenFile });
    expect(result.code).toBe(0);
    expect(result.stdout).toBe(field === "context" ? context : JSON.stringify(body));
    expect(result.stderr).toBe("");
  });

  it("does not forward oversized hook input", async () => {
    let requests = 0;
    const url = await listen((_req, res) => { requests++; res.end("{}"); });
    const result = await run(JSON.stringify({ hook_event_name: "PostToolUse", tool_response: "x".repeat(1024 * 1024) }), { OMB_HOOK_URL: url, OMB_HOOK_TOKEN_FILE: tokenFile });
    expect(result.code).toBe(0);
    expect(result.stdout).toBe("");
    expect(requests).toBe(0);
  });

  it("does not follow redirects with a private hook payload", async () => {
    let forwarded = 0;
    const destination = await listen((_req, res) => { forwarded++; res.end("{}"); });
    const url = await listen((_req, res) => { res.writeHead(307, { location: destination }); res.end(); });
    const result = await run(JSON.stringify({ hook_event_name: "PostToolUse", tool_response: "private output" }), { OMB_HOOK_URL: url, OMB_HOOK_TOKEN_FILE: tokenFile });
    expect(result.code).toBe(0);
    expect(result.stdout).toBe("");
    expect(forwarded).toBe(0);
  });

  it("exits 0 quietly on garbage input and no harness", async () => {
    const r = run("not json at all", { OMB_HOOK_URL: "http://127.0.0.1:1", OMB_HOOK_TOKEN_FILE: tokenFile });
    const result = await r;
    expect(result.code).toBe(0);
    expect(result.stdout).toBe("");
  }, 15_000);

  it("posts the event with the bearer from the token file and prints the harness's hookSpecificOutput", async () => {
    const seen: Array<{ auth: string | undefined; body: unknown }> = [];
    const url = await listen((req, res) => {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        seen.push({ auth: req.headers.authorization, body: JSON.parse(body) });
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ ok: true, hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: "remember the digest" } }));
      });
    });
    const payload = { hook_event_name: "PostToolUse", session_id: "s1", tool_name: "Bash", tool_use_id: "tu-1", tool_input: { command: "ls" }, tool_response: "a\nb" };
    const result = await run(JSON.stringify(payload), { OMB_HOOK_URL: url, OMB_HOOK_TOKEN_FILE: tokenFile });
    expect(result.code).toBe(0);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.auth).toBe("Bearer secret-token-123");
    expect(seen[0]!.body).toEqual({ event: "PostToolUse", payload });
    expect(JSON.parse(result.stdout)).toEqual({ hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: "remember the digest" } });
  }, 15_000);

  it("gives up within its budget when the harness never answers, still exiting 0", async () => {
    const url = await listen(() => { /* never respond */ });
    const result = await run(JSON.stringify({ hook_event_name: "Stop" }), { OMB_HOOK_URL: url, OMB_HOOK_TOKEN_FILE: tokenFile, OMB_HOOK_TIMEOUT_MS: "800" });
    expect(result.code).toBe(0);
    expect(result.stdout).toBe("");
    expect(result.ms).toBeLessThan(5_000);
  }, 15_000);

  it("prints nothing when the harness returns no hookSpecificOutput", async () => {
    const url = await listen((_req, res) => { res.setHeader("content-type", "application/json"); res.end(JSON.stringify({ ok: true })); });
    const result = await run(JSON.stringify({ hook_event_name: "PostToolUse", tool_use_id: "x" }), { OMB_HOOK_URL: url, OMB_HOOK_TOKEN_FILE: tokenFile });
    expect(result.code).toBe(0);
    expect(result.stdout).toBe("");
  }, 15_000);
});

describe("omb-hook helper: SessionStart context", () => {
  it("prints the harness's plain-text context on SessionStart, because Claude Code reads stdout as context there", async () => {
    const url = await listen((_req, res) => {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ ok: true, context: "[What Dev did in an earlier turn: tools: Bash ×3 · files: changed retry.ts]" }));
    });
    const result = await run(JSON.stringify({ hook_event_name: "SessionStart", source: "compact", session_id: "s1" }), { OMB_HOOK_URL: url, OMB_HOOK_TOKEN_FILE: tokenFile });
    expect(result.code).toBe(0);
    expect(result.stdout).toBe("[What Dev did in an earlier turn: tools: Bash ×3 · files: changed retry.ts]");
  }, 15_000);

  it("ignores a context field on events where stdout is not context", async () => {
    const url = await listen((_req, res) => {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ ok: true, context: "should not be printed" }));
    });
    const result = await run(JSON.stringify({ hook_event_name: "PostToolUse", tool_use_id: "t" }), { OMB_HOOK_URL: url, OMB_HOOK_TOKEN_FILE: tokenFile });
    expect(result.code).toBe(0);
    expect(result.stdout).toBe("");
  }, 15_000);
});

describe("required native pre-effect transport (simulated issuer, not qualification)", () => {
  const bot = "11111111-1111-4111-8111-111111111111";
  const thread = "source-task-thread";
  const scope = "c".repeat(64);
  const payload = { hook_event_name: "PreToolUse", session_id: "native-source-session", tool_use_id: "call-source-1", tool_name: "mcp__agents__propose_profile", tool_input: { changes: { soul: "original retained" } } };
  const required = { OMB_CANONICAL_TASK_GUARD_REQUIRED: "1", OMB_HOOK_TOKEN_FILE: tokenFile,
    OMB_HOOK_BOT_ID: bot, OMB_HOOK_THREAD_ID: thread, OMB_HOOK_TASK_SCOPE_SHA256: scope };
  const action = JSON.stringify({ event: "PreToolUse", native_session: payload.session_id,
    tool_input: { changes: { soul: "original retained" } }, tool_name: payload.tool_name });
  function receipt(changes: Record<string, unknown> = {}) {
    return { hookSpecificOutput: { hookEventName: "PreToolUse", permissionDecision: "allow" }, canonicalTaskAdmission: {
      authorised: true, bot_id: bot, thread_id: thread, scope_sha256: scope, native_session: payload.session_id,
      action_sha256: createHash("sha256").update(action).digest("hex"), task_id: "22222222-2222-4222-8222-222222222222",
      work_id: "work-1234567890abcdef", journal: "history/journal/2026-10-29-mira-source-fixture.md",
      admission_id: "a".repeat(64), commit_sha: "b".repeat(40), tool_use_id: payload.tool_use_id,
      issued_at: new Date(Date.now()-1000).toISOString(), expires_at: new Date(Date.now()+5000).toISOString(), ...changes } };
  }

  it("refuses a missing harness rather than treating transport absence as admission", async () => {
    const result=await run(JSON.stringify(payload),{...required,OMB_HOOK_URL:"http://127.0.0.1:1"});
    expect(result.code).toBe(2);expect(JSON.parse(result.stdout).hookSpecificOutput.permissionDecision).toBe("deny");
    expect(result.stderr).not.toContain("secret-token-123");
  });

  it("refuses a legacy observation success without a canonical permit", async () => {
    const url=await listen((_req,res)=>res.end(JSON.stringify({ok:true,hookSpecificOutput:{hookEventName:"PreToolUse",permissionDecision:"allow"}})));
    const result=await run(JSON.stringify(payload),{...required,OMB_HOOK_URL:url});
    expect(result.code).toBe(2);
  });

  it.each([
    {bot_id:"33333333-3333-4333-8333-333333333333"},{thread_id:"foreign"},{scope_sha256:"d".repeat(64)},
    {native_session:"foreign"},{action_sha256:"e".repeat(64)},{tool_use_id:"previous-call"},{authorised:false},
    {work_id:"work-foreign"},{commit_sha:""},{expires_at:"2000-01-01T00:00:00.000Z"},
    {issued_at:"2100-01-01T00:00:00.000Z"},
  ])("refuses a mismatched or stale issuer record %j", async changes => {
    const url=await listen((_req,res)=>res.end(JSON.stringify(receipt(changes))));
    expect((await run(JSON.stringify(payload),{...required,OMB_HOOK_URL:url})).code).toBe(2);
  });

  it("forwards a matching simulated receipt without disclosing it to the model", async () => {
    const body=receipt();const url=await listen((_req,res)=>res.end(JSON.stringify(body)));
    const result=await run(JSON.stringify(payload),{...required,OMB_HOOK_URL:url});
    expect(result.code).toBe(0);expect(JSON.parse(result.stdout)).toEqual({hookSpecificOutput:body.hookSpecificOutput});
    expect(result.stdout).not.toContain("work-1234567890abcdef");
  });

  it.each(["garbage",JSON.stringify({hook_event_name:"PreToolUse",tool_input:"x".repeat(1024*1024)})])("refuses malformed or oversized required input", async input => {
    expect((await run(input,{...required,OMB_HOOK_URL:"http://127.0.0.1:1"})).code).toBe(2);
  });

  it("refuses required timeout within the finite budget", async () => {
    const url=await listen(()=>{});
    const result=await run(JSON.stringify(payload),{...required,OMB_HOOK_URL:url,OMB_HOOK_TIMEOUT_MS:"400"});
    expect(result.code).toBe(2);expect(result.ms).toBeLessThan(5000);
  });

  it("refuses issuer changes to the action whose admission was checked", async () => {
    const body = receipt();
    const url = await listen((_req, res) => res.end(JSON.stringify({ ...body, hookSpecificOutput: {
      ...body.hookSpecificOutput, updatedInput: { changes: { soul: "different action" } },
    } })));
    expect((await run(JSON.stringify(payload), { ...required, OMB_HOOK_URL: url })).code).toBe(2);
  });

  it.each(["not-json", "[]", "null"])("refuses malformed issuer output %s", async body => {
    const url = await listen((_req, res) => res.end(body));
    expect((await run(JSON.stringify(payload), { ...required, OMB_HOOK_URL: url })).code).toBe(2);
  });

  it("refuses required redirects without forwarding the private action", async () => {
    let forwarded = 0;
    const destination = await listen((_req, res) => { forwarded++; res.end(JSON.stringify(receipt())); });
    const url = await listen((_req, res) => { res.writeHead(307, { location: destination }); res.end(); });
    expect((await run(JSON.stringify(payload), { ...required, OMB_HOOK_URL: url })).code).toBe(2);
    expect(forwarded).toBe(0);
  });

  it("refuses a missing bearer without sending the private action", async () => {
    let requests = 0;
    const url = await listen((_req, res) => { requests++; res.end(JSON.stringify(receipt())); });
    expect((await run(JSON.stringify(payload), { ...required, OMB_HOOK_URL: url, OMB_HOOK_TOKEN_FILE: join(dir, "absent") })).code).toBe(2);
    expect(requests).toBe(0);
  });

  it("preserves observation behavior when the required runtime emits a different hook", async () => {
    const result=await run(JSON.stringify({hook_event_name:"PostToolUse"}),{...required,OMB_HOOK_URL:"http://127.0.0.1:1"});
    expect(result.code).toBe(0);expect(result.stdout).toBe("");
  });
});

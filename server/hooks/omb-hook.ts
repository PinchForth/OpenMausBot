// Native hook stdout is a protocol channel. Required admission never inherits observation-hook fail-open behavior.
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";

const CONTEXT_EVENTS = new Set(["SessionStart", "UserPromptSubmit"]);
const requiredMode = process.env.OMB_CANONICAL_TASK_GUARD_REQUIRED === "1";
let strict = requiredMode;
const requestedBudget = Number(process.env.OMB_HOOK_TIMEOUT_MS);
const budgetMs = Number.isFinite(requestedBudget) && requestedBudget > 0 ? Math.min(requestedBudget, 4_000) : 4_000;
const MAX_INPUT_BYTES = 1024 * 1024;
const url = process.env.OMB_HOOK_URL ?? "";
const tokenFile = process.env.OMB_HOOK_TOKEN_FILE ?? "";

const done = (out?: string, admitted = false) => {
  const refused = strict && !admitted;
  const code = refused ? 2 : 0;
  if (refused) {
    process.stderr.write("Canonical task admission was not established; this effect must remain held.\n");
    out = JSON.stringify({ hookSpecificOutput: { hookEventName: "PreToolUse", permissionDecision: "deny",
      permissionDecisionReason: "Recover this task's own canonical account before retrying the original effect." } });
  }
  if (out === undefined) process.exit(code);
  process.stdout.write(out, () => process.exit(code));
};
process.stdout.on("error", () => process.exit(strict ? 2 : 0));
const fuse = setTimeout(() => done(), budgetMs);
fuse.unref();

function readStdin(): Promise<string> {
  return new Promise((resolve) => {
    let data = "";
    let bytes = 0;
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk: string) => {
      bytes += Buffer.byteLength(chunk);
      if (bytes > MAX_INPUT_BYTES) return done();
      data += chunk;
    });
    process.stdin.on("end", () => resolve(data));
    process.stdin.on("error", () => resolve(data));
  });
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`).join(",")}}`;
  }
  const encoded = JSON.stringify(value);
  if (typeof encoded !== "string") throw new TypeError("Unsupported native hook value");
  return encoded;
}

function canonicalPermit(body: Record<string, unknown>, payload: Record<string, unknown>): boolean {
  const permit = body.canonicalTaskAdmission as Record<string, unknown> | undefined;
  const output = body.hookSpecificOutput as Record<string, unknown> | undefined;
  const bot = process.env.OMB_HOOK_BOT_ID;
  const thread = process.env.OMB_HOOK_THREAD_ID;
  const scope = process.env.OMB_HOOK_TASK_SCOPE_SHA256;
  const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
  const hash = /^[a-f0-9]{64}$/;
  if (!bot || !uuid.test(bot) || !thread || !scope || !hash.test(scope) ||
      !permit || typeof permit !== "object" || Array.isArray(permit) ||
      !output || output.hookEventName !== "PreToolUse" || output.permissionDecision !== "allow" ||
      Object.hasOwn(output, "updatedInput") ||
      typeof payload.session_id !== "string" || !payload.session_id ||
      typeof payload.tool_name !== "string" || !payload.tool_name || !Object.hasOwn(payload,"tool_input")) return false;
  const issued = typeof permit.issued_at === "string" ? Date.parse(permit.issued_at) : NaN;
  const expires = typeof permit.expires_at === "string" ? Date.parse(permit.expires_at) : NaN;
  const now = Date.now();
  if (!Number.isFinite(issued) || !Number.isFinite(expires) || issued > now + 5_000 ||
      expires <= now || expires <= issued || expires - issued > 15_000 ||
      typeof payload.tool_use_id !== "string" || !payload.tool_use_id || permit.tool_use_id !== payload.tool_use_id) return false;
  const digest = createHash("sha256").update(canonicalJson({ event: "PreToolUse", native_session: payload.session_id,
    tool_name: payload.tool_name, tool_input: payload.tool_input })).digest("hex");
  return permit.authorised === true && permit.bot_id === bot && permit.thread_id === thread &&
    permit.scope_sha256 === scope && permit.native_session === payload.session_id && permit.action_sha256 === digest &&
    typeof permit.task_id === "string" && uuid.test(permit.task_id) &&
    typeof permit.work_id === "string" && /^work-[a-f0-9]{16}$/.test(permit.work_id) &&
    typeof permit.journal === "string" && /^history\/journal\/[a-z0-9-]+\.md$/.test(permit.journal) &&
    typeof permit.admission_id === "string" && hash.test(permit.admission_id) &&
    typeof permit.commit_sha === "string" && /^[a-f0-9]{40}$/.test(permit.commit_sha);
}

async function main(): Promise<void> {
  const raw = await readStdin();
  let payload: Record<string, unknown>;
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return done();
    payload = parsed as Record<string, unknown>;
  } catch {
    return done();
  }
  const event = typeof payload.hook_event_name === "string" ? payload.hook_event_name : "";
  strict = requiredMode && (!event || event === "PreToolUse");
  if (!event || !/^https?:\/\/127\.0\.0\.1(:\d+)?$/.test(url) || !tokenFile) return done();
  let token = "";
  try {
    token = readFileSync(tokenFile, "utf8").trim();
  } catch {
    return done();
  }
  if (!token) return done();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(200, budgetMs - 300));
  try {
    const response = await fetch(`${url}/api/internal/hook`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
      body: JSON.stringify({ event, payload }),
      redirect: "error",
      signal: controller.signal,
    });
    if (!response.ok) return done();
    const body = (await response.json().catch(() => null)) as Record<string, unknown> | null;
    if (strict) {
      if (!body || typeof body !== "object" || !canonicalPermit(body,payload)) return done();
      return done(JSON.stringify({ hookSpecificOutput: body.hookSpecificOutput }), true);
    }
    if (body && typeof body === "object" && body.hookSpecificOutput && typeof body.hookSpecificOutput === "object") {
      return done(JSON.stringify({ hookSpecificOutput: body.hookSpecificOutput }));
    }
    // Claude Code reads plain-text stdout as model context only on SessionStart and UserPromptSubmit.
    if (body && typeof body === "object" && typeof body.context === "string" && body.context && CONTEXT_EVENTS.has(event)) {
      return done(body.context);
    }
    return done();
  } catch {
    return done();
  } finally {
    clearTimeout(timer);
  }
}

void main();

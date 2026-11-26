import { mkdirSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { redactSecretsInText } from "./redact.ts";

export const DEFAULT_TURN_STEP_CAP = 150;
const MAX_CHECKPOINT_ENTRIES = 250;

export function stepCapFailureMessage(steps = DEFAULT_TURN_STEP_CAP): string {
  return `Stopped after ${steps} steps without a final answer. The steps so far already ran, so ask only for what's left.`;
}

export function handoffDir(): string {
  return resolve(process.env.OMB_DATA_DIR || join(homedir(), ".openmausbot"), "handoffs");
}

export interface HandoffInput {
  threadId: string;
  turnId: string;
  model?: string;
  usage?: { input: number; output: number };
  hasUsage?: boolean;
  failure?: string;
  steps?: number;
  sessionId?: string | null;
  pendingWork?: string;
  toolCalls: number;
  task: string;
  updates?: string[];
  messages: Array<{
    role: string;
    content?: unknown;
    tool_calls?: Array<{ function?: { name?: string; arguments?: string } }>;
  }>;
}

function serializeDetail(value: unknown, maxChars: number): string {
  if (typeof value === "string") return redactSecretsInText(value).replace(/\s+/g, " ").slice(0, maxChars);
  try {
    return redactSecretsInText(JSON.stringify(value) ?? "").replace(/\s+/g, " ").slice(0, maxChars);
  } catch {
    return redactSecretsInText(String(value ?? "")).replace(/\s+/g, " ").slice(0, maxChars);
  }
}

export class TurnStepTracker {
  readonly stepCap: number;
  steps = 0;
  toolCalls = 0;
  private lastAssistant = "";
  private readonly entries: Array<{ id?: string; name: string; args?: string; ok?: boolean; result?: string }> = [];
  private readonly byId = new Map<string, number>();
  private readonly pendingIds = new Set<string>();

  constructor(stepCap = DEFAULT_TURN_STEP_CAP) {
    this.stepCap = Math.max(1, Math.floor(stepCap));
  }

  get exhausted(): boolean {
    return this.steps >= this.stepCap;
  }

  get pendingTools(): number {
    return this.pendingIds.size;
  }

  recordAssistantText(text: string): void {
    if (text.trim()) this.lastAssistant = text.slice(-6000);
  }

  appendAssistantDelta(delta: string): void {
    if (!delta) return;
    this.lastAssistant = (this.lastAssistant + delta).slice(-6000);
  }

  recordToolStart(call: { id?: string; name?: string; input?: unknown }): void {
    this.toolCalls += 1;
    const name = String(call.name || "tool").slice(0, 80);
    const args = call.input === undefined ? undefined : serializeDetail(call.input, 300);
    if (this.entries.length >= MAX_CHECKPOINT_ENTRIES) {
      this.entries.shift();
      this.byId.clear();
      this.entries.forEach((entry, idx) => {
        if (entry.id) this.byId.set(entry.id, idx);
      });
    }
    const index = this.entries.push({ ...(call.id ? { id: call.id } : {}), name, ...(args ? { args } : {}) }) - 1;
    if (call.id) {
      this.byId.set(call.id, index);
      this.pendingIds.add(call.id);
    }
  }

  recordToolEnd(call: { id?: string; name?: string; ok?: boolean; output?: unknown }): void {
    if (call.id) this.pendingIds.delete(call.id);
    const result = call.output === undefined ? undefined : serializeDetail(call.output, 220);
    const existingIdx = call.id ? this.byId.get(call.id) : undefined;
    if (existingIdx !== undefined && this.entries[existingIdx]) {
      this.entries[existingIdx].ok = call.ok;
      if (result !== undefined) this.entries[existingIdx].result = result;
      return;
    }
    this.toolCalls += 1;
    const name = String(call.name || "tool").slice(0, 80);
    if (this.entries.length >= MAX_CHECKPOINT_ENTRIES) {
      this.entries.shift();
      this.byId.clear();
      this.entries.forEach((entry, idx) => {
        if (entry.id) this.byId.set(entry.id, idx);
      });
    }
    const index = this.entries.push({ ...(call.id ? { id: call.id } : {}), name, ok: call.ok, ...(result !== undefined ? { result } : {}) }) - 1;
    if (call.id) this.byId.set(call.id, index);
  }

  completeStep(): number {
    this.steps += 1;
    return this.steps;
  }

  toMessages(): HandoffInput["messages"] {
    const out: HandoffInput["messages"] = [];
    for (const entry of this.entries) {
      out.push({
        role: "assistant",
        content: null,
        tool_calls: [{ function: { name: entry.name, arguments: entry.args ?? "" } }],
      });
      if (entry.ok !== undefined || entry.result !== undefined) {
        out.push({
          role: "tool",
          content: JSON.stringify({ ok: entry.ok ?? true, ...(entry.result !== undefined ? { result: entry.result } : {}) }),
        });
      }
    }
    if (this.lastAssistant.trim()) out.push({ role: "assistant", content: this.lastAssistant });
    return out;
  }

  writeCheckpoint(opts: {
    threadId: string;
    turnId: string;
    model?: string;
    usage?: { input: number; output: number };
    hasUsage?: boolean;
    sessionId?: string | null;
    task: string;
    updates?: string[];
    pendingWork?: string;
  }): { failure: string; handoffPath: string | null } {
    const failure = stepCapFailureMessage(this.steps || this.stepCap);
    const handoffPath = writeTurnHandoff({
      threadId: opts.threadId,
      turnId: opts.turnId,
      model: opts.model,
      usage: opts.usage,
      hasUsage: opts.hasUsage,
      failure,
      steps: this.steps || this.stepCap,
      sessionId: opts.sessionId,
      pendingWork: opts.pendingWork,
      toolCalls: this.toolCalls,
      task: opts.task,
      updates: opts.updates,
      messages: this.toMessages(),
    });
    return { failure, handoffPath };
  }
}

export function writeTurnHandoff(input: HandoffInput): string | null {
  try {
    const dir = handoffDir();
    mkdirSync(dir, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    const file = join(dir, `cap-${String(input.threadId).slice(0, 8)}-${String(input.turnId).slice(0, 8)}-${stamp}.md`);
    const L: string[] = [];
    L.push("# Handoff - turn ended without a final answer", "");
    L.push(`- Thread: ${input.threadId}`);
    L.push(`- Turn: ${input.turnId}`);
    L.push(`- Model: ${input.model ?? "unknown"}`);
    if (input.sessionId) L.push(`- Provider session: ${redactSecretsInText(input.sessionId)}`);
    if (input.steps !== undefined) L.push(`- Steps used: ${input.steps}`);
    L.push(`- Tool calls used: ${input.toolCalls}`);
    if (input.hasUsage && input.usage) L.push(`- Tokens this turn: ${input.usage.input} in / ${input.usage.output} out`);
    L.push(`- Stop reason: ${redactSecretsInText(input.failure ?? "unknown")}`);
    L.push(`- Written at: ${new Date().toISOString()}`, "");
    L.push("## Current task", "```", redactSecretsInText(input.task).slice(0, 8000), "```", "");
    if (input.updates?.length) {
      L.push("## Updates delivered during this turn (in order)", "");
      for (const update of input.updates) L.push("```", redactSecretsInText(update).slice(0, 8000), "```", "");
    }
    L.push("## Work performed (tool calls in order)", "");
    let n = 0;
    for (const m of input.messages) {
      if (m.role === "assistant" && Array.isArray(m.tool_calls)) {
        for (const tc of m.tool_calls) {
          n += 1;
          const args = redactSecretsInText(String(tc.function?.arguments ?? "")).replace(/\s+/g, " ").slice(0, 300);
          L.push(`${n}. ${tc.function?.name ?? "tool"} - ${args}`);
        }
      } else if (m.role === "tool") {
        let verdict = "?";
        try { verdict = JSON.parse(String(m.content)).ok ? "ok" : "FAILED"; } catch {}
        L.push(`   result ${verdict}: ${redactSecretsInText(String(m.content ?? "")).replace(/\s+/g, " ").slice(0, 220)}`);
      }
    }
    if (n === 0) L.push("(none recorded)");
    if (input.pendingWork?.trim()) {
      L.push("", "## Pending work", "", redactSecretsInText(input.pendingWork).slice(0, 4000));
    }
    L.push("", "## Last assistant text", "");
    let last: string | null = null;
    for (let i = input.messages.length - 1; i >= 0; i -= 1) {
      const c = input.messages[i].content;
      if (input.messages[i].role === "assistant" && typeof c === "string" && c.trim()) { last = c; break; }
    }
    L.push(last ? redactSecretsInText(last).slice(0, 6000) : "(none)", "");
    L.push("## Instructions for whoever continues this task", "");
    L.push("1. Read this file completely.");
    L.push("2. Re-verify the current state of every file, branch, and PR mentioned above before acting - it may be stale.");
    L.push("3. Continue from the first unfinished step. Do not redo completed work.");
    L.push("4. If you get close to your own turn budget, append your progress to this file before stopping.");
    writeFileSync(file, L.join("\n"), { mode: 0o600 });
    return file;
  } catch {
    return null;
  }
}

/** Matched from the start of the failure, so a provider's error text ("HTTP 429: Rate limit reached") never reads as a spent budget. */
const CAP_STOP = /^(?:stopped after \d+ [a-z ]+ without a final answer|model-call limit reached)/i;

/** Only a spent budget continues: a provider error would fail the same way again, and a denied tool would be retried unattended. */
export function classifyContinuable(_stopReason: string | null | undefined, failure: string | undefined): "cap" | null {
  return failure && CAP_STOP.test(failure) ? "cap" : null;
}

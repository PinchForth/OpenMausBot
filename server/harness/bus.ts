// Fan-in event bus — port of upstream's ProviderService fan-in + EventNdjsonLogger tee, minus Effect.
import { isDeepStrictEqual } from "node:util";
import { createHash, randomUUID } from "node:crypto";
import { appendFile } from "node:fs/promises";
import { join } from "node:path";

import { EVENTS_DIR } from "../config.ts";
import { redactSecrets } from "../redact.ts";
import { capThreadLog, currentThreadLogCap } from "../thread-log-rotation.ts";
import { newId, type ProviderInstance, type RuntimeEvent, type RuntimeEventListener } from "../contracts.ts";
import { ProgressGate } from "./progress-gate.ts";

const INCOMPLETE_LOG_MESSAGE =
  "Canonical event history is incomplete: OpenMausBot could not write one or more events to disk. Live updates will continue.";

const DELTA_MERGE_MS = 50;

const MAX_BATCH_LINES = 64;

const MAX_QUEUED_EVENTS = 4096;
const MAX_QUEUED_BYTES = 32 * 1024 * 1024;

export type EventLogAppend = (
  path: string,
  data: string,
  options?: { mode?: number },
) => void | Promise<void>;

const defaultAppend: EventLogAppend = (path, data, options) => appendFile(path, data, options);

type QueuedLog = { event: RuntimeEvent; line: string; bytes: number };

type TextDelta = Extract<RuntimeEvent, { type: "content.delta" }>;

function sameStream(a: TextDelta, b: TextDelta): boolean {
  return a.provider === b.provider && a.providerInstanceId === b.providerInstanceId && a.turnId === b.turnId &&
    a.itemId === b.itemId && a.streamKind === b.streamKind && a.synthetic === b.synthetic;
}

function isTerminalEvent(event: RuntimeEvent): boolean {
  if (event.type === "turn.completed" || event.type === "cap.exhausted" || event.type === "session.exited") {
    return true;
  }
  return event.type === "runtime.error" && event.terminal === true;
}

/** Internal runtime dependency, never an event field or an agent-configurable grant.
 * The runtime must classify ordinary text with its qualified canonical classifier;
 * technical handbacks must call ContextLoop.check_native_handoff with runtime-owned
 * identity, target and reference. No consumer is installed by this interface. */
export interface AssistantPublicationReview {
  /** Compatibility metadata only. Provider/owns labels never bypass enabled review. */
  providers: ReadonlySet<string>;
  owns?(event: RuntimeEvent): boolean;
  review(item: Readonly<PublicationItem>): Promise<PublicationDecision>;
  attach?(instances: ProviderInstance[]): void;
  afterTurn?(event: RuntimeEvent): Promise<void>;
}

export interface PublicationItem {
  provider: string;
  providerInstanceId: string;
  threadId: string;
  turnId: string;
  itemId: string;
  native: { sessionId: string; turnId: string; itemId: string } | null;
  text: string;
  sha256: string;
}

export type PublicationDecision = {
  binding: Omit<PublicationItem, "text">;
} & ({ kind: "ordinary" } | {
  kind: "canonical";
  context: {
    status: "ok";
    source_checked: true;
    semantic_truth: "bounded_permission_observation" | "bounded_ledger_observation";
    task_id: string;
    admission_id: string;
    expires_at: string;
  };
});

const canonicalPublicationDecisions = new WeakSet<PublicationDecision>();
const scopedPublicationReviews = new WeakMap<AssistantPublicationReview, (event: RuntimeEvent) => Promise<boolean>>();
const publicationBotId = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const adapterEventOrigins = new WeakMap<RuntimeEvent, ProviderInstance>();
const originalDispatches = new WeakMap<ProviderInstance["adapter"], ProviderInstance["adapter"]["sendTurn"]>();

/** Server-owned bootstrap only. Credentials use the existing OS Access bearer
 * contract; model events cannot select the authority URL or turn review on. */
export function canonicalPublicationReviewFromEnvironment(env: NodeJS.ProcessEnv = process.env): AssistantPublicationReview | undefined {
  if (env.OS_CONTEXT_PUBLICATION !== "1") return undefined;
  const authority = new URL(env.OS_ACCESS_URL ?? "");
  if (authority.username || authority.password || authority.search || authority.hash ||
      (authority.protocol !== "https:" && !(authority.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(authority.hostname)))) {
    throw new Error("Canonical publication requires HTTPS or loopback and no URL credentials.");
  }
  if (!env.OS_ACCESS_TOKEN) throw new Error("Canonical publication requires the existing scoped OS Access credential.");
  const token = env.OS_ACCESS_TOKEN;
  const scoped = env.OS_CONTEXT_PUBLICATION_SCOPE === "registered";
  if (env.OS_CONTEXT_PUBLICATION_SCOPE && !scoped) throw new Error("Unknown publication scope.");
  if (env.OS_CONTEXT_PUBLICATION_BINDINGS) throw new Error("Static turn publication bindings are retired; runtime events and canonical accounts own association.");
  const instances = new Map<string, ProviderInstance>();
  const pending = new Map<string, PublicationItem[]>();
  const lane = (item: { provider: string; providerInstanceId?: string; threadId: string; turnId?: string }) =>
    JSON.stringify([item.provider, item.providerInstanceId, item.threadId, item.turnId]);
  const request = async (mode: string, item: Readonly<PublicationItem> | undefined, extra: Record<string, unknown> = {}) => {
    const response = await fetch(new URL("/os/context", authority), {
      method: "POST", redirect: "error", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify({ mode, item, ...extra }), signal: AbortSignal.timeout(140000),
    });
    const payload = await response.json() as Record<string, unknown>;
    if (!response.ok || !payload || typeof payload !== "object") throw new Error("Canonical context refused publication.");
    return payload;
  };
  type Dispatch = { selected: boolean; closed: boolean; ready: Promise<string | undefined> };
  const dispatches = new Map<string, Dispatch>();
  const starting = new Map<string, Dispatch>();
  const turnKey = (instance: string, thread: string, turn?: string) => JSON.stringify([instance, thread, turn]);
  const selection = async () => {
    const requestId = randomUUID();
    const payload = await request("native_publication_scope", undefined, { request_id: requestId });
    const accounts = payload.accounts as Array<{ bot_id: string; principal: string; providers: string[]; topic: string }>;
    const source = payload.source as { head?: string; blob?: string } | undefined;
    if (payload.status !== "ok" || payload.request_id !== requestId || payload.scope !== "registered" || !/^[0-9a-f]{40}$/.test(source?.head ?? "") ||
        !/^[0-9a-f]{40}$/.test(source?.blob ?? "") || !Array.isArray(accounts) || !accounts.length || accounts.some(a =>
          !a || !publicationBotId.test(a.bot_id) || typeof a.principal !== "string" || !a.principal || typeof a.topic !== "string" || !a.topic ||
          !Array.isArray(a.providers) || !a.providers.length || a.providers.some(p => typeof p !== "string" || !p)) ||
        new Set(accounts.map(a => a.bot_id)).size !== accounts.length) throw new Error("Invalid canonical publication scope selection.");
    return accounts;
  };
  const protection = async (event: RuntimeEvent): Promise<boolean> => {
    const instance = event.providerInstanceId ? instances.get(event.providerInstanceId) : undefined;
    if (!instance || adapterEventOrigins.get(event) !== instance || instance.driverKind !== event.provider || !event.turnId) return true;
    const entry = dispatches.get(turnKey(instance.instanceId, event.threadId, event.turnId)) ??
      starting.get(turnKey(instance.instanceId, event.threadId));
    if (!entry || entry.closed || await entry.ready !== event.turnId) return true;
    if (!entry.selected && isTerminalEvent(event)) entry.closed = true;
    return entry.selected;
  };
  const review: AssistantPublicationReview = { providers: new Set(), attach: attached => {
    for (const instance of attached) {
      instances.set(instance.instanceId, instance);
      if (!scoped) continue;
      const adapter = instance.adapter;
      const original = originalDispatches.get(adapter) ?? adapter.sendTurn;
      originalDispatches.set(adapter, original);
      adapter.sendTurn = async input => {
        const { botId, threadId } = input;
        // This is the existing server dispatcher call, not a model event label.
        // Selection is reread for every real dispatch; it is not a turn whitelist.
        const accounts = await selection();
        let finish!: (turn: string | undefined) => void;
        const entry: Dispatch = { closed: false, selected: !publicationBotId.test(botId ?? "") || accounts.some(a => a.bot_id === botId),
          ready: new Promise(resolve => { finish = resolve; }) };
        const key = turnKey(instance.instanceId, threadId);
        starting.set(key, entry);
        try {
          const result = await original.call(adapter, { ...input, botId, threadId });
          if (!result.turnId) throw new Error("Actual publication dispatch returned no turn.");
          dispatches.set(turnKey(instance.instanceId, threadId, result.turnId), entry);
          while (dispatches.size > 1024) dispatches.delete(dispatches.keys().next().value!);
          finish(result.turnId);
          return result;
        } catch (error) { finish(undefined); throw error; }
        finally { if (starting.get(key) === entry) starting.delete(key); }
      };
    }
  }, afterTurn: async event => {
    const key = lane(event);
    for (const item of pending.get(key) ?? []) {
      const ack = await request("native_publication_ack", item);
      if (ack.status !== "ok" || ack.phase !== "acknowledged") throw new Error("Canonical destination acknowledgement refused.");
    }
    pending.delete(key);
  }, review: async item => {
    const instance = instances.get(item.providerInstanceId);
    const consume = instance?.driverKind === item.provider ? instance.adapter.consumePublicationContext : undefined;
    let payload: Record<string, unknown>;
    if (consume && item.native) {
      let prepared = await request("native_publication_prepare", item);
      for (let pass = 0; prepared.status === "needs_context" && pass < 2; pass++) {
        if (prepared.reason !== "native_account_required" || typeof prepared.prompt !== "string") throw new Error("Canonical native account unavailable.");
        // The supported driver verifies the actual original dispatch and keeps
        // this confined same-session account turn private in its native audit.
        const reference = { threadId: item.threadId, turnId: item.turnId, itemId: item.itemId,
          native: { ...item.native }, sha256: item.sha256 };
        const native_account = await consume.call(instance!.adapter, reference, prepared.prompt);
        prepared = await request("native_publication_account", item, { native_account });
      }
      if (prepared.status !== "ok") throw new Error("Canonical native account incomplete.");
      if (prepared.prepared === true) {
        const key = lane(item);
        const items = pending.get(key) ?? [];
        items.push(structuredClone(item)); pending.set(key, items);
        // The current authority reviews immediately after real admission in
        // the account response. Preserve the split request for older servers.
        // A refusal carries no decision and remains ineligible below, while
        // the actual admitted withholding still receives its destination ACK.
        payload = "decision" in prepared || typeof prepared.publication_refused === "string"
          ? prepared : await request("native_publication", item);
      } else payload = prepared;
    } else payload = await request("native_publication", item);
    if (payload.status !== "ok" || !("decision" in payload)) throw new Error("Invalid canonical publication response.");
    const decision = payload.decision as PublicationDecision;
    const { text: _text, ...binding } = item;
    if (!decision || !isDeepStrictEqual(decision.binding,binding) || !["ordinary","canonical"].includes(decision.kind)) {
      throw new Error("Canonical publication binding mismatch.");
    }
    if (decision.kind === "canonical") {
      const context = decision.context;
      if (context?.status !== "ok" || context.source_checked !== true || !["bounded_permission_observation","bounded_ledger_observation"].includes(context.semantic_truth) ||
          !/^[0-9a-f-]{36}$/.test(context.task_id) || !/^[0-9a-f]{64}$/.test(context.admission_id) ||
          !Number.isFinite(Date.parse(context.expires_at)) || Date.parse(context.expires_at) <= Date.now() ||
          !("observation" in decision) || !decision.observation) throw new Error("Canonical observation required.");
      canonicalPublicationDecisions.add(decision);
    }
    return decision;
  } };
  if (scoped) scopedPublicationReviews.set(review, protection);
  return review;
}

const PUBLICATION_LIMIT = 262144;
const PUBLICATION_MAX_ITEMS = 256;
const PUBLICATION_REVIEW_MS = 150000;
const WITHHELD = "Assistant output retained privately pending canonical verification. Work can continue.";

export class EventBus {
  private readonly publicationTails = new Map<string, Promise<void>>();
  private readonly publicationQueued = new Map<string, number>();
  private readonly publicationOverflow = new Set<string>();
  private readonly publicationTurns = new Map<string, { turnId: string | undefined; closed: boolean }>();
  private readonly heldItems = new Map<string, { text: string; overflow: boolean; native: RuntimeEvent["native"] }>();
  private readonly completedItems = new Set<string>();
  private listeners = new Set<RuntimeEventListener>();
  private unsubscribes = new Map<string, () => void>();
  private pendingLogWarnings = new Map<string, RuntimeEvent>();
  private pendingText = new Map<string, { event: TextDelta; timer: ReturnType<typeof setTimeout> }>();
  private readonly appendLog: EventLogAppend;
  private readonly publication?: AssistantPublicationReview;
  private queued: QueuedLog[] = [];
  private queuedBytes = 0;
  private pumping = false;
  private kickScheduled = false;
  private readonly drainWaiters: Array<() => void> = [];
  private readonly queuedLogWarnings = new Map<string, RuntimeEvent>();
  // A status-line report is live only: delivered to listeners, never queued for the log.
  private readonly progress = new ProgressGate((event) => {
    if (this.pendingText.has(event.threadId)) this.flushThread(event.threadId);
    this.deliver(event);
  });

  private readonly gate?: (event: RuntimeEvent) => boolean;

  /** `gate` sees adapter events first; publication review also covers direct republishes. */
  constructor(appendLog: EventLogAppend = defaultAppend, gate?: (event: RuntimeEvent) => boolean, publication?: AssistantPublicationReview) {
    this.appendLog = appendLog;
    this.gate = gate;
    this.publication = publication;
  }

  attach(instances: ProviderInstance[]) {
    this.publication?.attach?.(instances);
    for (const instance of instances) {
      this.detach(instance.instanceId);
      const unsub = instance.adapter.onEvent((event) => {
        // An adapter may only emit events for its own driver kind.
        if (event.provider !== instance.driverKind) {
          console.error(`bus: dropped cross-driver event from ${instance.instanceId}`);
          return;
        }
        const stamped = { ...event, providerInstanceId: instance.instanceId };
        adapterEventOrigins.set(stamped, instance);
        if (!this.gate?.(stamped)) this.publish(stamped);
      });
      this.unsubscribes.set(instance.instanceId, unsub);
    }
  }

  publish(event: RuntimeEvent) {
    if (!this.publication) {
      this.publishReviewed(event);
      return;
    }
    // Snapshot before queuing: a producer cannot change the text or identity while review awaits.
    const snapshot = structuredClone(event);
    const origin = adapterEventOrigins.get(event);
    if (origin) adapterEventOrigins.set(snapshot, origin);
    const lane = JSON.stringify([event.provider, event.providerInstanceId, event.threadId]);
    const count = this.publicationQueued.get(lane) ?? 0;
    if (count >= PUBLICATION_MAX_ITEMS && !isTerminalEvent(event) && event.type !== "turn.started") {
      this.publicationOverflow.add(lane);
      return;
    }
    this.publicationQueued.set(lane, count + 1);
    const prior = this.publicationTails.get(lane) ?? Promise.resolve();
    const next = prior.then(() => this.reviewPublication(snapshot)).catch(() => {
      this.publicationNotice(snapshot);
    });
    this.publicationTails.set(lane, next);
    void next.finally(() => {
      const remaining = (this.publicationQueued.get(lane) ?? 1) - 1;
      if (remaining) this.publicationQueued.set(lane, remaining);
      else this.publicationQueued.delete(lane);
      if (this.publicationTails.get(lane) === next) this.publicationTails.delete(lane);
    });
  }

  private publicationNotice(event: RuntimeEvent) {
    this.publishReviewed({ eventId: newId(), provider: event.provider,
      providerInstanceId: event.providerInstanceId, threadId: event.threadId,
      turnId: event.turnId, createdAt: new Date().toISOString(), type: "runtime.notice", message: WITHHELD });
  }

  private async reviewPublication(event: RuntimeEvent) {
    // Only the authenticated factory and real dispatched adapter origin can
    // select unprotected baseline delivery. Direct/model/callback labels cannot.
    const protection = this.publication ? scopedPublicationReviews.get(this.publication) : undefined;
    if (protection && await protection(event) === false) {
      this.publishReviewed(event);
      return;
    }
    const lane = JSON.stringify([event.provider, event.providerInstanceId, event.threadId]);
    const current = this.publicationTurns.get(lane);
    if (event.type === "turn.started") {
      this.publicationTurns.set(lane, { turnId: event.turnId, closed: false });
      this.publicationOverflow.delete(lane);
    }
    if (isTerminalEvent(event) && current && event.turnId !== undefined && current.turnId !== event.turnId) return;
    const key = JSON.stringify([event.provider, event.providerInstanceId, event.threadId, event.turnId, event.itemId]);
    const assistantDelta = event.type === "content.delta" && event.streamKind === "assistant_text";
    const assistantItem = event.type === "item.completed" && event.itemType === "assistant_text";
    if (assistantDelta || assistantItem) {
      if (this.publicationOverflow.has(lane)) return;
      if (this.heldItems.size + this.completedItems.size >= PUBLICATION_MAX_ITEMS) {
        this.publicationOverflow.add(lane);
        this.publicationNotice(event);
        return;
      }
      if (current && (current.closed || current.turnId !== event.turnId)) return;
      if (!current) this.publicationTurns.set(lane, { turnId: event.turnId, closed: false });
    }
    if (assistantDelta) {
      if (this.completedItems.has(key)) return;
      const held = this.heldItems.get(key) ?? { text: "", overflow: false, native: event.native };
      if (Buffer.byteLength(held.text) + Buffer.byteLength(event.delta) > PUBLICATION_LIMIT) held.overflow = true;
      if (!held.overflow) held.text += event.delta;
      this.heldItems.set(key, held);
      return;
    }
    if (assistantItem) {
      if (this.completedItems.has(key)) return;
      if (event.itemId) this.completedItems.add(key);
      const held = this.heldItems.get(key);
      this.heldItems.delete(key);
      const native = event.native;
      if (!event.providerInstanceId || !event.turnId ||
          (native && (!native.sessionId || !native.turnId || native.itemId !== event.itemId)) || held?.overflow ||
          Buffer.byteLength(event.text) > PUBLICATION_LIMIT || (held && (held.text !== event.text || !isDeepStrictEqual(held.native, native)))) {
        this.publicationNotice(event);
        return;
      }
      const item: PublicationItem = { provider: event.provider, providerInstanceId: event.providerInstanceId,
        threadId: event.threadId, turnId: event.turnId, itemId: event.itemId ?? event.eventId, native: native ? { ...native } : null,
        text: event.text, sha256: createHash("sha256").update(event.text).digest("hex") };
      const { text: _text, ...binding } = item;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let decision: PublicationDecision;
      try {
        decision = await Promise.race([
          this.publication!.review(Object.freeze({ ...item, native: item.native ? Object.freeze(item.native) : null })),
          new Promise<never>((_resolve, reject) => {
            timer = setTimeout(() => reject(new Error("publication_review_unavailable")), PUBLICATION_REVIEW_MS);
            timer.unref?.();
          }),
        ]);
      } finally { clearTimeout(timer); }
      const sameBinding = decision && isDeepStrictEqual(decision.binding, binding);
      // Only the authenticated consuming path creates this local capability.
      // A model event, callback boolean or copied receipt cannot forge it.
      if (!sameBinding || (decision.kind !== "ordinary" && (!canonicalPublicationDecisions.has(decision) || Date.parse(decision.context.expires_at) <= Date.now()))) {
        this.publicationNotice(event);
        return;
      }
      const { raw: _raw, ...clean } = event;
      // Only the complete reviewed item is public. Native fragments remain in the native audit.
      this.publishReviewed(clean);
      return;
    }
    if (isTerminalEvent(event)) {
      if (!current || event.turnId === undefined || current.turnId === event.turnId) {
        this.publicationTurns.set(lane, { turnId: current?.turnId ?? event.turnId, closed: true });
      }
      const prefix = JSON.stringify([event.provider, event.providerInstanceId, event.threadId]).slice(0, -1) + ",";
      for (const heldKey of this.heldItems.keys()) if (heldKey.startsWith(prefix)) this.heldItems.delete(heldKey);
      for (const completedKey of this.completedItems) if (completedKey.startsWith(prefix)) this.completedItems.delete(completedKey);
      // Provider extensions such as lastText must not become a final fallback.
      const base = { eventId: event.eventId, provider: event.provider, providerInstanceId: event.providerInstanceId,
        threadId: event.threadId, turnId: event.turnId, createdAt: event.createdAt };
      if (event.type === "turn.completed") {
        const usage = event.usage && Number.isFinite(event.usage.input) && Number.isFinite(event.usage.output)
          ? { input: event.usage.input, output: event.usage.output,
              ...(Number.isFinite(event.usage.cachedInput) ? { cachedInput: event.usage.cachedInput } : {}) } : undefined;
        this.publishReviewed({ ...base, type: "turn.completed", ok: event.ok,
          stopReason: event.ok ? null : "publication_review_required", usage,
          ...(typeof event.cost === "number" && Number.isFinite(event.cost) ? { cost: event.cost } : {}) });
      } else if (event.type === "session.exited") {
        this.publishReviewed({ ...base, type: "session.exited", reason: "session_exited" });
      } else if (event.type === "runtime.error") {
        this.publishReviewed({ ...base, type: "runtime.error", terminal: true, message: WITHHELD });
      } else if (event.type === "cap.exhausted") {
        this.publishReviewed({ ...base, type: "cap.exhausted", handoffPath: event.handoffPath, reason: "cap" });
      }
      await this.flushDisk();
      await this.publication?.afterTurn?.(event);
      return;
    }
    if (event.type === "request.opened" && event.origin === "output") {
      this.publicationNotice(event);
      return;
    }
    if (event.type === "runtime.error" || event.type === "runtime.notice") {
      // Provider error prose is not a second assistant-output channel.
      this.publicationNotice(event);
      return;
    }
    const { raw: _raw, ...clean } = event;
    this.publishReviewed(clean);
  }

  private publishReviewed(event: RuntimeEvent) {
    if (event.type === "turn.progress") {
      this.progress.offer(event);
      return;
    }
    this.progress.supersede(event.threadId, isTerminalEvent(event));
    const pending = this.pendingText.get(event.threadId);
    if (event.type === "content.delta") {
      if (pending && sameStream(pending.event, event)) {
        pending.event = { ...pending.event, delta: pending.event.delta + event.delta };
        return;
      }
      if (pending) this.flushThread(event.threadId);
      const timer = setTimeout(() => this.flushThread(event.threadId), DELTA_MERGE_MS);
      timer.unref?.();
      this.pendingText.set(event.threadId, { event, timer });
      return;
    }
    if (pending) this.flushThread(event.threadId);
    this.write(event);
  }

  async flush(): Promise<void> {
    while (this.publicationTails.size) await Promise.all(this.publicationTails.values());
    for (const threadId of Array.from(this.pendingText.keys())) this.flushThread(threadId);
    return this.flushDisk();
  }

  async flushWithin(ms: number): Promise<boolean> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<false>((resolve) => {
      // Not unref'd: the caller is waiting on this answer before it exits.
      timer = setTimeout(() => resolve(false), ms);
    });
    try {
      return await Promise.race([this.flush().then(() => this.pendingLogWarnings.size === 0), timeout]);
    } finally {
      clearTimeout(timer);
    }
  }

  private flushThread(threadId: string) {
    const pending = this.pendingText.get(threadId);
    if (!pending) return;
    // Gone from the map before delivery: a listener may publish on this thread.
    this.pendingText.delete(threadId);
    clearTimeout(pending.timer);
    this.write(pending.event);
  }

  private write(event: RuntimeEvent) {
    // Queue first, then deliver: a listener publishing during delivery queues after this, so the log keeps publication order.
    this.enqueue(event);
    this.deliver(event);
    if (isTerminalEvent(event)) void this.flushDisk();
  }

  private enqueue(event: RuntimeEvent) {
    // People paste this log into bug reports: scrub credential-shaped content as the native tee does.
    const line = JSON.stringify(redactSecrets(event));
    const bytes = Buffer.byteLength(line);
    const overCount = this.queued.length >= MAX_QUEUED_EVENTS;
    const overBytes = this.queued.length > 0 && this.queuedBytes + bytes > MAX_QUEUED_BYTES;
    if ((overCount || overBytes) && !isTerminalEvent(event)) {
      if (!this.queuedLogWarnings.has(event.threadId)) {
        const warning = this.logWarning(event);
        this.queuedLogWarnings.set(event.threadId, warning);
        // Queued before delivery: listeners may publish synchronously.
        this.push(warning);
        console.error("bus: canonical event log queue is full");
        this.deliver(warning);
      }
      return;
    }
    this.queuedLogWarnings.delete(event.threadId);
    this.push(event, line, bytes);
  }

  private push(event: RuntimeEvent, line = JSON.stringify(redactSecrets(event)), bytes = Buffer.byteLength(line)) {
    this.queued.push({ event, line, bytes });
    this.queuedBytes += bytes;
    this.kick();
  }

  private kick() {
    if (this.pumping || this.kickScheduled) return;
    this.kickScheduled = true;
    queueMicrotask(() => {
      this.kickScheduled = false;
      if (this.queued.length === 0 || this.pumping) {
        this.settleDrain();
        return;
      }
      this.pumping = true;
      void this.drain().catch((error: unknown) => {
        console.error("bus: canonical event log drain failed", error);
      }).finally(() => {
        this.pumping = false;
        if (this.queued.length > 0) this.kick();
        else this.settleDrain();
      });
    });
  }

  private flushDisk(): Promise<void> {
    if (this.queued.length === 0 && !this.pumping && !this.kickScheduled) return Promise.resolve();
    return new Promise((resolve) => {
      this.drainWaiters.push(resolve);
      this.kick();
    });
  }

  private settleDrain() {
    if (this.queued.length > 0 || this.pumping || this.kickScheduled) return;
    const waiters = this.drainWaiters.splice(0);
    for (const resolve of waiters) resolve();
  }

  private async drain(): Promise<void> {
    if (this.queued.length === 0) return;
    const batch = this.queued.splice(0, MAX_BATCH_LINES);
    let bytes = 0;
    for (const item of batch) bytes += item.bytes;
    this.queuedBytes -= bytes;
    const groups: { threadId: string; items: QueuedLog[] }[] = [];
    for (const item of batch) {
      const last = groups.at(-1);
      if (last && last.threadId === item.event.threadId) last.items.push(item);
      else groups.push({ threadId: item.event.threadId, items: [item] });
    }
    for (const group of groups) await this.appendGroup(group.threadId, group.items);
    if (this.queued.length > 0) await this.drain();
  }

  private async appendGroup(threadId: string, items: QueuedLog[]): Promise<void> {
    const warning = this.pendingLogWarnings.get(threadId);
    const events = items.map((item) => item.event);
    const lines = items.map((item) => item.line);
    if (warning) lines.unshift(JSON.stringify(warning));
    const file = join(EVENTS_DIR, `${threadId}.ndjson`);
    try {
      await this.appendLog(file, lines.join("\n") + "\n", { mode: 0o600 });
      if (warning) this.pendingLogWarnings.delete(threadId);
      capThreadLog(file, currentThreadLogCap());
    } catch (error) {
      this.noteDiskFailure(events[0], error);
    } finally {
      const queuedWarning = this.queuedLogWarnings.get(threadId);
      if (queuedWarning && events.includes(queuedWarning)) this.queuedLogWarnings.delete(threadId);
    }
  }

  private logWarning(event: RuntimeEvent): RuntimeEvent {
    return {
      eventId: newId(),
      provider: event.provider,
      providerInstanceId: event.providerInstanceId,
      threadId: event.threadId,
      createdAt: new Date().toISOString(),
      turnId: event.turnId,
      type: "runtime.error",
      message: INCOMPLETE_LOG_MESSAGE,
    };
  }

  /** Never through publish(): that would retry the same failed write and recurse. */
  private noteDiskFailure(event: RuntimeEvent, error: unknown) {
    if (this.pendingLogWarnings.has(event.threadId)) return;
    const warning = this.logWarning(event);
    this.pendingLogWarnings.set(event.threadId, warning);
    console.error("bus: canonical event log write failed", error);
    this.deliver(warning);
  }

  private deliver(event: RuntimeEvent) {
    for (const listener of Array.from(this.listeners)) {
      try {
        listener(event);
      } catch (e) {
        console.error("bus: listener threw", e);
      }
    }
  }

  subscribe(listener: RuntimeEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  detachAll() {
    for (const id of this.unsubscribes.keys()) this.detach(id);
  }

  detach(instanceId: string) {
    this.unsubscribes.get(instanceId)?.();
    this.unsubscribes.delete(instanceId);
    void this.flush();
  }
}

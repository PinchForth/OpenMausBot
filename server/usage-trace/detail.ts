// The one short line a trace shows beside a tool: the query, the page, the action. Typed text, messages and file bodies are
// never kept, and every line passes the secret redaction before it is stored.
import { redactSecretsInText } from "../../shared/redact.ts";
import { TOOL_NAMESPACE } from "../../shared/tool-surface.ts";

export const DETAIL_MAX = 80;
export const ERROR_MAX = 120;

const clip = (text: string, max: number): string | undefined => {
  const flat = redactSecretsInText(text.replace(/\s+/g, " ").trim());
  if (!flat) return undefined;
  return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
};

function fieldsOf(input: unknown): Record<string, unknown> | null {
  if (input && typeof input === "object" && !Array.isArray(input)) return input as Record<string, unknown>;
  if (typeof input !== "string" || !/^\s*\{/.test(input)) return null;
  try {
    const parsed: unknown = JSON.parse(input);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : null;
  } catch {
    return null;
  }
}

function pageOf(raw: string): string | undefined {
  try {
    const url = new URL(raw);
    if (url.protocol !== "http:" && url.protocol !== "https:") return undefined;
    const path = url.pathname === "/" ? "" : url.pathname;
    return `${url.host.replace(/^www\./, "")}${path}`;
  } catch {
    return undefined;
  }
}

const QUERY_KEYS = ["query", "q", "search", "search_query", "pattern", "searchTerm"];
const PAGE_KEYS = ["url", "uri", "href", "link"];
const FILE_KEYS = ["file_path", "filePath", "path", "notebook_path", "file"];
const ACTION_KEYS = ["action", "operation", "method", "kind"];

/** A tool's detail from its call: a shell summary, a parsed preview, or nothing. Never the raw arguments. */
export function toolDetail(name: string, input: unknown, summary?: string): string | undefined {
  if (summary) return clip(summary, DETAIL_MAX);
  const fields = fieldsOf(input);
  if (!fields) return undefined;
  const text = (key: string) => typeof fields[key] === "string" ? (fields[key] as string) : undefined;
  for (const key of QUERY_KEYS) if (text(key)) return clip(text(key)!, DETAIL_MAX);
  for (const key of PAGE_KEYS) {
    const page = text(key) && pageOf(text(key)!);
    if (page) return clip(page, DETAIL_MAX);
  }
  for (const key of FILE_KEYS) {
    const path = text(key);
    if (path) return clip(path.split(/[\\/]/).filter(Boolean).slice(-2).join("/"), DETAIL_MAX);
  }
  if (text("command")) return clip(text("command")!, DETAIL_MAX);
  const bare = name.replace(TOOL_NAMESPACE, "");
  for (const key of ACTION_KEYS) if (text(key)) return clip(text(key)!.replace(/[_-]+/g, " "), DETAIL_MAX);
  if (typeof fields.name === "string" && /^(?:use_tool|call_tool)$/.test(bare)) return clip(fields.name, DETAIL_MAX);
  if (Array.isArray(fields.coordinate) && fields.coordinate.length === 2) return clip(`at ${fields.coordinate.join(", ")}`, DETAIL_MAX);
  return undefined;
}

/** A page title a browser tool reported, when its output states one. */
export function pageTitle(output: unknown): string | undefined {
  if (typeof output !== "string") return undefined;
  const match = /(?:^|\n|"|\b)(?:page )?title["']?\s*[:=]\s*["']?([^"'\n]{1,120})/i.exec(output);
  return match ? clip(match[1]!, 60) : undefined;
}

const ERROR_PRIORITY_KEYS = ["error", "message", "reason", "detail", "details", "stderr", "output", "result", "text", "content"] as const;
const ERROR_SKIP_KEYS = new Set(["ok", "isError", "is_error", "type", "id", "kind", "exitCode", "exit_code", "status"]);
const JSON_META_LINE = /^["'\[{},\]]*$|^"(?:ok|isError|is_error|type)"\s*:/;
const TRUNCATED_JSON_FIELD = /(?:^|\n)\s*"(?:error|message|reason|detail|details|stderr|output|result|text)"\s*:\s*"((?:[^"\\]|\\.)*)/;

function unescapeJsonFragment(raw: string): string {
  return raw.replace(/\\([\\/"bfnrt]|u[0-9a-fA-F]{4})/g, (_m, esc: string) => {
    switch (esc[0]) {
      case "n": return "\n";
      case "r": return "\r";
      case "t": return "\t";
      case "\"": return "\"";
      case "/": return "/";
      case "\\": return "\\";
      case "b": return "\b";
      case "f": return "\f";
      case "u": return String.fromCharCode(Number.parseInt(esc.slice(1), 16));
      default: return esc;
    }
  });
}

function extractErrorText(value: unknown, depth = 0): string | undefined {
  if (depth > 4 || value === null || value === undefined) return undefined;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return undefined;
    if (/^[\[{]/.test(trimmed)) {
      try {
        const parsed: unknown = JSON.parse(trimmed);
        const extracted = extractErrorText(parsed, depth + 1);
        if (extracted) return extracted;
      } catch {
        const match = TRUNCATED_JSON_FIELD.exec(trimmed);
        if (match?.[1]) {
          const decoded = unescapeJsonFragment(match[1]);
          if (decoded.trim()) return decoded;
        }
        const meaningful = trimmed
          .split(/\r?\n/)
          .map((line) => line.trim())
          .find((line) => line && !JSON_META_LINE.test(line) && line !== "[… preview shortened]");
        if (meaningful) return meaningful;
        return undefined;
      }
    }
    return value;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const extracted = extractErrorText(item, depth + 1);
      if (extracted) return extracted;
    }
    return undefined;
  }
  if (typeof value === "object") {
    const obj = value as Record<string, unknown>;
    for (const key of ERROR_PRIORITY_KEYS) {
      if (key in obj) {
        const extracted = extractErrorText(obj[key], depth + 1);
        if (extracted) return extracted;
      }
    }
    for (const [key, child] of Object.entries(obj)) {
      if (ERROR_SKIP_KEYS.has(key)) continue;
      if (typeof child === "string" || (child && typeof child === "object")) {
        const extracted = extractErrorText(child, depth + 1);
        if (extracted) return extracted;
      }
    }
    const exitCode = typeof obj.exitCode === "number" ? obj.exitCode : typeof obj.exit_code === "number" ? obj.exit_code : undefined;
    if (typeof exitCode === "number" && exitCode !== 0) return `Exit code ${exitCode}`;
    return undefined;
  }
  return undefined;
}

export function errorLine(output: unknown): string | undefined {
  const raw = extractErrorText(output);
  if (!raw) return undefined;
  const redacted = redactSecretsInText(raw);
  const first = redacted.split(/\r?\n/).find((line) => line.trim());
  return first ? clip(first, ERROR_MAX) : undefined;
}

/**
 * OpenClaw adapter for the host-agnostic `eigenflux profile refresh-task`
 * core. The CLI owns prompt assembly + memory reading; this module supplies the
 * two host-specific inputs:
 *   - memoryDirs: where OpenClaw keeps memory markdown (CLI reads the files).
 *   - sessionSnippets: recent user-driven topics extracted from the OpenClaw
 *     session transcript (host-specific JSONL format).
 *
 * Session extraction is best-effort: any failure (missing file, parse error)
 * yields an empty list and is logged at debug, never thrown.
 */

import { readdirSync, statSync, readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';
import { Logger } from './logger';

/**
 * Memory markdown lives under the OpenClaw workspace, relative to the state dir:
 * `<stateDir>/workspace/memory/*.md` — memory-core's source of truth. We hand
 * the directory to the CLI (which reads the markdown) so memory works even when
 * the sqlite vector index is unavailable (e.g. no embedding key configured).
 */
const MEMORY_DIR_REL = ['workspace', 'memory'];

export interface RefreshContext {
  /** Directories of memory markdown for the CLI to read (`--memory-dir`). */
  memoryDirs: string[];
  /** Recent session topics, host-extracted, for the CLI (`--session-snippet`). */
  sessionSnippets: string[];
}

export const EMPTY_CONTEXT: RefreshContext = { memoryDirs: [], sessionSnippets: [] };

/**
 * Resolve the OpenClaw state directory (e.g. ~/.openclaw), where memory/ and
 * agents/ live. NOTE: this is NOT `api.rootDir` — that is the plugin's own
 * install directory. The canonical source is the SDK's resolveStateDir(); we
 * fall back to the gateway's cwd and then ~/.openclaw so a missing/renamed SDK
 * export degrades gracefully instead of reading from the wrong place.
 */
export function resolveOpenClawStateDir(logger: Logger): string | undefined {
  try {
    // Deep SDK subpath — load defensively so an SDK change can't break plugin load.
    const mod = require('openclaw/plugin-sdk/memory-core-host-runtime-core') as {
      resolveStateDir?: () => string;
    };
    const dir = mod.resolveStateDir?.();
    if (dir && typeof dir === 'string') return dir;
  } catch (err) {
    logger.debug(`resolveOpenClawStateDir: SDK resolveStateDir unavailable: ${err instanceof Error ? err.message : String(err)}`);
  }
  // Fallbacks: the gateway runs with cwd = state dir; then the default location.
  const cwd = process.cwd();
  if (cwd && existsSync(join(cwd, 'agents'))) return cwd;
  const home = join(homedir(), '.openclaw');
  if (existsSync(join(home, 'agents'))) return home;
  return undefined;
}

const MAX_SESSION_TURNS = 12;
const MAX_SESSION_SNIPPET_CHARS = 280;

/**
 * Resolve the host-specific inputs for the CLI refresh-prompt core: the memory
 * directory and recent session snippets, from the OpenClaw state directory
 * (e.g. ~/.openclaw). The CLI reads the memory markdown itself.
 */
export function collectOpenClawContext(
  stateDir: string,
  logger: Logger,
  options?: { agentName?: string }
): RefreshContext {
  const agentName = options?.agentName ?? 'main';
  const memoryDir = join(stateDir, ...MEMORY_DIR_REL);
  return {
    memoryDirs: existsSync(memoryDir) ? [memoryDir] : [],
    sessionSnippets: readSessionSnippets(stateDir, agentName, logger),
  };
}

/**
 * Read the most recent session transcript for the agent and extract recent
 * user-driven topics — what the user is actually working on. Skips EigenFlux
 * system payloads (feed/refresh/PM) so broadcasts don't leak back in as
 * "session" signal.
 */
function readSessionSnippets(stateDir: string, agentName: string, logger: Logger): string[] {
  try {
    const dir = join(stateDir, 'agents', agentName, 'sessions');
    const latest = latestSessionFile(dir);
    if (!latest) return [];

    const lines = readFileSync(latest, 'utf-8').split('\n');
    const snippets: string[] = [];
    // Walk newest-first, collecting user/assistant text turns.
    for (let i = lines.length - 1; i >= 0 && snippets.length < MAX_SESSION_TURNS; i -= 1) {
      const line = lines[i].trim();
      if (!line) continue;
      const text = extractTurnText(line);
      if (text) snippets.push(text);
    }
    return snippets.reverse();
  } catch (err) {
    logger.debug(`readSessionSnippets: ${err instanceof Error ? err.message : String(err)}`);
    return [];
  }
}

function latestSessionFile(dir: string): string | undefined {
  let newest: { path: string; mtime: number } | undefined;
  let entries: string[];
  try {
    entries = readdirSync(dir);
  } catch {
    return undefined;
  }
  for (const name of entries) {
    if (!name.endsWith('.jsonl') || name.endsWith('.trajectory.jsonl')) continue;
    const path = join(dir, name);
    try {
      const mtime = statSync(path).mtimeMs;
      if (!newest || mtime > newest.mtime) newest = { path, mtime };
    } catch {
      // skip unreadable entries
    }
  }
  return newest?.path;
}

/**
 * Pull human-meaningful text out of one transcript JSONL line. Returns undefined
 * for tool calls, sentinels, and EigenFlux system payloads.
 */
function extractTurnText(line: string): string | undefined {
  let obj: unknown;
  try {
    obj = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (!obj || typeof obj !== 'object') return undefined;
  const message = (obj as { message?: unknown }).message;
  if (!message || typeof message !== 'object') return undefined;

  const role = (message as { role?: unknown }).role;
  if (role !== 'user' && role !== 'assistant') return undefined;

  const content = (message as { content?: unknown }).content;
  let text = '';
  if (typeof content === 'string') {
    text = content;
  } else if (Array.isArray(content)) {
    text = content
      .filter((c): c is { type: string; text: string } =>
        !!c && typeof c === 'object' && (c as { type?: unknown }).type === 'text' &&
        typeof (c as { text?: unknown }).text === 'string')
      .map((c) => c.text)
      .join(' ');
  }
  text = text.trim();
  if (!text) return undefined;

  // Drop EigenFlux system payloads and bare sentinels — they are noise here.
  if (/EIGENFLUX_FEED_PAYLOAD|EIGENFLUX PROFILE REVIEW TASK|profile is due for|EigenFlux feed payload/i.test(text)) return undefined;
  if (/^(NO_REPLY|HEARTBEAT_OK|Triggered a silent profile refresh)/.test(text)) return undefined;

  const oneLine = text.replace(/\s+/g, ' ');
  return oneLine.length > MAX_SESSION_SNIPPET_CHARS
    ? `${oneLine.slice(0, MAX_SESSION_SNIPPET_CHARS)}…`
    : oneLine;
}

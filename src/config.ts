/**
 * Configuration for the EigenFlux plugin.
 *
 * Server management is now handled by the eigenflux CLI.
 * The plugin config only holds plugin-level settings and
 * per-server notification routing overrides.
 */

import * as os from 'os';
import * as path from 'path';

import { Logger } from './logger';
import { normalizeReplyTarget } from './reply-target';
import { execEigenflux } from './cli-executor';

const PLUGIN_VERSION = '0.0.46';
// Minimum eigenflux CLI version this plugin build expects. When the installed
// CLI is older, the discovery service nudges the agent to update it (the agent
// updates the CLI at runtime; nothing is auto-run at install time). Bump this
// when the plugin starts relying on newer CLI behavior.
const EXPECTED_CLI_VERSION = '0.0.46';
const DEFAULT_EIGENFLUX_BIN = 'eigenflux';
const DEFAULT_SESSION_KEY = 'main';
const DEFAULT_AGENT_ID = 'main';
const DEFAULT_OPENCLAW_CLI_BIN = 'openclaw';
const HOST_KIND = 'openclaw';

// ─── Types ──────────────────────────────────────────────────────────────────

export type NotificationRouteOverrides = {
  sessionKey: boolean;
  agentId: boolean;
  replyChannel: boolean;
  replyTo: boolean;
  replyAccountId: boolean;
};

export type RoutingConfig = {
  sessionKey: string;
  agentId: string;
  replyChannel?: string;
  replyTo?: string;
  replyAccountId?: string;
  routeOverrides: NotificationRouteOverrides;
};

export type DiscoveredServer = {
  name: string;
  endpoint: string;
  stream_endpoint?: string;
  current: boolean;
};

export type EigenFluxPluginConfig = {
  eigenfluxBin?: string;
  skills?: string[];
  openclawCliBin?: string;
  serverRouting?: Record<string, {
    sessionKey?: string;
    agentId?: string;
    replyChannel?: string;
    replyTo?: string;
    replyAccountId?: string;
  }>;
};

export type ResolvedEigenFluxPluginConfig = {
  eigenfluxBin: string;
  skills: string[];
  openclawCliBin: string;
  serverRouting: Record<string, RoutingConfig>;
};

type DerivedNotificationRoute = {
  agentId?: string;
  replyChannel?: string;
  replyTo?: string;
  replyAccountId?: string;
};

// ─── Helpers ────────────────────────────────────────────────────────────────

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readNonEmptyString(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function isSessionPeerShape(value: string | undefined): boolean {
  const normalized = value?.trim().toLowerCase();
  return (
    normalized === 'direct' ||
    normalized === 'dm' ||
    normalized === 'group' ||
    normalized === 'channel'
  );
}

function deriveNotificationRoute(sessionKey: string | undefined): DerivedNotificationRoute {
  const trimmed = readNonEmptyString(sessionKey);
  if (!trimmed) {
    return {};
  }

  const parts = trimmed.split(':').filter((part) => part.length > 0);
  if (parts.length < 3 || parts[0]?.toLowerCase() !== 'agent') {
    return {};
  }

  const agentId = readNonEmptyString(parts[1]);
  if (parts.length >= 6 && isSessionPeerShape(parts[4])) {
    return {
      agentId,
      replyChannel: readNonEmptyString(parts[2]),
      replyAccountId: readNonEmptyString(parts[3]),
      replyTo: normalizeReplyTarget(parts.slice(5).join(':'), {
        channel: readNonEmptyString(parts[2]),
        sessionKey: trimmed,
      }),
    };
  }

  if (parts.length >= 5 && isSessionPeerShape(parts[3])) {
    return {
      agentId,
      replyChannel: readNonEmptyString(parts[2]),
      replyTo: normalizeReplyTarget(parts.slice(4).join(':'), {
        channel: readNonEmptyString(parts[2]),
        sessionKey: trimmed,
      }),
    };
  }

  return { agentId };
}

function createRouteOverrides(
  normalized: Record<string, unknown>
): NotificationRouteOverrides {
  const sessionKey = readNonEmptyString(normalized.sessionKey);
  const agentId = readNonEmptyString(normalized.agentId);
  const replyChannel = readNonEmptyString(normalized.replyChannel);
  const replyTo = readNonEmptyString(normalized.replyTo);
  const replyAccountId = readNonEmptyString(normalized.replyAccountId);

  return {
    sessionKey: sessionKey !== undefined && sessionKey !== DEFAULT_SESSION_KEY,
    agentId:
      agentId !== undefined &&
      agentId !== DEFAULT_AGENT_ID &&
      !(sessionKey && deriveNotificationRoute(sessionKey).agentId === agentId),
    replyChannel: replyChannel !== undefined,
    replyTo: replyTo !== undefined,
    replyAccountId: replyAccountId !== undefined,
  };
}

// ─── Server Discovery ───────────────────────────────────────────────────────

export type DiscoveryResult =
  | { kind: 'ok'; servers: DiscoveredServer[] }
  | { kind: 'not_installed'; bin: string };

export async function discoverServers(
  eigenfluxBin: string,
  logger?: Logger
): Promise<DiscoveryResult> {
  const result = await execEigenflux<DiscoveredServer[]>(
    eigenfluxBin,
    ['server', 'list', '--format', 'json'],
    { logger }
  );

  if (result.kind === 'success') {
    if (Array.isArray(result.data)) {
      return { kind: 'ok', servers: result.data };
    }
    logger?.warn('eigenflux server list returned non-array data');
    return { kind: 'ok', servers: [] };
  }

  if (result.kind === 'not_installed') {
    return { kind: 'not_installed', bin: result.bin };
  }

  if (result.kind === 'auth_required') {
    logger?.warn('eigenflux server list: auth required (unexpected)');
    return { kind: 'ok', servers: [] };
  }

  logger?.error(`eigenflux server list failed: ${result.error.message}`);
  return { kind: 'ok', servers: [] };
}

/**
 * Read the installed CLI version via `eigenflux version` (its JSON output
 * includes `cli_version`). Returns null when the CLI is missing or the version
 * can't be determined — callers treat null as "don't nag".
 */
export async function getInstalledCliVersion(
  eigenfluxBin: string,
  logger?: Logger
): Promise<string | null> {
  const result = await execEigenflux<{ cli_version?: string }>(
    eigenfluxBin,
    ['version'],
    { logger }
  );
  if (result.kind === 'success' && typeof result.data?.cli_version === 'string') {
    return result.data.cli_version;
  }
  return null;
}

/**
 * True when `installed` is an older semver than `target` (compares
 * major.minor.patch). Unknown/unparseable input returns false so we never nag
 * on bad data.
 */
export function isCliOutdated(installed: string | null, target: string): boolean {
  if (!installed) return false;
  const parse = (v: string): number[] =>
    v.split('.').slice(0, 3).map((part) => parseInt(part, 10));
  const a = parse(installed);
  const b = parse(target);
  for (let i = 0; i < 3; i++) {
    const x = a[i] ?? 0;
    const y = b[i] ?? 0;
    if (Number.isNaN(x) || Number.isNaN(y)) return false;
    if (x !== y) return x < y;
  }
  return false;
}

// ─── EigenFlux Home ─────────────────────────────────────────────────────────

export function resolveEigenfluxHome(baseDir?: string): string {
  const envHome = process.env.EIGENFLUX_HOME;
  if (envHome) {
    const expanded = expandHomeDir(envHome);
    if (!expanded.endsWith('.eigenflux')) {
      return path.join(expanded, '.eigenflux');
    }
    return expanded;
  }
  if (baseDir) {
    return path.join(baseDir, '.eigenflux');
  }
  return path.join(os.homedir(), '.eigenflux');
}

// ─── Config Resolution ──────────────────────────────────────────────────────

function resolveRoutingConfig(
  raw: Record<string, unknown> | undefined,
  logger?: Logger
): RoutingConfig {
  const normalized = isRecord(raw) ? raw : {};
  const sessionKey = readNonEmptyString(normalized.sessionKey) ?? DEFAULT_SESSION_KEY;
  const derivedRoute = deriveNotificationRoute(sessionKey);
  const replyChannel = readNonEmptyString(normalized.replyChannel) ?? derivedRoute.replyChannel;
  const replyTo =
    normalizeReplyTarget(readNonEmptyString(normalized.replyTo), {
      channel: replyChannel,
      sessionKey,
    }) ?? derivedRoute.replyTo;

  return {
    sessionKey,
    agentId: readNonEmptyString(normalized.agentId) ?? derivedRoute.agentId ?? DEFAULT_AGENT_ID,
    replyChannel,
    replyTo,
    replyAccountId:
      readNonEmptyString(normalized.replyAccountId) ?? derivedRoute.replyAccountId,
    routeOverrides: createRouteOverrides(normalized),
  };
}

export function resolvePluginConfig(
  pluginConfig: unknown,
  logger?: Logger
): ResolvedEigenFluxPluginConfig {
  const normalized = isRecord(pluginConfig) ? pluginConfig : {};

  const rawRouting = isRecord(normalized.serverRouting) ? normalized.serverRouting : {};
  const serverRouting: Record<string, RoutingConfig> = {};
  for (const [serverName, rawConfig] of Object.entries(rawRouting)) {
    serverRouting[serverName] = resolveRoutingConfig(
      isRecord(rawConfig) ? rawConfig : undefined,
      logger
    );
  }

  const rawSkills = Array.isArray(normalized.skills)
    ? normalized.skills.filter((s): s is string => typeof s === 'string' && s.trim().length > 0)
    : ['ef-broadcast', 'ef-communication'];

  return {
    eigenfluxBin: readNonEmptyString(normalized.eigenfluxBin) ?? DEFAULT_EIGENFLUX_BIN,
    skills: rawSkills,
    openclawCliBin:
      readNonEmptyString(normalized.openclawCliBin) ?? DEFAULT_OPENCLAW_CLI_BIN,
    serverRouting,
  };
}

// ─── Exports ────────────────────────────────────────────────────────────────

export function expandHomeDir(input: string): string {
  if (input === '~') {
    return os.homedir();
  }
  if (input.startsWith('~/')) {
    return path.join(os.homedir(), input.slice(2));
  }
  return input;
}

export const PLUGIN_CONFIG = {
  DEFAULT_EIGENFLUX_BIN,
  DEFAULT_SESSION_KEY,
  DEFAULT_AGENT_ID,
  DEFAULT_OPENCLAW_CLI_BIN,
  HOST_KIND,
  PLUGIN_VERSION,
  EXPECTED_CLI_VERSION,
} as const;

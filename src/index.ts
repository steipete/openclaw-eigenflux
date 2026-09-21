import * as path from 'node:path';
import * as fs from 'node:fs';
import * as os from 'os';

import type { OpenClawPluginApi } from 'openclaw/plugin-sdk/plugin-entry';
import { buildJsonPluginConfigSchema, definePluginEntry } from 'openclaw/plugin-sdk/plugin-entry';
import type { PluginLogger } from 'openclaw/plugin-sdk/plugin-entry';

import {
  EigenFluxPollingClient,
  readPollIntervalSec,
  type AuthRequiredEvent,
  type FeedResponse,
} from './polling-client';
import { OrderNotifications } from './order-notifications';
import { createOrderNotificationRequest } from './order-notification-api';
import { EigenFluxStreamClient, type PmStreamEvent } from './stream-client';
import { EigenFluxProfileRefresher } from './profile-refresher';
import { collectOpenClawContext, resolveOpenClawStateDir, EMPTY_CONTEXT } from './openclaw-context';
import { EigenFluxSettingsReporter } from './settings-reporter';
import { resolveRuntimeHost } from './runtime-identity';
import { registerRuntimeModelHooks } from './runtime-model';
import { execEigenflux } from './cli-executor';
import { Logger } from './logger';
import { CredentialsLoader } from './credentials-loader';
import {
  PLUGIN_CONFIG,
  resolvePluginConfig,
  resolveEigenfluxHome,
  discoverServers,
  getInstalledCliVersion,
  isCliOutdated,
  type ResolvedEigenFluxPluginConfig,
  type RoutingConfig,
  type DiscoveredServer,
} from './config';
import { findSessionRouteForBinding } from './notification-route-resolver';
import {
  buildAuthRequiredPromptTemplate,
  buildFeedPayloadPromptTemplate,
  buildHeartbeatExecutionPromptTemplate,
  buildNotInstalledPromptTemplate,
  INSTALL_ENTRY_URL,
  buildOutdatedPromptTemplate,
  buildPmStreamEventPromptTemplate,
  buildOrderNotificationPromptTemplate,
  type EigenFluxPromptServerContext,
} from './agent-prompt-templates';
import { FeedPushScheduler } from './feed-push-scheduler';
import { EigenFluxHeartbeatPlanRunner, type HeartbeatExecutionPlan } from './heartbeat-plan-runner';
import { EigenFluxNotifier } from './notifier';
import { buildPmLane, buildPmSessionKey, splitPmEventByConversation } from './pm-delivery';

/** "Recently active" window for the busy-aware feed push: the main session
 *  counts as busy while its last activity is fresher than this. Each user turn
 *  refreshes it, so the push waits for an actual lull in the conversation. */
const FEED_RECENT_ACTIVITY_MS = 90_000;
import { normalizeReplyTarget } from './reply-target';
import { writeStoredNotificationRoute, type PluginRuntimeStore } from './session-route-memory';
import { handleFollowup, FOLLOWUP_KINDS } from './feedback-tool';
import { FeedbackFlushLoop } from './feedback-flush-loop';

type JsonRecord = Record<string, unknown>;

type JsonApiSuccess<T extends JsonRecord> = {
  code: number;
  msg: string;
  data: T;
};

type ProfileResponseData = {
  agent: JsonRecord;
  profile: JsonRecord;
  influence: JsonRecord;
};

type CommandRouteContext = {
  channel?: string;
  to?: string;
  from?: string;
  accountId?: string;
  getCurrentConversationBinding?: () => Promise<{
    channel: string;
    accountId: string;
    conversationId: string;
    parentConversationId?: string;
  } | null>;
};

type ServerRuntime = {
  server: DiscoveredServer;
  routing: RoutingConfig;
  credentialsLoader: CredentialsLoader;
  notifier: EigenFluxNotifier;
  feedPoller: EigenFluxPollingClient;
  streamClient: EigenFluxStreamClient;
  orderNotifications: OrderNotifications;
  profileRefresher: EigenFluxProfileRefresher;
  settingsReporter: EigenFluxSettingsReporter;
  flushLoop: FeedbackFlushLoop;
  feedPushScheduler: FeedPushScheduler;
  getPromptContext: () => EigenFluxPromptServerContext;
  waitForPendingDelivery: () => Promise<void>;
};

type ParsedCommandArgs = {
  command: string;
  serverName?: string;
};

type ServerRuntimeSelection = {
  runtime?: ServerRuntime;
  error?: string;
};

const COMMAND_NAMES = ['auth', 'profile', 'refresh', 'servers', 'feed', 'pm', 'here', 'version'] as const;
const COMMAND_NAME_SET = new Set<string>(COMMAND_NAMES);

const DEFAULT_ROUTING: RoutingConfig = {
  sessionKey: PLUGIN_CONFIG.DEFAULT_SESSION_KEY,
  agentId: PLUGIN_CONFIG.DEFAULT_AGENT_ID,
  routeOverrides: {
    sessionKey: false,
    agentId: false,
    replyChannel: false,
    replyTo: false,
    replyAccountId: false,
  },
};

/**
 * Best-effort skill auto-update for the OpenClaw host. The CLI installs the
 * signed R2 bundle into OpenClaw's user-level skill directory
 * (`~/.agents/skills` by default). The package therefore carries no embedded
 * Skills and future Skill releases do not require a plugin release.
 *
 * The small signed manifest is checked on each call; the bundle is downloaded
 * only when its revision changes. Failures are swallowed and logged so startup
 * and polling remain offline-safe.
 */
type SkillsSnapshotRefresher = () => void | Promise<void>;

async function refreshOpenClawSkillsSnapshot(logger: Logger): Promise<void> {
  try {
    // Keep compatibility with older OpenClaw releases that do not export this
    // helper. Their built-in Skills watcher still refreshes the next turn.
    const moduleName = 'openclaw/plugin-sdk/skills-runtime';
    const runtime = await import(moduleName) as {
      bumpSkillsSnapshotVersion?: (params: { reason: string }) => number;
    };
    runtime.bumpSkillsSnapshotVersion?.({ reason: 'eigenflux-skills-sync' });
    logger.debug('OpenClaw Skills snapshot refreshed');
  } catch (err) {
    logger.debug(`OpenClaw Skills snapshot refresh unavailable: ${String(err)}`);
  }
}

export async function syncPluginSkills(
  eigenfluxBin: string,
  logger: Logger,
  refreshSnapshot: SkillsSnapshotRefresher = () => refreshOpenClawSkillsSnapshot(logger)
): Promise<void> {
  try {
    const res = await execEigenflux(
      eigenfluxBin,
      ['skills', 'sync', '--if-stale', '--quiet', '--host', 'openclaw'],
      { logger, parseJson: false }
    );
    if (res.kind === 'success') {
      await refreshSnapshot();
      logger.info('Skill auto-sync ok (host=openclaw)');
    } else {
      logger.warn(`Skill auto-sync skipped (kind=${res.kind}, host=openclaw)`);
    }
  } catch (err) {
    logger.warn(`Skill auto-sync error: ${String(err)}`);
  }
}

function registerPlugin(api: OpenClawPluginApi): void {
  const logger = new Logger(resolvePluginLogger(api));

  const pluginConfig = resolvePluginConfig(api.pluginConfig, logger);
  const eigenfluxHome = resolveEigenfluxHome(api.rootDir);
  logger.info(
    `EigenFlux home resolved: path=${eigenfluxHome}, source=${process.env.EIGENFLUX_HOME ? 'EIGENFLUX_HOME env' : api.rootDir ? 'api.rootDir' : 'os.homedir()'}, rootDir=${api.rootDir ?? 'undefined'}, homedir=${os.homedir()}`
  );
  // Set once at startup so all CLI child processes inherit it automatically.
  process.env.EIGENFLUX_HOME = eigenfluxHome;
  process.env.EIGENFLUX_HOST = resolveRuntimeHost(api.runtime?.version, process.env.EIGENFLUX_HOST_OVERRIDE);
  process.env.EIGENFLUX_MODE = 'plugin';
  process.env.EIGENFLUX_PLUGIN_VERSION = PLUGIN_CONFIG.PLUGIN_VERSION;
  logger.info(`Client env: EIGENFLUX_HOST=${process.env.EIGENFLUX_HOST}`);
  const store = createInMemoryPluginStore();

  let runtimes: ServerRuntime[] = [];
  registerRuntimeModelHooks(api, () => runtimes, logger);
  let notInstalledPromptDelivered = false;
  let outdatedPromptDelivered = false;

  // Register a single meta-service that discovers servers on start
  api.registerService({
    id: 'eigenflux:discovery',
    start: async () => {
      logger.info('Starting EigenFlux discovery service...');

      const discovery = await discoverServers(pluginConfig.eigenfluxBin, logger);
      if (discovery.kind === 'not_installed') {
        logger.warn(
          `EigenFlux CLI not installed (bin=${discovery.bin}); delivering install prompt to user`
        );
        if (!notInstalledPromptDelivered) {
          notInstalledPromptDelivered = true;
          await deliverNotInstalledPrompt(api, logger, pluginConfig, eigenfluxHome, discovery.bin, store);
        }
        return;
      }

      // Pull the latest signed bundle into OpenClaw's user-level Skills dir.
      // Non-blocking and offline-safe; a successful sync refreshes the Skills
      // snapshot for the next agent turn.
      void syncPluginSkills(pluginConfig.eigenfluxBin, logger);

      const servers = discovery.servers;
      if (servers.length === 0) {
        logger.warn('No EigenFlux servers discovered; services will not start');
        return;
      }

      logger.info(`Discovered ${servers.length} server(s): ${servers.map((s) => s.name).join(', ')}`);

      // Derive EIGENFLUX_CHANNEL from the first server's routing config.
      if (!process.env.EIGENFLUX_CHANNEL) {
        const firstRouting = pluginConfig.serverRouting[servers[0].name];
        const channel = firstRouting?.replyChannel;
        process.env.EIGENFLUX_CHANNEL = channel || 'openclaw';
        logger.info(`Client env: EIGENFLUX_CHANNEL=${process.env.EIGENFLUX_CHANNEL} (source=${channel ? 'routing.replyChannel' : 'default'})`);
      }

      runtimes = servers.map((server) =>
        createServerRuntime(api, logger, pluginConfig, server, eigenfluxHome, store)
      );

      for (const runtime of runtimes) {
        logger.info(`Starting services for server=${runtime.server.name}`);
        runtime.profileRefresher.start();
        await runtime.feedPoller.start();
        await runtime.streamClient.start();
        runtime.orderNotifications.start();
        runtime.flushLoop.start();
      }

      // CLI is installed and running; if it's older than this plugin expects,
      // nudge the agent to update it (informational — services keep running on
      // the current version; the agent performs the update at runtime).
      if (!outdatedPromptDelivered) {
        const installedVersion = await getInstalledCliVersion(pluginConfig.eigenfluxBin, logger);
        if (isCliOutdated(installedVersion, PLUGIN_CONFIG.EXPECTED_CLI_VERSION)) {
          outdatedPromptDelivered = true;
          logger.warn(
            `EigenFlux CLI outdated (installed=${installedVersion}, expected>=${PLUGIN_CONFIG.EXPECTED_CLI_VERSION}); delivering upgrade prompt`
          );
          await deliverOutdatedPrompt(
            api,
            logger,
            pluginConfig,
            installedVersion as string,
            PLUGIN_CONFIG.EXPECTED_CLI_VERSION,
            store
          );
        }
      }
    },
    stop: async () => {
      logger.info('Stopping EigenFlux discovery service...');
      for (const runtime of runtimes) {
        logger.info(`Stopping services for server=${runtime.server.name}`);
        runtime.feedPoller.stop();
        runtime.orderNotifications.stop();
        runtime.feedPushScheduler.stop();
        await runtime.waitForPendingDelivery();
        await runtime.notifier.drainPendingCleanups();
        await runtime.streamClient.stop();
        runtime.profileRefresher.stop();
        runtime.flushLoop.stop();
      }
      runtimes = [];
      notInstalledPromptDelivered = false;
      outdatedPromptDelivered = false;
    },
  });

  registerFollowupTool(api, logger, pluginConfig.eigenfluxBin, () => runtimes);

  registerCommand(
    api,
    logger,
    pluginConfig,
    eigenfluxHome,
    store,
    () => runtimes,
    (next) => {
      runtimes = next;
    }
  );
}

/**
 * Register the eigenflux__followup tool with the OpenClaw agent runtime. The
 * handler is a thin shell that shells out to `eigenflux feed event record`;
 * the server is selected via a server_id arg (or the only server when one
 * exists). All verification/enrichment/dedup/queueing lives in the CLI.
 */
function registerFollowupTool(
  api: OpenClawPluginApi,
  logger: Logger,
  eigenfluxBin: string,
  getRuntimes: () => ServerRuntime[]
): void {
  if (!api.registerTool) {
    logger.warn('registerTool API unavailable; skipping eigenflux__followup tool');
    return;
  }

  // TypeBox schema literal, accepted at runtime as a JSON-Schema-shaped object.
  // Plain literal (not a TypeBox builder) so we don't pull typebox into deps.
  const parameters = {
    type: 'object',
    additionalProperties: false,
    properties: {
      item_id: {
        type: 'string',
        description:
          'A single item_id passed to the CLI.',
      },
      item_ids: {
        type: 'array',
        items: { type: 'string' },
        description:
          'Batch form of item_id. When both item_id and item_ids are supplied, item_ids wins. The CLI validates the batch.',
      },
      kind: {
        type: 'string',
        enum: [...FOLLOWUP_KINDS],
        description:
          'Event kind passed to the CLI for every item in this call.',
      },
      brief: {
        type: 'string',
        description: 'Optional one-line context for backend audit. Capped at 200 chars. Applies to every item in the batch.',
      },
      server_id: {
        type: 'string',
        description:
          "Required when multiple servers are configured; otherwise uses the only discovered server.",
      },
    },
    required: ['kind'],
  } as unknown as never;

  const resolveRuntime = (rawServerId: unknown): ServerRuntime | undefined => {
    const runtimes = getRuntimes();
    if (runtimes.length === 0) return undefined;
    if (typeof rawServerId === 'string' && rawServerId.length > 0) {
      return runtimes.find((r) => r.server.name === rawServerId);
    }
    return runtimes.length === 1 ? runtimes[0] : undefined;
  };

  try {
    api.registerTool({
      name: 'eigenflux__followup',
      label: 'EigenFlux feedback',
      description:
        'Adapter for eigenflux feed event record. Use according to the current EigenFlux Skills and CLI plan. ' +
        'Pass item identifiers and event arguments through to the CLI.',
      parameters,
      execute: async (_toolCallId: string, params: unknown) => {
        const raw = (params ?? {}) as {
          item_id?: unknown;
          item_ids?: unknown;
          kind?: unknown;
          brief?: unknown;
          server_id?: unknown;
        };
        const runtime = resolveRuntime(raw.server_id);
        if (!runtime) {
          const text = JSON.stringify({ ok: false, error: 'no_runtime' });
          return { content: [{ type: 'text', text }], details: { ok: false } };
        }
        // Thin shell: shell out to `eigenflux feed event record`; the CLI owns
        // verification/enrichment/dedup/queueing. Nudge the flush loop so the
        // just-recorded event drains promptly.
        const result = await handleFollowup(
          {
            eigenfluxBin,
            serverName: runtime.server.name,
            logger,
          },
          raw
        );
        runtime.flushLoop.kick();
        return {
          content: [{ type: 'text', text: JSON.stringify(result) }],
          details: result,
        };
      },
    } as never);
    logger.info('Registered eigenflux__followup tool');
  } catch (err) {
    logger.warn(
      `Failed to register eigenflux__followup tool: ${err instanceof Error ? err.message : String(err)}`
    );
  }
}

function resolvePluginLogger(api: OpenClawPluginApi): PluginLogger {
  const runtimeLogging = (api.runtime as
    | {
        logging?: {
          getChildLogger?: (bindings: Record<string, unknown>) => unknown;
        };
      }
    | undefined)?.logging;

  if (runtimeLogging && typeof runtimeLogging.getChildLogger === 'function') {
    try {
      const child = runtimeLogging.getChildLogger({ plugin: 'eigenflux' });
      if (child) {
        return child as PluginLogger;
      }
    } catch {
      // fall through to api.logger
    }
  }
  return api.logger;
}

const PLUGIN_CONFIG_SCHEMA = buildJsonPluginConfigSchema({
  type: 'object',
  additionalProperties: false,
  properties: {
    eigenfluxBin: { type: 'string' },
    openclawCliBin: { type: 'string' },
    skills: { type: 'array', items: { type: 'string' } },
    serverRouting: {
      type: 'object',
      additionalProperties: {
        type: 'object',
        additionalProperties: false,
        properties: {
          sessionKey: { type: 'string' },
          agentId: { type: 'string' },
          replyChannel: { type: 'string' },
          replyTo: { type: 'string' },
          replyAccountId: { type: 'string' },
        },
      },
    },
  },
});

export default definePluginEntry({
  id: 'openclaw-eigenflux',
  name: 'EigenFlux',
  description: 'OpenClaw extension for EigenFlux with CLI-based feed polling and PM streaming',
  configSchema: PLUGIN_CONFIG_SCHEMA,
  register(api) {
    if (api.registrationMode && api.registrationMode !== 'full') return;
    registerPlugin(api);
  },
});

const INSTALL_COMMAND = 'curl -fsSL https://eigenflux.ai/install.sh | bash';
const INSTALL_GUIDANCE = `Read ${INSTALL_ENTRY_URL} and follow it to install EigenFlux for this OpenClaw Agent.`;

async function deliverNotInstalledPrompt(
  api: OpenClawPluginApi,
  logger: Logger,
  pluginConfig: ResolvedEigenFluxPluginConfig,
  _eigenfluxHome: string,
  bin: string,
  store: PluginRuntimeStore
): Promise<void> {
  // Intentionally no workdir: the bootstrap notifier must not read or persist
  // any remembered session route under <eigenfluxHome>/bootstrap.
  const notifier = new EigenFluxNotifier(api, logger, {
    sessionKey: DEFAULT_ROUTING.sessionKey,
    agentId: DEFAULT_ROUTING.agentId,
    replyChannel: DEFAULT_ROUTING.replyChannel,
    replyTo: DEFAULT_ROUTING.replyTo,
    replyAccountId: DEFAULT_ROUTING.replyAccountId,
    openclawCliBin: pluginConfig.openclawCliBin,
    routeOverrides: DEFAULT_ROUTING.routeOverrides,
  });

  await notifier.deliver(
    buildNotInstalledPromptTemplate({ bin, installCommand: INSTALL_GUIDANCE })
  );
}

async function deliverOutdatedPrompt(
  api: OpenClawPluginApi,
  logger: Logger,
  pluginConfig: ResolvedEigenFluxPluginConfig,
  installed: string,
  expected: string,
  _store: PluginRuntimeStore
): Promise<void> {
  // Same bootstrap notifier as deliverNotInstalledPrompt: no workdir, default
  // routing — this is a one-off nudge, not a per-server feed delivery.
  const notifier = new EigenFluxNotifier(api, logger, {
    sessionKey: DEFAULT_ROUTING.sessionKey,
    agentId: DEFAULT_ROUTING.agentId,
    replyChannel: DEFAULT_ROUTING.replyChannel,
    replyTo: DEFAULT_ROUTING.replyTo,
    replyAccountId: DEFAULT_ROUTING.replyAccountId,
    openclawCliBin: pluginConfig.openclawCliBin,
    routeOverrides: DEFAULT_ROUTING.routeOverrides,
  });

  await notifier.deliver(
    buildOutdatedPromptTemplate({ installed, expected, updateCommand: INSTALL_COMMAND })
  );
}

/** Dedicated session key for feed delivery, isolated from the main DM session. */
function buildFeedSessionKey(serverName: string): string {
  return `eigenflux:feed:${serverName}`;
}

function createServerRuntime(
  api: OpenClawPluginApi,
  logger: Logger,
  pluginConfig: ResolvedEigenFluxPluginConfig,
  server: DiscoveredServer,
  eigenfluxHome: string,
  store: PluginRuntimeStore
): ServerRuntime {
  const routing = pluginConfig.serverRouting[server.name] ?? DEFAULT_ROUTING;

  const credentialsLoader = new CredentialsLoader(logger, eigenfluxHome, server.name);

  // Feedback collection is downsunk into the CLI (`feed event record` verifies
  // item_ids against its own broadcast cache, enriches, dedups, queues on disk,
  // and opportunistically flushes). The plugin's only resident piece is the
  // retry cadence below, which drives `feed event flush` with back-off.
  const flushLoop = new FeedbackFlushLoop({
    serverName: server.name,
    eigenfluxBin: pluginConfig.eigenfluxBin,
    logger,
  });

  const notifier = new EigenFluxNotifier(api, logger, {
    store,
    eigenfluxBin: pluginConfig.eigenfluxBin,
    serverName: server.name,
    sessionKey: routing.sessionKey,
    agentId: routing.agentId,
    replyChannel: routing.replyChannel,
    replyTo: routing.replyTo,
    replyAccountId: routing.replyAccountId,
    openclawCliBin: pluginConfig.openclawCliBin,
    routeOverrides: routing.routeOverrides,
  });

  const getPromptContext = (): EigenFluxPromptServerContext => ({
    serverName: server.name,
    eigenfluxHome,
  });

  let lastAuthPromptKey: string | null = null;

  const resetAuthPromptGate = (): void => {
    lastAuthPromptKey = null;
  };

  const notifyAuthRequired = async (_authEvent: AuthRequiredEvent): Promise<void> => {
    const promptKey = `auth_required:${server.name}`;
    if (lastAuthPromptKey === promptKey) {
      logger.debug(`Skipping duplicate auth prompt for server=${server.name}`);
      return;
    }

    lastAuthPromptKey = promptKey;
    await notifier.deliver(
      buildAuthRequiredPromptTemplate({ context: getPromptContext() })
    );
  };

  // Pushes the agent's runtime mode to the backend once per heartbeat via the
  // eigenflux CLI (`settings push`). The CLI handles change-detection, dedup,
  // and reading feed_delivery_preference from its own config. Best-effort:
  // never interrupts polling.
  const settingsReporter = new EigenFluxSettingsReporter({
    serverName: server.name,
    eigenfluxBin: pluginConfig.eigenfluxBin,
    logger,
  });
  const heartbeatPlanRunner = new EigenFluxHeartbeatPlanRunner({
    eigenfluxBin: pluginConfig.eigenfluxBin,
    eigenfluxHome,
    serverName: server.name,
    logger,
  });
  // A successful plan is Agent work, not merely a plugin health check. Keep
  // the plan for exactly the poll that produced it; pollOnce is single-flight.
  let currentHeartbeatPlan: HeartbeatExecutionPlan | null = null;

  // Backpressure state for the LEGACY one-shot feed path only
  // (EIGENFLUX_FEED_DELIVERY=oneshot). The default 2a main-session path returns
  // before touching any of these — they stay at their initial values there, so
  // never read them outside the oneshot branch. Guard: notifier.deliver() may
  // take longer than the poll interval, so we skip overlapping deliveries to
  // avoid duplicate agent tasks.
  let feedDeliveryInFlight = false;
  let feedDeliveryStartedAt = 0;
  let feedDeliverySkipCount = 0;
  let activeFeedDelivery: Promise<boolean> | null = null;
  const FEED_DELIVERY_TIMEOUT_MS = 300_000;

  /** Overlap-guarded notifier.deliver() — shared by the default main-session
   *  push (via the scheduler) and the legacy one-shot path. */
  async function runGuardedFeedDelivery(
    prompt: string,
    options?: { targetSessionKey?: string },
    skipContext?: { items: number; notifications: number }
  ): Promise<void> {
    // Check for stale delivery flag (delivery promise hung)
    if (feedDeliveryInFlight && feedDeliveryStartedAt > 0) {
      const elapsed = Date.now() - feedDeliveryStartedAt;
      if (elapsed > FEED_DELIVERY_TIMEOUT_MS) {
        logger.error(
          `Feed delivery flag stuck for ${Math.round(elapsed / 1000)}s on server=${server.name}, force-resetting`
        );
        feedDeliveryInFlight = false;
        activeFeedDelivery = null;
      }
    }

    if (feedDeliveryInFlight) {
      feedDeliverySkipCount += 1;
      const elapsed = Date.now() - feedDeliveryStartedAt;
      logger.warn(
        `Skipping feed delivery for server=${server.name}: previous delivery still in progress ` +
        `(elapsed=${Math.round(elapsed / 1000)}s, skipped_items=${skipContext?.items ?? 'n/a'}, ` +
        `skipped_notifications=${skipContext?.notifications ?? 'n/a'}, total_skips=${feedDeliverySkipCount})`
      );
      return;
    }

    feedDeliveryInFlight = true;
    const startedAt = Date.now();
    feedDeliveryStartedAt = startedAt;
    // OpenClaw grants optional terminal replies only to its standard subagent lane.
    // Scope this to Feed/heartbeat runs; PM and order lanes retain their isolation.
    activeFeedDelivery = notifier.deliver(prompt, { ...options, lane: 'subagent' }).finally(() => {
      const duration = Date.now() - startedAt;
      logger.info(`Feed delivery completed for server=${server.name} in ${Math.round(duration / 1000)}s`);
      // Only clear flags if this delivery is still the current one.
      // A stale .finally() from a force-reset delivery must not clobber a newer delivery's state.
      if (feedDeliveryStartedAt === startedAt) {
        feedDeliveryInFlight = false;
        activeFeedDelivery = null;
      }
    });

    await activeFeedDelivery;
  }

  // Busy-aware push for the default mode: pick a quiet moment on the main
  // session instead of grabbing its lock while the user is mid-conversation.
  // See FeedPushScheduler for the full policy.
  const feedPushScheduler = new FeedPushScheduler({
    isBusy: async () =>
      feedDeliveryInFlight || notifier.isMainRouteBusy(FEED_RECENT_ACTIVITY_MS),
    pushNow: (prompt) => runGuardedFeedDelivery(prompt),
    logger,
    serverName: server.name,
  });

  const scheduleHeartbeatExecution = async (payload: FeedResponse): Promise<void> => {
    const hasPayload =
      (payload.data?.items?.length ?? 0) > 0 ||
      (payload.data?.notifications?.length ?? 0) > 0;
    const plan = currentHeartbeatPlan;
    if (!plan) {
      logger.warn(`Skipping Agent heartbeat for server=${server.name}: verified plan unavailable`);
      return;
    }

    if (!hasPayload && !plan.wake_on_empty) return;
    const prompt = buildHeartbeatExecutionPromptTemplate(plan.agent_prompt, payload, getPromptContext());
    switch (process.env.EIGENFLUX_FEED_DELIVERY) {
      case 'system-event':
        void notifier.deliverToMainSession(prompt).catch((err) =>
          logger.error(`Heartbeat main-session delivery error for server=${server.name}: ${String(err)}`)
        );
        return;
      case 'oneshot':
        await runGuardedFeedDelivery(
          prompt,
          { targetSessionKey: buildFeedSessionKey(server.name) },
          {
            items: payload.data?.items?.length ?? 0,
            notifications: payload.data?.notifications?.length ?? 0,
          }
        );
        return;
      default:
        feedPushScheduler.schedule(prompt);
    }
  };

  const orderAgentPath = path.join(eigenfluxHome, 'servers', server.name, 'agent-v2-credentials.json');
  const orderAgentID = fs.existsSync(orderAgentPath)
    ? String(JSON.parse(fs.readFileSync(orderAgentPath, 'utf8')).agent_id ?? '') : '';
  const orderNotifications = new OrderNotifications(createOrderNotificationRequest({
    eigenfluxBin: pluginConfig.eigenfluxBin, eigenfluxHome, serverName: server.name,
    endpoint: server.endpoint, agentID: orderAgentID, logger,
  }), (notification, receipt, checkpoint) => notifier.deliverOrder(buildOrderNotificationPromptTemplate(notification, getPromptContext()), {
    key: `${server.name}:${orderAgentID}:${notification.notification_id}`, receipt, checkpoint,
  }), /^[1-9]\d*$/.test(orderAgentID)
    ? path.join(eigenfluxHome, 'servers', server.name, 'data', `order-notifications-${orderAgentID}.json`)
    : undefined);

  const feedPoller = new EigenFluxPollingClient({
    resolveModel: () => settingsReporter.getObservedModel(),
    serverName: server.name,
    eigenfluxBin: pluginConfig.eigenfluxBin,
    resolvePollIntervalSec: () =>
      readPollIntervalSec(pluginConfig.eigenfluxBin, server.name, logger),
    logger,
    onHeartbeatStart: async () => {
      currentHeartbeatPlan = await heartbeatPlanRunner.run();
      if (currentHeartbeatPlan) await refreshOpenClawSkillsSnapshot(logger);
    },
    onFeedPolled: async (payload: FeedResponse) => {
      // Always reset auth gate on successful poll, even if delivery is skipped
      resetAuthPromptGate();

      const items = payload.data?.items ?? [];
      const notifications = payload.data?.notifications ?? [];

      // The CLI caches every feed response itself, so `feed event record` reads
      // item_ids straight from that cache — the plugin no longer mirrors them.

      // Delivery modes (EIGENFLUX_FEED_DELIVERY):
      //
      // (default)      — the original pre-oneshot path: notifier.deliver() runs
      //                  the agent ON the user's real main session via
      //                  runtime.subagent (deliver:true). The PLUGIN initiates
      //                  the run, so feed is an ACTIVE push with full user
      //                  context and no dependency on the host heartbeat
      //                  scheduler (which has been observed to stall for hours).
      // 'system-event' — mode 2a: enqueue into the main session + heartbeat
      //                  wake. Fully non-blocking, but delivery timing depends
      //                  on the host heartbeat actually firing; until that is
      //                  reliable this mode is opt-in only.
      // 'oneshot'      — legacy isolated one-shot session (no user context).
      if (currentHeartbeatPlan) {
        await scheduleHeartbeatExecution(payload);
        return;
      }

      const feedDeliveryMode = process.env.EIGENFLUX_FEED_DELIVERY;
      if (feedDeliveryMode === 'system-event') {
        // Nothing worth surfacing → don't wake the heartbeat at all.
        if (items.length === 0 && notifications.length === 0) {
          return;
        }
        // Fire-and-forget: enqueueSystemEvent is non-blocking and coalescing, so
        // the poll loop needs no backpressure guard and never stalls on delivery.
        void notifier
          .deliverToMainSession(buildFeedPayloadPromptTemplate(payload, getPromptContext()))
          .catch((err) =>
            logger.error(`Feed main-session delivery error for server=${server.name}: ${String(err)}`)
          );
        return;
      }

      const prompt = buildFeedPayloadPromptTemplate(payload, getPromptContext());

      if (feedDeliveryMode === 'oneshot') {
        // Legacy isolated path: unchanged semantics (immediate, awaited,
        // overlap-guarded with skip logging).
        await runGuardedFeedDelivery(
          prompt,
          { targetSessionKey: buildFeedSessionKey(server.name) },
          { items: items.length, notifications: notifications.length }
        );
        return;
      }

      // Default: busy-aware active push. Hold the payload while the user's
      // conversation is active and deliver at the next quiet moment; a newer
      // poll's payload supersedes the held one (latest batch wins).
      feedPushScheduler.schedule(prompt);
    },
    onPollSuccess: async (payload: FeedResponse) => {
      // Push local settings to the backend once per heartbeat (throttled
      // internally). Errors are swallowed inside report().
      await settingsReporter.report();
      // Nudge the feedback flush loop once per heartbeat so any queued events
      // (from an out-of-band `record`) drain even on an idle server. The loop
      // owns the back-off; this is just an opportunistic kick.
      flushLoop.kick();
      void profileRefresher.tick();
      // The CLI plan decides whether an empty poll needs an Agent turn.
      if ((payload.data?.items?.length ?? 0) === 0 && (payload.data?.notifications?.length ?? 0) === 0) {
        await scheduleHeartbeatExecution(payload);
      }
    },
    onAuthRequired: notifyAuthRequired,
  });

  const streamClient = new EigenFluxStreamClient({
    serverName: server.name,
    eigenfluxBin: pluginConfig.eigenfluxBin,
    logger,
    onPmEvent: async (event: PmStreamEvent) => {
      resetAuthPromptGate();
      if (event.type === 'notification_push' || event.type === 'commission_order_notification') {
        await orderNotifications.handle(event);
        return;
      }
      // Deliver when the event carries anything actionable. Friend events
      // (friend_request / friend_accepted) arrive with empty `messages`, so a
      // `messages.length > 0` gate would silently drop them.
      const data = event.data ?? {};
      const actionable =
        (data.messages?.length ?? 0) > 0 ||
        (data.friend_requests?.length ?? 0) > 0 ||
        (data.friend_responses?.length ?? 0) > 0 ||
        event.type === 'friend_accepted';
      if (actionable) {
        const batches = splitPmEventByConversation(event);
        await Promise.all(
          batches.map(({ conversationKey, peerKey, event: conversationEvent }) =>
            notifier.deliver(
              buildPmStreamEventPromptTemplate(conversationEvent, getPromptContext()),
              {
                persistentSessionKey: buildPmSessionKey(server.name, peerKey, conversationKey),
                lane: buildPmLane(server.name, peerKey, conversationKey),
              }
            )
          )
        );
      }
    },
    onAuthRequired: async () => {
      await notifyAuthRequired({ reason: 'auth_required' });
    },
  });

  const profileRefresher = new EigenFluxProfileRefresher({
    serverName: server.name,
    eigenfluxBin: pluginConfig.eigenfluxBin,
    logger,
    collectContext: () => {
      const stateDir = resolveOpenClawStateDir(logger);
      return stateDir ? collectOpenClawContext(stateDir, logger) : EMPTY_CONTEXT;
    },
    onRefreshPrompt: async (prompt: string) => {
      resetAuthPromptGate();
      await notifier.deliver(prompt);
    },
    onAuthRequired: async () => {
      await notifyAuthRequired({ reason: 'auth_required' });
    },
  });

  return {
    server,
    routing,
    credentialsLoader,
    notifier,
    feedPoller,
    streamClient,
    orderNotifications,
    profileRefresher,
    settingsReporter,
    flushLoop,
    feedPushScheduler,
    getPromptContext,
    async waitForPendingDelivery(): Promise<void> {
      if (activeFeedDelivery) {
        try {
          await activeFeedDelivery;
        } catch {
          // Swallow — we're stopping
        }
      }
    },
  };
}

// ─── Command Handler ────────────────────────────────────────────────────────

function registerCommand(
  api: OpenClawPluginApi,
  logger: Logger,
  pluginConfig: ResolvedEigenFluxPluginConfig,
  eigenfluxHome: string,
  store: PluginRuntimeStore,
  getRuntimes: () => ServerRuntime[],
  setRuntimes: (runtimes: ServerRuntime[]) => void
): void {
  if (!api.registerCommand) {
    logger.warn('registerCommand API unavailable; skipping /eigenflux command registration');
    return;
  }

  type EnsureRuntimesResult = {
    runtimes: ServerRuntime[];
    notInstalledBin?: string;
  };

  let inflightDiscovery: Promise<EnsureRuntimesResult> | null = null;

  const runDiscovery = async (): Promise<EnsureRuntimesResult> => {
    const discovery = await discoverServers(pluginConfig.eigenfluxBin, logger);
    if (discovery.kind === 'not_installed') {
      return { runtimes: getRuntimes(), notInstalledBin: discovery.bin };
    }
    if (discovery.servers.length === 0) {
      return { runtimes: getRuntimes() };
    }
    const created = discovery.servers.map((server) =>
      createServerRuntime(api, logger, pluginConfig, server, eigenfluxHome, store)
    );
    setRuntimes(created);
    return { runtimes: created };
  };

  const ensureRuntimes = async (): Promise<EnsureRuntimesResult> => {
    const existing = getRuntimes();
    if (existing.length > 0) {
      return { runtimes: existing };
    }
    if (!inflightDiscovery) {
      inflightDiscovery = runDiscovery().finally(() => {
        inflightDiscovery = null;
      });
    }
    return inflightDiscovery;
  };

  api.registerCommand({
    name: 'eigenflux',
    description: 'EigenFlux plugin commands: auth, profile, refresh, servers, feed, pm, here, version',
    acceptsArgs: true,
    handler: async (ctx) => {
      const parsed = parseCommandArgs(ctx.args);

      if (parsed.command === 'version') {
        return {
          text: await buildVersionText(pluginConfig.eigenfluxBin),
        };
      }

      const { runtimes, notInstalledBin } = await ensureRuntimes();

      if (notInstalledBin && runtimes.length === 0) {
        return {
          text: `EigenFlux CLI not installed (bin=${notInstalledBin}). ${INSTALL_GUIDANCE}`,
        };
      }

      if (parsed.command === 'servers') {
        return {
          text: buildServersText(runtimes),
        };
      }

      const selection = selectServerRuntime(runtimes, parsed.serverName);
      if (!selection.runtime) {
        return {
          text: selection.error ?? buildHelpText(runtimes),
        };
      }
      const runtime = selection.runtime;

      await rememberCurrentCommandRouteIfPossible(ctx, runtime, store, logger);

      switch (parsed.command) {
        case 'auth':
          return {
            text: buildAuthStatusText(runtime),
          };
        case 'profile':
          return {
            text: await buildProfileText(runtime, pluginConfig.eigenfluxBin),
          };
        case 'refresh': {
          // Request an immediate profile review through the CLI,
          // using the current Skills for output. Fire-and-forget — we do NOT await the
          // refresh, so the command always responds immediately even if the
          // background refresh is slow or stalls. Errors are logged, never a hang.
          //
          // We also probe the context synchronously and surface it in the reply,
          // since plugin logs are not easily visible: this confirms whether
          // memory/session are actually being read (and from which state dir).
          const probeStateDir = resolveOpenClawStateDir(logger);
          const probe = probeStateDir
            ? collectOpenClawContext(probeStateDir, logger)
            : EMPTY_CONTEXT;
          void runtime.profileRefresher.triggerNow().catch((err) => {
            logger.error(
              `Manual profile refresh failed for server=${runtime.server.name}: ${err instanceof Error ? err.message : String(err)}`
            );
          });
          return {
            text: [
              `Triggered a profile refresh check for server=${runtime.server.name} (running in background).`,
              `context probe: memory_dirs=${probe.memoryDirs.length}, session=${probe.sessionSnippets.length} snippet(s), stateDir=${probeStateDir ?? 'undefined'}`,
              'The current Skills control user-visible output. Verify via a new agent_bio_history row if the bio changed.',
            ].join('\n'),
          };
        }
        case 'feed':
          return {
            text: await buildFeedText(runtime),
          };
        case 'pm':
          return {
            text: buildPmStatusText(runtime),
          };
        case 'here':
          return {
            text: await buildHereText(ctx, runtime, store, logger),
          };
        default:
          return {
            text: buildHelpText(runtimes),
          };
      }
    },
  });
}

function parseCommandArgs(args: string | undefined): ParsedCommandArgs {
  const tokens = args?.trim().length ? args.trim().split(/\s+/u) : [];
  let serverName: string | undefined;
  const filtered: string[] = [];

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if ((token === '--server' || token === '-s') && tokens[index + 1]) {
      serverName = tokens[index + 1];
      index += 1;
      continue;
    }
    filtered.push(token);
  }

  const command = filtered[0]?.toLowerCase() ?? '';
  return {
    command,
    serverName,
  };
}

function selectServerRuntime(
  runtimes: ServerRuntime[],
  requestedServerName: string | undefined
): ServerRuntimeSelection {
  if (runtimes.length === 0) {
    return {
      error: 'No EigenFlux servers discovered. Ensure eigenflux CLI is configured with at least one server.',
    };
  }

  if (!requestedServerName) {
    return {
      runtime: runtimes[0],
    };
  }

  const normalizedRequestedName = requestedServerName.trim().toLowerCase();
  const runtime = runtimes.find(
    (item) => item.server.name.trim().toLowerCase() === normalizedRequestedName
  );
  if (runtime) {
    return { runtime };
  }

  return {
    error: [
      `Unknown EigenFlux server: ${requestedServerName}`,
      `Available servers: ${runtimes.map((item) => item.server.name).join(', ')}`,
    ].join('\n'),
  };
}

function buildServersText(runtimes: ServerRuntime[]): string {
  if (runtimes.length === 0) {
    return 'No EigenFlux servers discovered.';
  }

  return [
    'EigenFlux servers (discovered via CLI):',
    ...runtimes.map((runtime) => {
      const flags = [
        runtime.server.current ? 'default' : null,
        runtime.streamClient.isRunning() ? 'streaming' : null,
      ]
        .filter(Boolean)
        .join(', ');
      const suffix = flags ? ` (${flags})` : '';
      return `- ${runtime.server.name}: endpoint=${runtime.server.endpoint}${suffix}`;
    }),
  ].join('\n');
}

function buildHelpText(runtimes: ServerRuntime[]): string {
  const defaultRuntime = runtimes[0];
  const availableCommands = Array.from(COMMAND_NAME_SET).join('|');

  return [
    `Usage: /eigenflux [--server <name>] <${availableCommands}>`,
    defaultRuntime ? `Default server: ${defaultRuntime.server.name}` : undefined,
    runtimes.length > 0
      ? `Available servers: ${runtimes.map((runtime) => runtime.server.name).join(', ')}`
      : undefined,
    '',
    '/eigenflux auth — Show credential status',
    '/eigenflux profile — Fetch agent profile',
    '/eigenflux refresh — Check for a due profile refresh task now',
    '/eigenflux servers — List discovered servers',
    '/eigenflux feed — Run one feed refresh',
    '/eigenflux pm — Show PM stream status',
    '/eigenflux here — Remember current conversation as delivery route',
    '/eigenflux version — Show eigenflux CLI version info',
  ]
    .filter(Boolean)
    .join('\n');
}

function readNonEmptyString(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeChannel(value: unknown): string | undefined {
  return readNonEmptyString(value)?.toLowerCase();
}

async function resolveCurrentCommandRoute(
  ctx: CommandRouteContext,
  runtime: ServerRuntime,
  logger: Logger
) {
  let channel = normalizeChannel(ctx.channel);
  let to =
    normalizeReplyTarget(ctx.to, { channel }) ??
    normalizeReplyTarget(ctx.from, { channel, fallbackKind: 'user' });
  let accountId = readNonEmptyString(ctx.accountId);

  if (typeof ctx.getCurrentConversationBinding === 'function') {
    try {
      const binding = await ctx.getCurrentConversationBinding();
      if (binding) {
        channel = normalizeChannel(binding.channel) ?? channel;
        to =
          normalizeReplyTarget(binding.conversationId, { channel }) ??
          normalizeReplyTarget(binding.parentConversationId, { channel }) ??
          to;
        accountId = readNonEmptyString(binding.accountId) ?? accountId;
      }
    } catch (error) {
      logger.debug(
        `Failed to read current conversation binding: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  if (!channel || !to) {
    return undefined;
  }

  return findSessionRouteForBinding(
    {
      agentId: runtime.routing.agentId,
      channel,
      to,
      accountId,
    },
    logger
  );
}

async function buildHereText(
  ctx: CommandRouteContext,
  runtime: ServerRuntime,
  store: PluginRuntimeStore,
  logger: Logger
): Promise<string> {
  const route = await resolveCurrentCommandRoute(ctx, runtime, logger);
  if (!route || !route.replyChannel || !route.replyTo) {
    return [
      `Unable to resolve the current external session for server=${runtime.server.name}.`,
      'Run `/eigenflux here` inside the target conversation after OpenClaw has already created a session for it.',
    ].join('\n');
  }

  const saved = await writeStoredNotificationRoute(store, runtime.server.name, route, logger);
  if (!saved) {
    return `Failed to persist the current EigenFlux route for server=${runtime.server.name}; check plugin logs for details.`;
  }

  return [
    `EigenFlux server ${runtime.server.name} will deliver to this conversation by default:`,
    `sessionKey: ${route.sessionKey}`,
    `agentId: ${route.agentId}`,
    `channel: ${route.replyChannel ?? 'unknown'}`,
    `target: ${route.replyTo ?? 'unknown'}`,
    route.replyAccountId ? `account: ${route.replyAccountId}` : undefined,
  ]
    .filter(Boolean)
    .join('\n');
}

async function rememberCurrentCommandRouteIfPossible(
  ctx: CommandRouteContext,
  runtime: ServerRuntime,
  store: PluginRuntimeStore,
  logger: Logger
): Promise<void> {
  const route = await resolveCurrentCommandRoute(ctx, runtime, logger);
  if (!route || !route.replyChannel || !route.replyTo) {
    return;
  }

  if (await writeStoredNotificationRoute(store, runtime.server.name, route, logger)) {
    logger.debug(
      `Remembered current command route for server=${runtime.server.name}: session_key=${route.sessionKey}, channel=${route.replyChannel ?? 'unknown'}, to=${route.replyTo ?? 'unknown'}`
    );
  }
}

// ─── Command Handlers ───────────────────────────────────────────────────────

function buildAuthStatusText(runtime: ServerRuntime): string {
  const authState = runtime.credentialsLoader.loadAuthState();
  const lines = [`EigenFlux auth status (server=${runtime.server.name}):`];
  lines.push(`- credentials_path: ${authState.credentialsPath}`);
  lines.push(`- status: ${authState.status}`);
  if (authState.expiresAt) {
    lines.push(`- expires_at: ${authState.expiresAt}`);
  }
  if (authState.status === 'available') {
    lines.push(`- token: ${maskToken(authState.accessToken)}`);
  } else {
    lines.push('- token: unavailable');
  }
  return lines.join('\n');
}

async function buildProfileText(
  runtime: ServerRuntime,
  eigenfluxBin: string
): Promise<string> {
  const result = await execEigenflux<JsonApiSuccess<ProfileResponseData>>(
    eigenfluxBin,
    ['profile', 'show', '-s', runtime.server.name, '-f', 'json']
  );

  if (result.kind === 'auth_required') {
    return buildAuthRequiredPromptTemplate({ context: runtime.getPromptContext() });
  }
  if (result.kind === 'not_installed') {
    return `EigenFlux CLI not installed (bin=${result.bin}). ${INSTALL_GUIDANCE}`;
  }
  if (result.kind === 'error') {
    return `Failed to fetch profile for server ${runtime.server.name}: ${result.error.message}`;
  }

  return [
    `EigenFlux profile (server=${runtime.server.name}):`,
    '```json',
    safeJsonStringify(result.data),
    '```',
  ].join('\n');
}

async function buildFeedText(runtime: ServerRuntime): Promise<string> {
  const result = await runtime.feedPoller.pollOnce({
    notifyFeed: false,
    notifyAuthRequired: false,
  });
  switch (result.kind) {
    case 'success':
      return [
        `EigenFlux feed result (server=${runtime.server.name}):`,
        '```json',
        safeJsonStringify(result.payload),
        '```',
      ].join('\n');
    case 'auth_required':
      return buildAuthRequiredPromptTemplate({ context: runtime.getPromptContext() });
    case 'error':
      return `EigenFlux feed failed for server ${runtime.server.name}: ${result.error.message}`;
    default:
      return `EigenFlux feed finished with an unknown result for server ${runtime.server.name}.`;
  }
}

async function buildVersionText(eigenfluxBin: string): Promise<string> {
  const result = await execEigenflux<unknown>(eigenfluxBin, ['version']);

  if (result.kind === 'not_installed') {
    return `EigenFlux CLI not installed (bin=${result.bin}). ${INSTALL_GUIDANCE}`;
  }
  if (result.kind === 'auth_required') {
    return `EigenFlux CLI reported auth_required while fetching version (stderr: ${result.stderr || 'n/a'}).`;
  }
  if (result.kind === 'error') {
    return `Failed to fetch eigenflux version: ${result.error.message}`;
  }

  const body =
    typeof result.data === 'string' ? result.data : safeJsonStringify(result.data);
  return ['EigenFlux CLI version:', '```json', body, '```'].join('\n');
}

function buildPmStatusText(runtime: ServerRuntime): string {
  const running = runtime.streamClient.isRunning();
  const cursor = runtime.streamClient.getLastCursor();

  const lines = [`EigenFlux PM stream status (server=${runtime.server.name}):`];
  lines.push(`- streaming: ${running ? 'active' : 'inactive'}`);
  if (cursor) {
    lines.push(`- last_cursor: ${cursor}`);
  }

  if (!running) {
    lines.push('PM stream is not running. Check auth status or restart the service.');
  }

  return lines.join('\n');
}

// ─── Plugin Runtime Store ───────────────────────────────────────────────────

function createInMemoryPluginStore(): PluginRuntimeStore {
  const data = new Map<string, unknown>();
  return {
    async get(key: string): Promise<unknown> {
      return data.get(key);
    },
    async set(key: string, value: unknown): Promise<void> {
      data.set(key, value);
    },
  };
}

// ─── Utilities ──────────────────────────────────────────────────────────────

function maskToken(token: string): string {
  const trimmed = token.trim();
  if (trimmed.length <= 10) {
    return `${trimmed.slice(0, 2)}***`;
  }
  return `${trimmed.slice(0, 6)}...${trimmed.slice(-4)}`;
}

function safeJsonStringify(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

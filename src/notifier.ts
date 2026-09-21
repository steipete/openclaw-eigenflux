import type { OrderDeliveryReceipt } from './order-notifications';
import { createHash, randomUUID } from 'node:crypto';
import type { OpenClawPluginApi } from 'openclaw/plugin-sdk/plugin-entry';
import { type NotificationRouteOverrides } from './config';
import { Logger } from './logger';
import {
  isInternalSessionKey,
  resolveNotificationRoute,
  type NotificationRouteConfig,
  type NotificationRouteSource,
  type ResolvedNotificationRoute,
  type ResolvedNotificationRouteResult,
} from './notification-route-resolver';
import { writeStoredNotificationRoute, type PluginRuntimeStore } from './session-route-memory';
import { globalDeliveryCoordinator } from './delivery-coordinator';

/**
 * Typed subset of the OpenClaw runtime API used by the notifier.
 */
/** Minimal shape of a session-store entry we read/write when seeding a route. */
type SessionStoreEntryLike = {
  deliveryContext?: { channel?: string; to?: string; accountId?: string };
  lastChannel?: string;
  lastTo?: string;
  lastAccountId?: string;
  [key: string]: unknown;
};

type SessionStoreReadParams = {
  agentId?: string;
  sessionKey: string;
  storePath?: string;
};

type TaskRunLike = {
  id: string;
  runId?: string;
  status?: string;
  createdAt?: number;
  startedAt?: number;
  endedAt?: number;
};

type EigenFluxRuntimeApi = {
  subagent?: {
    run?: (params: {
      sessionKey: string;
      message: string;
      deliver?: boolean;
      idempotencyKey?: string;
      /** Queue lane for this run. Background deliveries use a dedicated lane so a
       *  slow/stuck run never blocks the user's interactive (per-chat) lane. */
      lane?: string;
    }) => Promise<{ runId: string }>;
    waitForRun?: (params: {
      runId: string;
      timeoutMs?: number;
    }) => Promise<{ status: 'ok' | 'error' | 'timeout'; error?: string }>;
    deleteSession?: (params: {
      sessionKey: string;
      deleteTranscript?: boolean;
    }) => Promise<void>;
  };
  /** Session-store helpers. Used to seed the one-shot feed session's delivery
   *  context before a deliver:true run, since `subagent.run` cannot carry a
   *  reply target and a fresh one-shot session has none of its own. */
  agent?: {
    session?: {
      resolveStorePath?: (store: string | undefined, opts?: { agentId?: string }) => string;
      /** Row-scoped read: one session entry by agent/session identity. Used by
       *  the busy check to see how recently the main conversation was active. */
      getSessionEntryAsync?: (
        params: SessionStoreReadParams
      ) => Promise<SessionStoreEntryLike | undefined>;
      getSessionEntry?: (params: SessionStoreReadParams) => SessionStoreEntryLike | undefined;
      /** Row-scoped write: patch a single session entry by identity. Replaces the
       *  deprecated whole-store `updateSessionStore` (SQLite-migration guard
       *  `sdk-session-store-write`). Merges the returned partial into the entry. */
      patchSessionEntry?: (params: {
        agentId?: string;
        sessionKey: string;
        storePath?: string;
        update: (
          entry: SessionStoreEntryLike,
          context: { existingEntry?: SessionStoreEntryLike }
        ) => Partial<SessionStoreEntryLike> | null;
      }) => Promise<unknown>;
    };
  };
  /** Task-run management. Used to truly cancel a run that outlived our wait budget
   *  ("stop waiting" != "stop the run"). Optional: older hosts may not expose it. */
  tasks?: {
    async?: {
      runs: {
        bindSession: (params: { sessionKey: string }) => {
          list: () => Promise<TaskRunLike[]>;
        };
      };
    };
    runs?: {
      bindSession?: (params: { sessionKey: string }) => {
        list: () => TaskRunLike[];
        cancel: (params: { taskId: string; cfg: unknown }) => Promise<{
          found: boolean;
          cancelled: boolean;
        }>;
      };
    };
  };
  system?: {
    enqueueSystemEvent?: (
      text: string,
      options: {
        sessionKey: string;
        deliveryContext?: {
          channel?: string;
          to?: string;
          accountId?: string;
        };
      }
    ) => boolean;
    requestHeartbeatNow?: (options: {
      reason?: string;
      coalesceMs?: number;
      agentId?: string;
      sessionKey?: string;
    }) => void;
    runCommandWithTimeout?: CommandRunner;
  };
};

const COMMAND_TIMEOUT_MS = 15000;
// deliver: true runs the full agent loop (LLM + reply + channel send), which can
// take well over a minute on long feed payloads. 3 minutes gives agents plenty
// of room to complete while still bounding a genuinely stuck run.
const SUBAGENT_WAIT_TIMEOUT_MS = 180_000;
const SUBAGENT_QUEUE_TIMEOUT_MS = 300_000;
// waitForRun is the authoritative lifecycle API. Preserve the previous maximum
// queue + execution budget without relying on the ephemeral task registry.
const SUBAGENT_TOTAL_WAIT_TIMEOUT_MS =
  SUBAGENT_QUEUE_TIMEOUT_MS + SUBAGENT_WAIT_TIMEOUT_MS;
// Background deliveries (feed / PM / profile) run on a dedicated queue lane so a
// slow or stuck agent run never serializes behind / ahead of the user's own
// interactive (per-chat) lane. The lane is orthogonal to the session key.
const BACKGROUND_LANE = 'eigenflux-bg';
const HEARTBEAT_REASON = 'plugin:eigenflux';

export type EigenFluxNotifierConfig = {
  store?: PluginRuntimeStore;
  eigenfluxBin?: string;
  serverName?: string;
  sessionKey: string;
  agentId: string;
  replyChannel?: string;
  replyTo?: string;
  replyAccountId?: string;
  openclawCliBin: string;
  sessionStorePath?: string;
  routeOverrides?: NotificationRouteOverrides;
};

export type DeliverOptions = {
  /**
   * When set, deliver to a one-shot session derived from this prefix
   * (a random suffix is appended to prevent context accumulation).
   * The session is automatically deleted after delivery completes.
   * Skips route persistence and stale-route re-resolve fallback.
   * Typical use: `eigenflux:feed:<serverName>` for isolated feed processing.
   */
  targetSessionKey?: string;
  /**
   * Deliver to this stable, isolated session without deleting it afterwards.
   * PM delivery uses one key per EigenFlux conv_id so conversation context is
   * retained without sharing the user's agent:main:main session.
   */
  persistentSessionKey?: string;
  /** Stable queue lane for this delivery. PMs use one lane per conversation. */
  lane?: string;
  /**
   * When true, deliver *silently*: run the host agent loop (so it can read its
   * own memory/session and execute CLI tools like `eigenflux profile update`)
   * but suppress the user-facing channel reply. Used by the daily profile
   * refresh so bio updates stay imperceptible to the user.
   *
   * Silent delivery only uses the subagent / command-agent transports with
   * delivery turned off; the heartbeat fallbacks are skipped because they
   * surface in the user's main session.
   */
  silent?: boolean;
};

type CommandRunner = (
  argv: string[],
  options: { timeoutMs: number }
) => Promise<{ code: number | null; stdout?: string; stderr?: string }>;

type NotifyAttemptResult =
  | {
      ok: true;
      mode: string;
      sessionKey?: string;
      runId?: string;
      detail?: string;
    }
  | {
      ok: false;
      mode: string;
      error: string;
      /** Stop the transport chain. Retrying could duplicate a timed-out run. */
      terminal?: boolean;
    };

export class EigenFluxNotifier {
  private readonly api: OpenClawPluginApi;
  private readonly logger: Logger;
  private readonly config: EigenFluxNotifierConfig;
  private readonly pendingCleanups: Promise<void>[] = [];

  constructor(api: OpenClawPluginApi, logger: Logger, config: EigenFluxNotifierConfig) {
    this.api = api;
    this.logger = logger;
    this.config = config;
  }

  private get runtime(): EigenFluxRuntimeApi {
    return (this.api.runtime ?? {}) as EigenFluxRuntimeApi;
  }

  /** Order delivery has one durable host run. Never fall back after submission. */
  async deliverOrder(message: string, context: {
    key: string;
    receipt: OrderDeliveryReceipt;
    checkpoint: (receipt: OrderDeliveryReceipt) => void;
  }): Promise<boolean> {
    const { receipt, checkpoint } = context;
    if (receipt.status === 'delivered') return true;
    if (receipt.status === 'failed' || (receipt.status === 'submitting' && !receipt.runId)) return false;
    const subagent = this.runtime.subagent;
    if (!subagent?.run || !subagent.waitForRun) return false;
    let runId = receipt.runId;
    if (!runId) {
      const digest = createHash('sha256').update(context.key).digest('hex');
      const base = await this.resolveRoute();
      const route = { ...base.route, sessionKey: `agent:${base.route.agentId}:eigenflux:order:${digest}` };
      await this.seedOneShotDeliveryContext(route.sessionKey, route);
      checkpoint({ status: 'submitting' });
      // A thrown call can still have been accepted remotely. The submitting
      // receipt deliberately prevents automatic replay in that ambiguous case.
      const submitted = await subagent.run({ sessionKey: route.sessionKey, message,
        deliver: true, idempotencyKey: `eigenflux-order-${digest}`, lane: 'eigenflux-order' });
      if (!submitted.runId) throw new Error('Order run acceptance missing runId');
      runId = submitted.runId;
      checkpoint({ status: 'running', runId });
    }
    // A timeout / unavailable wait endpoint leaves the same run in progress.
    // No cancellation, CLI fallback, new session, or new run is permitted.
    const result = await subagent.waitForRun({ runId, timeoutMs: 1000 });
    if (result.status === 'error') {
      checkpoint({ status: 'failed', runId });
      this.logger.warn(`Order run requires reconciliation: run_id=${runId}`);
      return false;
    }
    return result.status === 'ok';
  }

  async deliver(message: string, options?: DeliverOptions): Promise<boolean> {
    const serializationKey =
      options?.persistentSessionKey ?? options?.targetSessionKey ?? this.config.sessionKey;
    return globalDeliveryCoordinator.run(serializationKey, () =>
      this.deliverCoordinated(message, options)
    );
  }

  private async deliverCoordinated(message: string, options?: DeliverOptions): Promise<boolean> {
    const targetKey = options?.targetSessionKey;
    const persistentKey = options?.persistentSessionKey;
    const silent = options?.silent === true;

    // Isolated deliveries never touch the user's main conversation. A target
    // prefix creates a one-shot session; a persistent key (PM conv_id) retains
    // its own conversation context across messages.
    if (targetKey || persistentKey) {
      // Sweep any pending cleanups from previous deliveries before starting a new one.
      // This prevents orphan sessions from accumulating if deleteSession was slow.
      await this.drainPendingCleanups();

      // Resolve the delivery target (channel/to/accountId) from the standard route,
      // then override the sessionKey with a one-shot key to avoid context accumulation.
      const baseRoute = await this.resolveRoute();

      const sessionKey = persistentKey ?? `${targetKey}:${Date.now()}-${randomUUID().slice(0, 8)}`;
      const route: ResolvedNotificationRoute = {
        sessionKey,
        agentId: baseRoute.route.agentId,
        ...(baseRoute.route.replyChannel && { replyChannel: baseRoute.route.replyChannel }),
        ...(baseRoute.route.replyTo && { replyTo: baseRoute.route.replyTo }),
        ...(baseRoute.route.replyAccountId && { replyAccountId: baseRoute.route.replyAccountId }),
      };
      this.logger.info(
        `Delivery route resolved: source=${persistentKey ? 'targeted-persistent' : 'targeted-oneshot'}, ${formatRouteForLog(route)}, message_preview=${previewMessage(message)}`
      );

      // Seed the isolated session's delivery context BEFORE the run. A deliver:true
      // subagent resolves its reply target from the session store (it cannot be passed
      // through subagent.run), and a fresh one-shot session has none — without this the
      // run fails with Feishu "requires target" (missing target). Skip when silent
      // (no channel reply) since there is nothing to route. Best-effort.
      if (!silent) {
        await this.seedOneShotDeliveryContext(sessionKey, route);
      }

      // Isolated delivery uses only runtime.subagent. Current command/heartbeat
      // fallbacks cannot preserve an arbitrary session key and would leak back
      // into the main DM session.
      const result = await this.attemptDelivery(message, route, {
        subagentOnly: true,
        silent,
        lane: options?.lane,
      });
      if (result.result.ok) {
        this.logDispatch(result.result);
      } else {
        this.logger.error(`Failed to deliver notification to targeted session: ${result.errors.join(' | ')}`);
      }

      // Fire-and-forget: queue cleanup so it doesn't block the next delivery.
      // Pending cleanups are drained at the start of the next delivery and on stop().
      if (!persistentKey) {
        this.enqueueCleanup(sessionKey);
      }

      return result.result.ok;
    }

    // Standard delivery: resolve route, attempt delivery, remember on success.
    const initial = await this.resolveRoute();
    this.logger.info(
      `Delivery route resolved: source=${initial.source}, ${formatRouteForLog(initial.route)}, message_preview=${previewMessage(message)}`
    );

    const firstAttempt = await this.attemptDelivery(message, initial.route, {
      silent,
      lane: options?.lane,
    });
    if (firstAttempt.result.ok) {
      await this.rememberRouteIfChanged(firstAttempt.finalRoute, initial.source);
      this.logDispatch(firstAttempt.result);
      return true;
    }

    // If every transport failed with a remembered route, it may be stale.
    // Re-resolve fresh (skipping remembered), then retry the transport chain once.
    if (initial.source === 'remembered') {
      this.logger.warn(
        `All transports failed with remembered route; re-resolving without remembered config.`
      );
      const fallback = await this.resolveRoute({ ignoreRemembered: true });
      if (
        fallback.route.sessionKey !== initial.route.sessionKey ||
        fallback.route.replyTo !== initial.route.replyTo ||
        fallback.route.replyChannel !== initial.route.replyChannel
      ) {
        this.logger.info(
          `Retrying delivery with fresh route: source=${fallback.source}, ${formatRouteForLog(fallback.route)}`
        );
        const retry = await this.attemptDelivery(message, fallback.route, {
          silent,
          lane: options?.lane,
        });
        if (retry.result.ok) {
          await this.rememberRouteIfChanged(retry.finalRoute, fallback.source);
          this.logDispatch(retry.result);
          return true;
        }
        this.logger.error(
          `Failed to deliver notification after fresh re-resolve: ${retry.errors.join(' | ')}`
        );
        return false;
      }
      this.logger.warn('Fresh re-resolve produced the same route; skipping retry.');
    }

    this.logger.error(`Failed to deliver notification: ${firstAttempt.errors.join(' | ')}`);
    return false;
  }

  /**
   * Deliver feed into the user's MAIN session via a system event + heartbeat
   * wake, instead of a throwaway one-shot subagent session (plan §五·附, mode
   * 2a). This is what lets feed-derived broadcasts read the user's own working
   * context, keeps the main session's prompt cache warm (vs a cold one-shot
   * each cycle), and makes feed visible for in-session discussion.
   *
   * It deliberately uses ONLY the heartbeat transport — OpenClaw defers a
   * heartbeat while the main lane is busy (`requests-in-flight`), so this never
   * jumps ahead of the user's own messages. It does NOT fall back to a
   * deliver:true subagent (that was the path that contended on the user lane).
   * Fire-and-forget: enqueueSystemEvent is non-blocking and coalescing, so the
   * poll loop needs no backpressure guard.
   */
  async deliverToMainSession(message: string): Promise<boolean> {
    // Build the route from config. When the notifier config carries an explicit
    // reply target we use it as-is and skip resolveRoute()'s session-store scan
    // (it walks ~/.openclaw/agents/* and parses every sessions.json).
    const route: ResolvedNotificationRoute = {
      sessionKey: this.config.sessionKey,
      agentId: this.config.agentId,
      ...(this.config.replyChannel ? { replyChannel: this.config.replyChannel } : {}),
      ...(this.config.replyTo ? { replyTo: this.config.replyTo } : {}),
      ...(this.config.replyAccountId ? { replyAccountId: this.config.replyAccountId } : {}),
    };
    // The feed poller's config usually has NO reply target (channel/to) and an
    // INTERNAL sessionKey alias ("main"). Both must be recovered from the
    // session store's remembered route (the same source PM delivery resolves
    // through), for two distinct reasons:
    //
    // 1. Reply target: without channel/to the enqueued system event carries
    //    deliveryContext=none, so the main session's reply has nowhere to go.
    //
    // 2. CANONICAL sessionKey: the runtime bridge's enqueueSystemEvent does NOT
    //    canonicalize the key — an event enqueued under the literal alias
    //    "main" sits in a queue that NO consumer ever drains: both the
    //    heartbeat runner (resolveHeartbeatSession) and ordinary user-message
    //    runs (get-reply) canonicalize first and drain e.g. "agent:main:main".
    //    Enqueuing under the alias means the feed rots in memory until the
    //    gateway restarts. With the canonical key, the NEXT user message run
    //    drains the event even if no heartbeat ever fires — the heartbeat wake
    //    below is just an accelerator, not a dependency.
    if (!route.replyChannel || !route.replyTo || isInternalSessionKey(route.sessionKey)) {
      try {
        const { route: remembered, source } = await this.resolveRoute();
        if (!route.replyChannel || !route.replyTo) {
          if (remembered.replyChannel && remembered.replyTo) {
            route.replyChannel = remembered.replyChannel;
            route.replyTo = remembered.replyTo;
            if (remembered.replyAccountId) {
              route.replyAccountId = remembered.replyAccountId;
            }
            this.logger.info(
              `deliverToMainSession recovered reply target from ${source} route: ` +
              `channel=${remembered.replyChannel}, to=${remembered.replyTo}`
            );
          } else {
            this.logger.warn(
              'deliverToMainSession: no reply target in config or remembered route; ' +
              'feed reply may not reach the user this cycle'
            );
          }
        }
        if (isInternalSessionKey(route.sessionKey) && !isInternalSessionKey(remembered.sessionKey)) {
          this.logger.info(
            `deliverToMainSession adopting canonical session key from ${source} route: ` +
            `${route.sessionKey} -> ${remembered.sessionKey}`
          );
          route.sessionKey = remembered.sessionKey;
          if (remembered.agentId) {
            route.agentId = remembered.agentId;
          }
        }
      } catch (error) {
        this.logger.warn(
          `deliverToMainSession: resolveRoute failed while recovering reply target: ${formatError(error)}`
        );
      }
    }
    const result = await this.tryNotifyViaRuntimeHeartbeat(message, route);
    if (!result.ok) {
      // A compliant OpenClaw runtime always exposes the heartbeat APIs, so this
      // is rare; surface it at warn with the rollback hint rather than silently
      // dropping the feed for that cycle.
      this.logger.warn(
        `Feed main-session delivery failed: ${result.error ?? 'unknown'}. ` +
        `If runtime.system heartbeat APIs are unavailable on this host, set ` +
        `EIGENFLUX_FEED_DELIVERY=oneshot to fall back to the isolated path.`
      );
    }
    return result.ok;
  }

  /**
   * Seed the one-shot feed session's delivery context into the session store
   * BEFORE running the deliver:true subagent.
   *
   * Why this is necessary: `runtime.subagent.run` has no parameter to carry a
   * reply target, and a deliver:true run resolves its target from the session
   * store entry's `deliveryContext` (OpenClaw's `extractDeliveryInfo`). A freshly
   * minted one-shot session key has no entry at all, so the agent's reply fails
   * with Feishu "Delivering to … requires target" (a *missing*-target error, not
   * a format error). Writing the resolved route here gives that run a real
   * destination, while preserving the isolated-session design.
   *
   * Store path + agent id mirror the read side: a non-`agent:` key resolves to
   * the default agent ("main"), which matches `route.agentId`. Best-effort —
   * failures are logged, never thrown (delivery still attempts and can fall back).
   */
  private async seedOneShotDeliveryContext(
    sessionKey: string,
    route: ResolvedNotificationRoute
  ): Promise<void> {
    if (!route.replyChannel || !route.replyTo) {
      this.logger.warn(
        `Cannot seed deliveryContext for one-shot session ${sessionKey}: route has no channel/to`
      );
      return;
    }
    const session = this.runtime.agent?.session;
    if (typeof session?.resolveStorePath !== 'function' || typeof session?.patchSessionEntry !== 'function') {
      this.logger.warn(
        'runtime.agent.session store API unavailable; cannot seed deliveryContext for one-shot session'
      );
      return;
    }
    const deliveryContext = {
      channel: route.replyChannel,
      to: route.replyTo,
      ...(route.replyAccountId ? { accountId: route.replyAccountId } : {}),
    };
    try {
      const configuredStore = (this.api.config as { session?: { store?: string } } | undefined)?.session?.store;
      const storePath = session.resolveStorePath(configuredStore, { agentId: route.agentId });
      // Row-scoped write (patchSessionEntry) instead of the deprecated whole-store
      // updateSessionStore: patch merges the returned partial into just this entry.
      await session.patchSessionEntry({
        agentId: route.agentId,
        sessionKey,
        storePath,
        update: () => ({
          deliveryContext,
          lastChannel: route.replyChannel,
          lastTo: route.replyTo,
          ...(route.replyAccountId ? { lastAccountId: route.replyAccountId } : {}),
        }),
      });
      this.logger.info(
        `Seeded deliveryContext for one-shot session ${sessionKey}: channel=${deliveryContext.channel}, to=${deliveryContext.to}, account=${route.replyAccountId ?? 'n/a'}`
      );
    } catch (error) {
      this.logger.warn(
        `Failed to seed deliveryContext for one-shot session ${sessionKey}: ${formatError(error)}`
      );
    }
  }

  private async attemptDelivery(
    message: string,
    route: ResolvedNotificationRoute,
    options: { subagentOnly?: boolean; silent?: boolean; lane?: string } = {}
  ): Promise<{
    result: NotifyAttemptResult;
    finalRoute: ResolvedNotificationRoute;
    errors: string[];
  }> {
    const silent = options.silent === true;
    const attempts: Array<() => Promise<NotifyAttemptResult>> = [
      () => this.tryNotifyViaRuntimeSubagent(message, route, silent, options.lane),
    ];

    // The command-agent fallback cannot carry an arbitrary session key on the
    // current host. Never use it for isolated PM/one-shot sessions because it
    // would silently fall back into the main session and break isolation.
    if (!options.subagentOnly) {
      attempts.push(() => this.tryNotifyViaRuntimeCommandAgent(message, route, silent));
    }

    // Heartbeat fallbacks surface in the user's main session, so they can never
    // be silent. Skip them entirely for silent delivery.
    if (!options.subagentOnly && !silent) {
      attempts.push(
        () => this.tryNotifyViaRuntimeHeartbeat(message, route),
        () => this.tryNotifyViaRuntimeCommandHeartbeat(message),
      );
    }

    const errors: string[] = [];
    for (const attempt of attempts) {
      const result = await attempt();
      if (result.ok) {
        const finalRoute: ResolvedNotificationRoute = {
          sessionKey: result.sessionKey ?? route.sessionKey,
          agentId: route.agentId,
          replyChannel: route.replyChannel,
          replyTo: route.replyTo,
          replyAccountId: route.replyAccountId,
        };
        return { result, finalRoute, errors };
      }
      this.logger.warn(
        `Notification attempt failed: mode=${result.mode}, ${formatRouteForLog(route)}, error=${result.error}`
      );
      errors.push(`${result.mode}: ${result.error}`);
      if (result.terminal) {
        return { result, finalRoute: route, errors };
      }
    }
    return {
      result: { ok: false, mode: 'all', error: errors.join(' | ') },
      finalRoute: route,
      errors,
    };
  }

  private async tryNotifyViaRuntimeSubagent(
    message: string,
    route: ResolvedNotificationRoute,
    silent = false,
    lane = BACKGROUND_LANE
  ): Promise<NotifyAttemptResult> {
    const runtimeSubagent = this.runtime.subagent;

    if (!runtimeSubagent || typeof runtimeSubagent.run !== 'function') {
      return {
        ok: false,
        mode: 'runtime.subagent',
        error: 'runtime.subagent.run is unavailable',
      };
    }

    try {
      // deliver:false still runs the full agent loop (LLM + tool calls), so the
      // agent can update its bio via the CLI — it just suppresses the channel reply.
      const deliver = !silent;
      this.logger.info(
        `Attempting runtime.subagent delivery: ${formatRouteForLog(route)}, deliver=${deliver}, lane=${lane}`
      );
      const { runId } = await runtimeSubagent.run({
        sessionKey: route.sessionKey,
        message,
        deliver,
        idempotencyKey: randomUUID(),
        lane,
      });

      // run() only enqueues; wait long enough for the full agent loop (LLM +
      // reply + channel send) to complete so we can surface real errors.
      if (typeof runtimeSubagent.waitForRun === 'function') {
        const waited = await this.waitForRunFromExecutionStart(route.sessionKey, runId);
        if (waited.status === 'error') {
          return {
            ok: false,
            mode: 'runtime.subagent',
            error: `subagent run error${waited.error ? `: ${waited.error}` : ''}`,
          };
        }
        if (waited.status === 'timeout') {
          // "Stop waiting" is not "stop the run": truly cancel it and report
          // failure. terminal=true prevents a fallback from double-delivering.
          const cancelled = await this.tryCancelRun(route.sessionKey, runId);
          return {
            ok: false,
            mode: 'runtime.subagent',
            terminal: true,
            error:
              (waited.error ??
                `subagent completion wait timed out after ${Math.round(SUBAGENT_TOTAL_WAIT_TIMEOUT_MS / 1000)}s`) +
              (cancelled ? '; run cancelled' : '; cancellation unavailable or failed'),
          };
        }
      }

      return {
        ok: true,
        mode: 'runtime.subagent',
        sessionKey: route.sessionKey,
        runId,
      };
    } catch (error) {
      return {
        ok: false,
        mode: 'runtime.subagent',
        error: formatError(error),
      };
    }
  }

  /**
   * Wait on the authoritative agent lifecycle API. The task registry is an
   * ephemeral cancellation/telemetry view: a fast or already-terminal run may
   * never appear in bindSession().list(), so it must not gate completion waits.
   */
  private async waitForRunFromExecutionStart(
    _sessionKey: string,
    runId: string
  ): Promise<{ status: 'ok' | 'error' | 'timeout'; error?: string }> {
    const waitForRun = this.runtime.subagent?.waitForRun;
    if (typeof waitForRun !== 'function') {
      return { status: 'ok' };
    }

    return waitForRun({ runId, timeoutMs: SUBAGENT_TOTAL_WAIT_TIMEOUT_MS });
  }

  /** Best-effort true cancellation after either queue or execution timeout. */
  private async tryCancelRun(sessionKey: string, runId: string): Promise<boolean> {
    const runs = this.runtime.tasks?.runs;
    const asyncTasks = this.runtime.tasks?.async;
    if (!runs || typeof runs.bindSession !== 'function') {
      this.logger.debug(
        `tryCancelRun: runtime.tasks.runs unavailable; cannot cancel run_id=${runId}`
      );
      return false;
    }
    try {
      const tasks = asyncTasks
        ? await asyncTasks.runs.bindSession({ sessionKey }).list()
        : runs.bindSession({ sessionKey }).list();
      const task = tasks.find((t) => t.runId === runId);
      if (!task) {
        this.logger.warn(
          `tryCancelRun: no task found for run_id=${runId} on session=${sessionKey}; cannot cancel`
        );
        return false;
      }
      const result = await runs.bindSession({ sessionKey }).cancel({ taskId: task.id, cfg: this.api.config });
      this.logger.warn(
        `Cancelled stuck background run: run_id=${runId}, task_id=${task.id}, ` +
        `found=${result.found}, cancelled=${result.cancelled}`
      );
      return result.cancelled;
    } catch (error) {
      this.logger.warn(`tryCancelRun failed for run_id=${runId}: ${formatError(error)}`);
      return false;
    }
  }

  private async tryNotifyViaRuntimeCommandAgent(
    message: string,
    route: ResolvedNotificationRoute,
    silent = false
  ): Promise<NotifyAttemptResult> {
    return this.runRuntimeCommand(
      'runtime.command.agent',
      this.buildAgentCliArgs(message, route, silent),
      route
    );
  }

  private async tryNotifyViaRuntimeHeartbeat(
    message: string,
    route: ResolvedNotificationRoute
  ): Promise<NotifyAttemptResult> {
    const runtimeSystem = this.runtime.system;

    if (
      !runtimeSystem ||
      typeof runtimeSystem.enqueueSystemEvent !== 'function' ||
      typeof runtimeSystem.requestHeartbeatNow !== 'function'
    ) {
      return {
        ok: false,
        mode: 'runtime.system.heartbeat',
        error: 'runtime.system heartbeat APIs are unavailable',
      };
    }

    try {
      const deliveryContext = this.resolveHeartbeatDeliveryContext(route);
      this.logger.info(
        `Attempting runtime.system.heartbeat delivery: ${formatRouteForLog(route)}, delivery_context=${formatDeliveryContextForLog(deliveryContext)}`
      );
      const enqueued = runtimeSystem.enqueueSystemEvent(message, {
        sessionKey: route.sessionKey,
        ...(deliveryContext ? { deliveryContext } : {}),
      });
      runtimeSystem.requestHeartbeatNow({
        reason: HEARTBEAT_REASON,
        coalesceMs: 0,
        agentId: route.agentId,
        sessionKey: route.sessionKey,
      });

      return {
        ok: true,
        mode: 'runtime.system.heartbeat',
        sessionKey: route.sessionKey,
        detail: enqueued ? 'enqueued' : 'duplicate-enqueued',
      };
    } catch (error) {
      return {
        ok: false,
        mode: 'runtime.system.heartbeat',
        error: formatError(error),
      };
    }
  }

  private async tryNotifyViaRuntimeCommandHeartbeat(message: string): Promise<NotifyAttemptResult> {
    const { route } = await this.resolveRoute();
    return this.runRuntimeCommand(
      'runtime.command.heartbeat',
      this.buildHeartbeatCliArgs(message),
      route
    );
  }

  private async runRuntimeCommand(
    mode: string,
    argv: string[],
    route: ResolvedNotificationRoute
  ): Promise<NotifyAttemptResult> {
    const runtimeCommand = this.runtime.system?.runCommandWithTimeout;

    if (typeof runtimeCommand !== 'function') {
      return {
        ok: false,
        mode,
        error: 'runtime.system.runCommandWithTimeout is unavailable',
      };
    }

    try {
      this.logger.info(
        `Attempting ${mode} delivery: ${formatRouteForLog(route)}, argv=${formatCommandArgsForLog(argv)}`
      );
      const result = await runtimeCommand(argv, { timeoutMs: COMMAND_TIMEOUT_MS });
      if (result.code === 0) {
        return {
          ok: true,
          mode,
          sessionKey: route.sessionKey,
        };
      }

      return {
        ok: false,
        mode,
        error: `${formatCommandFailure(result)} (argv=${formatCommandArgsForLog(argv)})`,
      };
    } catch (error) {
      return {
        ok: false,
        mode,
        error: formatError(error),
      };
    }
  }

  private buildAgentCliArgs(
    message: string,
    route: ResolvedNotificationRoute,
    silent = false
  ): string[] {
    const args = [
      this.config.openclawCliBin,
      'agent',
      '--message',
      message,
      '--agent',
      route.agentId,
    ];

    // --deliver runs the channel reply; omit it for silent delivery so the
    // agent loop still runs (and can call the CLI) without messaging the user.
    if (!silent) {
      args.push('--deliver');
    }

    if (route.replyChannel) {
      args.push('--reply-channel', route.replyChannel);
    }
    if (route.replyTo) {
      args.push('--reply-to', route.replyTo);
    }
    if (route.replyAccountId) {
      args.push('--reply-account', route.replyAccountId);
    }

    return args;
  }

  private buildHeartbeatCliArgs(message: string): string[] {
    return [
      this.config.openclawCliBin,
      'system',
      'event',
      '--text',
      message,
      '--mode',
      'now',
    ];
  }

  private resolveHeartbeatDeliveryContext(
    route: ResolvedNotificationRoute
  ):
    | {
        channel?: string;
        to?: string;
        accountId?: string;
      }
    | undefined {
    if (!route.replyChannel && !route.replyTo && !route.replyAccountId) {
      return undefined;
    }

    return {
      ...(route.replyChannel ? { channel: route.replyChannel } : {}),
      ...(route.replyTo ? { to: route.replyTo } : {}),
      ...(route.replyAccountId ? { accountId: route.replyAccountId } : {}),
    };
  }

  private async resolveRoute(
    options: { ignoreRemembered?: boolean } = {}
  ): Promise<ResolvedNotificationRouteResult> {
    return resolveNotificationRoute(
      this.config as NotificationRouteConfig,
      this.logger,
      options
    );
  }

  /**
   * Busy check for the busy-aware feed push: should the main conversation be
   * left alone right now?
   *
   * Two independent signals, both cheap local reads (no LLM, no network):
   * 1. Active task runs bound to the target session — covers our own feed /
   *    subagent runs (same runtime.tasks.runs API tryCancelRun already uses).
   * 2. Recent session activity via the row-scoped session entry's updatedAt —
   *    covers the user's reply runs, which are NOT task runs. "Recently
   *    updated" is a proxy for "mid-conversation": each turn refreshes it, so
   *    it only goes quiet once the user has actually stopped talking.
   *
   * Unavailable APIs contribute `false` (degrade to pushing immediately, the
   * pre-scheduler behavior). Never throws.
   */
  async isMainRouteBusy(recentActivityMs: number): Promise<boolean> {
    let route: ResolvedNotificationRoute;
    try {
      ({ route } = await this.resolveRoute());
    } catch {
      return false;
    }

    const tasksRuntime = this.runtime.tasks;
    const runs = tasksRuntime?.async ? tasksRuntime.async.runs : tasksRuntime?.runs;
    if (runs && typeof runs.bindSession === 'function') {
      try {
        // list() returns the session's FULL task history, terminal records
        // included — only a task without endedAt is actually running.
        const tasks = await runs.bindSession({ sessionKey: route.sessionKey }).list();
        const active = tasks.some((task) => task.endedAt === undefined);
        if (active) {
          return true;
        }
      } catch {
        // ignore — fall through to the activity signal
      }
    }

    const session = this.runtime.agent?.session;
    if (session) {
      try {
        const params = {
          agentId: route.agentId,
          sessionKey: route.sessionKey,
        };
        const entry = typeof session.getSessionEntryAsync === 'function'
          ? await session.getSessionEntryAsync(params)
          : session.getSessionEntry?.(params);
        const updatedAt = typeof entry?.updatedAt === 'number' ? entry.updatedAt : undefined;
        if (updatedAt !== undefined && Date.now() - updatedAt < recentActivityMs) {
          return true;
        }
      } catch {
        // ignore — assume idle
      }
    }

    return false;
  }

  /**
   * Persist the successful route to the eigenflux CLI config unless it came from
   * the remembered config already (no-op when unchanged).
   */
  private async rememberRouteIfChanged(
    route: ResolvedNotificationRoute,
    source: NotificationRouteSource
  ): Promise<void> {
    if (!route.sessionKey || !route.agentId) {
      return;
    }
    if (isInternalSessionKey(route.sessionKey)) {
      return;
    }
    if (!route.replyChannel || !route.replyTo) {
      return;
    }
    if (source === 'remembered') {
      this.logger.debug(
        `Skipping remembered-route write; route came from config (session_key=${route.sessionKey})`
      );
      return;
    }
    await writeStoredNotificationRoute(
      this.config.store,
      this.config.serverName,
      route,
      this.logger
    );
  }

  /**
   * Best-effort cleanup of a one-shot session. Failures are logged but do not
   * propagate — the session may already have been cleaned up by the runtime.
   */
  private async tryDeleteSession(sessionKey: string): Promise<void> {
    const deleteSession = this.runtime.subagent?.deleteSession;
    if (typeof deleteSession !== 'function') {
      this.logger.debug(`deleteSession unavailable; skipping cleanup for session_key=${sessionKey}`);
      return;
    }
    try {
      await deleteSession({ sessionKey, deleteTranscript: true });
      this.logger.info(`One-shot session cleaned up: session_key=${sessionKey}`);
    } catch (error) {
      this.logger.warn(`Failed to clean up one-shot session session_key=${sessionKey}: ${formatError(error)}`);
    }
  }

  /** Queue a session cleanup as fire-and-forget (non-blocking). */
  private enqueueCleanup(sessionKey: string): void {
    const cleanup = this.tryDeleteSession(sessionKey);
    this.pendingCleanups.push(cleanup);
    cleanup.finally(() => {
      const idx = this.pendingCleanups.indexOf(cleanup);
      if (idx >= 0) this.pendingCleanups.splice(idx, 1);
    });
  }

  /** Await all pending session cleanups. Called before new delivery and on stop(). */
  async drainPendingCleanups(): Promise<void> {
    if (this.pendingCleanups.length === 0) return;
    this.logger.debug(`Draining ${this.pendingCleanups.length} pending session cleanup(s)`);
    await Promise.allSettled([...this.pendingCleanups]);
  }

  private logDispatch(result: Extract<NotifyAttemptResult, { ok: true }>): void {
    const details = [
      `mode=${result.mode}`,
      result.sessionKey ? `session_key=${result.sessionKey}` : null,
      result.runId ? `run_id=${result.runId}` : null,
      result.detail ? `detail=${result.detail}` : null,
    ]
      .filter(Boolean)
      .join(', ');
    this.logger.info(`Notification dispatched: ${details}`);
  }
}

function formatCommandFailure(result: {
  code: number | null;
  stdout?: string;
  stderr?: string;
}): string {
  return (
    result.stderr?.trim() ||
    result.stdout?.trim() ||
    `command exited with ${result.code ?? 'unknown'}`
  );
}

function formatError(error: unknown): string {
  if (error instanceof Error) {
    return `${error.name}: ${error.message}`;
  }
  return String(error);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatRouteForLog(route: ResolvedNotificationRoute): string {
  return [
    `session_key=${route.sessionKey}`,
    `agent_id=${route.agentId}`,
    `channel=${route.replyChannel ?? 'n/a'}`,
    `to=${route.replyTo ?? 'n/a'}`,
    `account=${route.replyAccountId ?? 'n/a'}`,
  ].join(', ');
}

function formatDeliveryContextForLog(
  deliveryContext:
    | {
        channel?: string;
        to?: string;
        accountId?: string;
      }
    | undefined
): string {
  if (!deliveryContext) {
    return 'none';
  }
  return [
    `channel=${deliveryContext.channel ?? 'n/a'}`,
    `to=${deliveryContext.to ?? 'n/a'}`,
    `account=${deliveryContext.accountId ?? 'n/a'}`,
  ].join(', ');
}

function previewMessage(message: string, maxLength = 120): string {
  const singleLine = message.replace(/\s+/gu, ' ').trim();
  if (singleLine.length <= maxLength) {
    return JSON.stringify(singleLine);
  }
  return JSON.stringify(`${singleLine.slice(0, maxLength - 3)}...`);
}

function formatCommandArgsForLog(argv: string[]): string {
  const sanitized = [...argv];
  for (let index = 0; index < sanitized.length; index += 1) {
    if (sanitized[index] === '--message' || sanitized[index] === '--text') {
      if (typeof sanitized[index + 1] === 'string') {
        sanitized[index + 1] = `<len:${sanitized[index + 1].length}>`;
      }
    }
  }
  return JSON.stringify(sanitized);
}

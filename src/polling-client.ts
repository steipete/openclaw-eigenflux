/**
 * Polling client for EigenFlux feed updates.
 * Uses the eigenflux CLI (`eigenflux feed poll`) instead of direct HTTP calls.
 */

import { execEigenflux } from './cli-executor';
import { Logger } from './logger';

export const POLL_INTERVAL_CONFIG_KEY = 'feed_poll_interval';
export const DEFAULT_POLL_INTERVAL_SEC = 600;
export const MIN_POLL_INTERVAL_SEC = 10;
export const MAX_POLL_INTERVAL_SEC = 24 * 60 * 60;

/**
 * Reads the feed poll interval (in seconds) from the eigenflux CLI config
 * (`eigenflux config get --key feed_poll_interval`). Values are stored as
 * decimal-string seconds per the config KV conventions. Falls back to 600
 * (10 minutes) when the key is unset, the value is invalid, out of the
 * supported range [10s, 86400s], or the CLI call fails.
 */
export async function readPollIntervalSec(
  eigenfluxBin: string,
  serverName: string,
  logger: Logger
): Promise<number> {
  const result = await execEigenflux<unknown>(
    eigenfluxBin,
    ['config', 'get', '--key', POLL_INTERVAL_CONFIG_KEY, '--server', serverName, '--format', 'json'],
    { logger }
  );

  if (result.kind !== 'success' || result.data === undefined || result.data === null) {
    return DEFAULT_POLL_INTERVAL_SEC;
  }

  let numeric: number | undefined;
  if (typeof result.data === 'number' && Number.isFinite(result.data)) {
    numeric = result.data;
  } else if (typeof result.data === 'string') {
    const parsed = Number(result.data.trim());
    if (Number.isFinite(parsed)) {
      numeric = parsed;
    }
  }

  if (numeric === undefined) {
    logger.warn(
      `Ignoring non-numeric pollInterval from eigenflux config (server=${serverName}, value=${JSON.stringify(result.data)}); using ${DEFAULT_POLL_INTERVAL_SEC}s`
    );
    return DEFAULT_POLL_INTERVAL_SEC;
  }

  const floored = Math.floor(numeric);
  if (floored < MIN_POLL_INTERVAL_SEC || floored > MAX_POLL_INTERVAL_SEC) {
    logger.warn(
      `pollInterval ${floored}s from eigenflux config (server=${serverName}) is outside [${MIN_POLL_INTERVAL_SEC}s, ${MAX_POLL_INTERVAL_SEC}s]; using ${DEFAULT_POLL_INTERVAL_SEC}s`
    );
    return DEFAULT_POLL_INTERVAL_SEC;
  }

  return floored;
}

export interface FeedItem {
  item_id: string;
  summary?: string;
  broadcast_type: string;
  domains?: string[];
  keywords?: string[];
  group_id?: string;
  source_type?: string;
  url?: string;
  updated_at: number;
}

export interface FeedNotification {
  notification_id: string;
  type: string;
  content: string;
  created_at: number;
}

export interface FeedResponseData {
  items: FeedItem[];
  has_more: boolean;
  notifications: FeedNotification[];
  /**
   * Output-contract digest delivered inline by the backend (the feed API gained
   * the `output_contract` field). When present it is the authoritative source;
   * the CLI-synced host copy is only a fallback for older servers.
   */
  output_contract?: string;
  /**
   * Impression that served this batch. The CLI caches it alongside the feed
   * items so `feed event record` can join followup events back to their
   * impression when reporting to the backend.
   */
  impression_id?: string;
}

export interface FeedResponse {
  code: number;
  msg: string;
  data: FeedResponseData;
}

export interface PollingClientConfig {
  serverName: string;
  eigenfluxBin: string;
  /** Actual model observed for this runtime; absent until ownership is verified. */
  resolveModel?: () => string | undefined;
  /**
   * Resolves the seconds to wait before the next poll. Invoked after every
   * poll completes so the interval can be changed at runtime via the
   * eigenflux CLI config (`pollInterval` key).
   */
  resolvePollIntervalSec: () => Promise<number>;
  logger: Logger;
  onFeedPolled: (payload: FeedResponse) => Promise<void>;
  onAuthRequired: (event: AuthRequiredEvent) => Promise<void>;
  /** Runs the current thin Heartbeat contract before the feed poll. */
  onHeartbeatStart?: () => Promise<void>;
  /**
   * Optional hook invoked once per successful poll (heartbeat), regardless of
   * whether the feed contained any items/notifications. Used for side-channel
   * housekeeping such as reporting agent settings to the backend. Must be
   * best-effort: it is awaited but its rejection is caught and logged so it can
   * never interrupt polling.
   */
  onPollSuccess?: (payload: FeedResponse) => Promise<void>;
}

export interface AuthRequiredEvent {
  reason: 'auth_required';
}

export type PollResult =
  | {
      kind: 'success';
      payload: FeedResponse;
    }
  | {
      kind: 'auth_required';
      authEvent: AuthRequiredEvent;
    }
  | {
      kind: 'error';
      error: Error;
    };

export interface PollOnceOptions {
  notifyFeed?: boolean;
  notifyAuthRequired?: boolean;
}

export class EigenFluxPollingClient {
  private config: PollingClientConfig;
  private timeoutId: NodeJS.Timeout | null = null;
  private isRunning = false;
  private activePoll: Promise<PollResult> | null = null;

  constructor(config: PollingClientConfig) {
    this.config = config;
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      this.config.logger.warn('Polling client already running');
      return;
    }

    this.isRunning = true;
    this.config.logger.info(
      `Starting polling client for server=${this.config.serverName}`
    );

    // Initial fetch, then self-schedule subsequent polls using the interval
    // freshly resolved from the eigenflux CLI config after each run.
    await this.pollOnce();
    this.scheduleNext();
  }

  stop(): void {
    if (!this.isRunning) {
      return;
    }

    this.config.logger.info(`Stopping polling client for server=${this.config.serverName}`);
    this.isRunning = false;

    if (this.timeoutId) {
      clearTimeout(this.timeoutId);
      this.timeoutId = null;
    }
  }

  private async scheduleNext(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    let intervalSec: number;
    try {
      intervalSec = await this.config.resolvePollIntervalSec();
    } catch (error) {
      this.config.logger.warn(
        `Failed to resolve pollInterval for server=${this.config.serverName}: ${error instanceof Error ? error.message : String(error)}; using ${DEFAULT_POLL_INTERVAL_SEC}s`
      );
      intervalSec = DEFAULT_POLL_INTERVAL_SEC;
    }

    if (!this.isRunning) {
      return;
    }

    this.config.logger.debug(
      `Scheduling next feed poll for server=${this.config.serverName} in ${intervalSec}s`
    );
    this.timeoutId = setTimeout(() => {
      this.timeoutId = null;
      this.pollOnce()
        .catch((err) => {
          this.config.logger.error(
            `Polling error: ${err instanceof Error ? err.message : String(err)}`
          );
        })
        .finally(() => {
          this.scheduleNext();
        });
    }, intervalSec * 1000);
  }

  async pollOnce(options: PollOnceOptions = {}): Promise<PollResult> {
    if (this.activePoll) {
      this.config.logger.warn('Skipping feed poll because a previous poll is still in progress');
      return this.activePoll;
    }

    const run = async (): Promise<PollResult> => {
      const notifyFeed = options.notifyFeed ?? true;
      const notifyAuthRequired = options.notifyAuthRequired ?? true;

      try {
        if (this.config.onHeartbeatStart) {
          try {
            await this.config.onHeartbeatStart();
          } catch (hookError) {
            this.config.logger.warn(
              `onHeartbeatStart hook failed for server=${this.config.serverName}: ${hookError instanceof Error ? hookError.message : String(hookError)}`
            );
          }
        }

        this.config.logger.info(`Polling feed via CLI for server=${this.config.serverName}`);

        const result = await execEigenflux<FeedResponseData>(
          this.config.eigenfluxBin,
          ['feed', 'poll', '--limit', '20', '--action', 'refresh', '-s', this.config.serverName, '-f', 'json'],
          { logger: this.config.logger, env: { EIGENFLUX_MODEL: this.config.resolveModel?.() } }
        );

        if (result.kind === 'auth_required') {
          const authEvent: AuthRequiredEvent = { reason: 'auth_required' };
          if (notifyAuthRequired) {
            await this.config.onAuthRequired(authEvent);
          }
          return { kind: 'auth_required', authEvent };
        }

        if (result.kind === 'not_installed') {
          return {
            kind: 'error',
            error: new Error(`eigenflux CLI not installed (bin=${result.bin})`),
          };
        }

        if (result.kind === 'error') {
          return { kind: 'error', error: result.error };
        }

        // Reconstruct full FeedResponse envelope from CLI data output
        const feedResponse: FeedResponse = {
          code: 0,
          msg: 'success',
          data: result.data,
        };

        const items = feedResponse.data.items ?? [];
        const notifications = feedResponse.data.notifications ?? [];
        this.config.logger.info(
          `Polled feed: ${items.length} items, notifications=${notifications.length}, has_more=${feedResponse.data.has_more}`
        );

        try {
          if (notifyFeed && (items.length > 0 || notifications.length > 0)) {
            await this.config.onFeedPolled(feedResponse);
          }
        } finally {
          // Delivery starts immediately; its failure cannot skip identity reporting.
          if (this.config.onPollSuccess) {
            try {
              await this.config.onPollSuccess(feedResponse);
            } catch (hookError) {
              this.config.logger.warn(
                `onPollSuccess hook failed for server=${this.config.serverName}: ${hookError instanceof Error ? hookError.message : String(hookError)}`
              );
            }
          }
        }

        return { kind: 'success', payload: feedResponse };
      } catch (error) {
        const normalized = error instanceof Error ? error : new Error(String(error));
        this.config.logger.error(
          `Failed to poll feed for server=${this.config.serverName}: ${normalized.message}`
        );
        return { kind: 'error', error: normalized };
      }
    };

    this.activePoll = run().finally(() => {
      this.activePoll = null;
    });
    return this.activePoll;
  }
}

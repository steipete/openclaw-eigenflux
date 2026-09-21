/**
 * Stream client for EigenFlux private message updates.
 * Manages a long-running `eigenflux stream` child process that outputs NDJSON.
 */

import { spawn, ChildProcess } from 'child_process';
import { createInterface, Interface as ReadlineInterface } from 'readline';
import { Logger } from './logger';
import { cliEnvironment } from './cli-executor';

const EXIT_AUTH_REQUIRED = 4;
const INITIAL_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 60_000;
const BACKOFF_MULTIPLIER = 2;
const STOP_GRACE_MS = 5_000;
const MAX_CONSECUTIVE_FAILURES = 20;

export interface PmStreamEvent {
  type: string;
  notification?: unknown;
  data: {
    messages?: Array<{
      msg_id: string;
      conv_id: string;
      sender_id?: string;
      sender_name?: string;
      content: string;
      created_at: number;
    }>;
    // Pending incoming friend requests, replayed on (re)connect by the server.
    friend_requests?: Array<{
      request_id: string;
      from_uid: string;
      from_name?: string;
      greeting?: string;
      created_at: number;
    }>;
    friend_requests_has_more?: boolean;
    // Missed friend-request responses (accepted/rejected), backfilled on
    // (re)connect from the server's durable pm:notify store.
    friend_responses?: Array<{
      notification_id: string;
      type: string;
      content: string;
      created_at: number;
    }>;
    // Present on `type: "friend_accepted"` push events.
    friend_uid?: string;
    next_cursor?: string;
    [key: string]: unknown;
  };
}

export interface StreamClientConfig {
  serverName: string;
  eigenfluxBin: string;
  logger: Logger;
  onPmEvent: (event: PmStreamEvent) => Promise<void>;
  onAuthRequired: () => Promise<void>;
  onStreamError?: (error: Error) => void;
}

export class EigenFluxStreamClient {
  private config: StreamClientConfig;
  private child: ChildProcess | null = null;
  private readline: ReadlineInterface | null = null;
  private stopping = false;
  private running = false;
  private lastCursor: string | null = null;
  private backoffMs = INITIAL_BACKOFF_MS;
  private consecutiveFailures = 0;
  private restartTimer: NodeJS.Timeout | null = null;

  constructor(config: StreamClientConfig) {
    this.config = config;
  }

  isRunning(): boolean {
    return this.running;
  }

  getLastCursor(): string | null {
    return this.lastCursor;
  }

  async start(): Promise<void> {
    if (this.running) {
      this.config.logger.warn('Stream client already running');
      return;
    }

    this.running = true;
    this.stopping = false;
    this.config.logger.info(`Starting stream client for server=${this.config.serverName}`);
    this.spawnStreamProcess();
  }

  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.config.logger.info(`Stopping stream client for server=${this.config.serverName}`);
    this.stopping = true;
    this.running = false;

    if (this.restartTimer) {
      clearTimeout(this.restartTimer);
      this.restartTimer = null;
    }

    if (this.readline) {
      this.readline.close();
      this.readline = null;
    }

    if (this.child) {
      const child = this.child;
      this.child = null;

      child.kill('SIGTERM');

      await new Promise<void>((resolve) => {
        const forceKillTimer = setTimeout(() => {
          try {
            child.kill('SIGKILL');
          } catch {
            // Process already exited
          }
          resolve();
        }, STOP_GRACE_MS);

        child.once('exit', () => {
          clearTimeout(forceKillTimer);
          resolve();
        });
      });
    }
  }

  private spawnStreamProcess(): void {
    if (this.stopping || !this.running) {
      return;
    }

    const args = ['stream', '-s', this.config.serverName, '-f', 'json'];
    if (this.lastCursor) {
      args.push('--cursor', this.lastCursor);
    }

    this.config.logger.info(
      `Spawning: ${this.config.eigenfluxBin} ${args.join(' ')}`
    );

    const child = spawn(this.config.eigenfluxBin, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: cliEnvironment(),
    });
    this.child = child;

    const rl = createInterface({ input: child.stdout! });
    this.readline = rl;

    rl.on('line', (line) => {
      this.handleLine(line);
    });

    child.stderr?.on('data', (chunk: Buffer) => {
      const text = chunk.toString().trim();
      if (text) {
        this.config.logger.debug(`[stream stderr] ${text}`);
      }
    });

    child.on('error', (err) => {
      this.config.logger.error(`Stream process error: ${err.message}`);
      this.config.onStreamError?.(err);
      this.scheduleRestart();
    });

    child.on('exit', (code, signal) => {
      this.config.logger.info(
        `Stream process exited (code=${code}, signal=${signal})`
      );

      if (this.stopping) {
        return;
      }

      if (code === EXIT_AUTH_REQUIRED) {
        this.config.logger.warn('Stream auth required');
        this.config.onAuthRequired().then(() => {
          this.scheduleRestart();
        }).catch((err) => {
          this.config.logger.error(`Auth required handler error: ${err instanceof Error ? err.message : String(err)}`);
          this.scheduleRestart();
        });
        return;
      }

      this.scheduleRestart();
    });
  }

  private handleLine(line: string): void {
    const trimmed = line.trim();
    if (!trimmed) {
      return;
    }

    try {
      const event = JSON.parse(trimmed) as PmStreamEvent;

      // Update cursor for reconnect resume
      if (event.type !== 'notification_push' && event.type !== 'commission_order_notification' && event.data?.next_cursor) {
        this.lastCursor = event.data.next_cursor;
      }

      // Reset backoff on successful message
      this.backoffMs = INITIAL_BACKOFF_MS;
      this.consecutiveFailures = 0;

      this.config.onPmEvent(event).catch((err) => {
        this.config.logger.error(
          `PM event handler error: ${err instanceof Error ? err.message : String(err)}`
        );
      });
    } catch (err) {
      this.config.logger.warn(
        `Failed to parse stream line: ${(err as Error).message}`
      );
    }
  }

  private scheduleRestart(): void {
    if (this.stopping || !this.running) {
      return;
    }

    this.consecutiveFailures += 1;

    if (this.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
      this.config.logger.error(
        `Stream client giving up after ${MAX_CONSECUTIVE_FAILURES} consecutive failures for server=${this.config.serverName}`
      );
      this.running = false;
      return;
    }

    this.config.logger.info(
      `Stream reconnecting in ${this.backoffMs}ms (failure #${this.consecutiveFailures}) for server=${this.config.serverName}`
    );

    this.restartTimer = setTimeout(() => {
      this.restartTimer = null;
      this.spawnStreamProcess();
    }, this.backoffMs);

    this.backoffMs = Math.min(
      this.backoffMs * BACKOFF_MULTIPLIER,
      MAX_BACKOFF_MS
    );
  }
}

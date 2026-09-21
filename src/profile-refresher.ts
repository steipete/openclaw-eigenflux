/** OpenClaw context and delivery adapter for the CLI-owned profile refresh task. */
import { execEigenflux } from './cli-executor';
import { Logger } from './logger';
import { EMPTY_CONTEXT, type RefreshContext } from './openclaw-context';

export interface ProfileRefresherConfig {
  serverName: string;
  eigenfluxBin: string;
  logger: Logger;
  collectContext?: () => RefreshContext | Promise<RefreshContext>;
  onRefreshPrompt: (prompt: string) => Promise<void>;
  onAuthRequired: () => Promise<void>;
}

export class EigenFluxProfileRefresher {
  private running = false;
  private inFlight: Promise<void> | null = null;
  private manualInFlight = false;

  constructor(private readonly config: ProfileRefresherConfig) {}

  isRunning(): boolean {
    return this.running;
  }

  start(): void {
    this.running = true;
  }

  stop(): void {
    this.running = false;
  }

  /** Called by the existing host heartbeat. The CLI owns timing and eligibility. */
  async tick(): Promise<void> {
    if (!this.running) return;
    await this.check(false);
  }

  /** An explicit manual request uses the central task's force option. */
  async triggerNow(): Promise<void> {
    await this.check(true);
  }

  private check(manual: boolean): Promise<void> {
    if (this.inFlight) {
      if (manual && !this.manualInFlight) {
        return this.inFlight.then(() => this.check(true));
      }
      return this.inFlight;
    }
    this.manualInFlight = manual;
    this.inFlight = this.refresh(manual).catch((err) => {
      this.config.logger.warn(
        `Profile refresh adapter failed for server=${this.config.serverName}: ${String(err)}`
      );
    }).finally(() => {
      this.inFlight = null;
      this.manualInFlight = false;
    });
    return this.inFlight;
  }

  private async refresh(manual: boolean): Promise<void> {
    let context: RefreshContext = EMPTY_CONTEXT;
    try {
      context = (await this.config.collectContext?.()) ?? EMPTY_CONTEXT;
    } catch (err) {
      this.config.logger.warn(`Profile context collection failed: ${String(err)}`);
    }
    if (!manual && !this.running) return;

    const result = await execEigenflux<string>(this.config.eigenfluxBin, [
      'profile', 'refresh-task', '-s', this.config.serverName, '--format', 'agent',
      ...(manual ? ['--force'] : []),
      ...(context.memoryDirs ?? []).flatMap((dir) => ['--memory-dir', dir]),
      ...(context.sessionSnippets ?? []).flatMap((snippet) => ['--session-snippet', snippet]),
    ], { logger: this.config.logger, parseJson: false });

    if (!manual && !this.running) return;
    if (result.kind === 'auth_required') {
      await this.config.onAuthRequired();
      return;
    }
    if (result.kind !== 'success') {
      const detail = result.kind === 'error' ? result.error.message : `CLI missing: ${result.bin}`;
      this.config.logger.warn(
        `Profile refresh task unavailable for server=${this.config.serverName}: ${detail}`
      );
      return;
    }
    const prompt = typeof result.data === 'string' ? result.data.trim() : '';
    if (!prompt) return;

    try {
      await this.config.onRefreshPrompt(prompt);
      this.config.logger.info(`profile_refresh_telemetry ${JSON.stringify({
        server: this.config.serverName,
        outcome: 'delivered',
        prompt_bytes: Buffer.byteLength(prompt, 'utf-8'),
        delivered: true,
      })}`);
    } catch (err) {
      this.config.logger.warn(`Profile refresh delivery failed: ${String(err)}`);
    }
  }
}

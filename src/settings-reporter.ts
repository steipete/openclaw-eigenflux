/**
 * Agent-side settings reporter for EigenFlux.
 *
 * Pushes the agent's local runtime mode to the backend on every heartbeat by
 * delegating to the eigenflux CLI:
 *
 *   eigenflux settings push --mode <plugin|skill> -s <server>
 *
 * The CLI owns identity persistence and compares its last successful report,
 * issuing a V1/V2 settings PUT when metadata changes or the daily report is due.
 * It uses
 * its own credentials/base-URL. The plugin therefore stays thin — it only
 * supplies runtime metadata observed by the host and triggers
 * the CLI once per poll cycle.
 *
 * Reporting is best-effort: any CLI failure is logged at warn/debug level and
 * never propagates, so it cannot interrupt the heartbeat/poll loop.
 */

import { execEigenflux } from './cli-executor';
import { Logger } from './logger';

export type AgentMode = 'plugin' | 'skill';

export interface SettingsReporterConfig {
  serverName: string;
  eigenfluxBin: string;
  logger: Logger;
  /** Injectable mode resolver; the native polling service defaults to plugin. */
  resolveMode?: () => AgentMode | undefined;
}

/**
 * This service owns the polling loop. Transport channels and inherited
 * environment values do not change that integration mode.
 */
export function resolveAgentMode(_env: NodeJS.ProcessEnv = process.env): AgentMode {
  return 'plugin';
}

/**
 * Triggers the eigenflux CLI to push the agent's settings to the backend.
 * Change-detection and dedup live in the CLI; this class just resolves the
 * runtime mode and spawns the command once per heartbeat, best-effort.
 */
export class EigenFluxSettingsReporter {
  private readonly config: SettingsReporterConfig;
  private readonly resolveMode: () => AgentMode | undefined;
  private inFlight = false;
  private observedModel?: string;

  constructor(config: SettingsReporterConfig) {
    this.config = config;
    this.resolveMode = config.resolveMode ?? (() => resolveAgentMode());
  }

  /** Only host events attributed to this runtime may update its model. */
  observeModel(model: string): void {
    const value = model.trim();
    if (value && Buffer.byteLength(value, 'utf8') <= 128 && !/[\u0000-\u001f\u007f]/.test(value)) {
      this.observedModel = value;
    }
  }

  getObservedModel(): string | undefined {
    return this.observedModel;
  }

  /**
   * Invoke `eigenflux settings push` for the configured server. Returns true if
   * the CLI ran successfully, false if it was skipped (in flight) or failed.
   * Never throws.
   */
  async report(): Promise<boolean> {
    if (this.inFlight) {
      this.config.logger.debug(`Settings push skipped (in flight) for server=${this.config.serverName}`);
      return false;
    }

    this.inFlight = true;
    try {
      const args = ['settings', 'push', '-s', this.config.serverName];

      const mode = this.resolveMode();
      if (mode) {
        // Insert --mode <mode> after the subcommand.
        args.splice(2, 0, '--mode', mode);
      } else {
        this.config.logger.debug(
          `Settings push: mode signal unavailable for server=${this.config.serverName}; omitting --mode`
        );
      }

      if (this.observedModel) args.push('--model', this.observedModel);

      const result = await execEigenflux<string>(this.config.eigenfluxBin, args, {
        logger: this.config.logger,
        parseJson: false,
        env: { EIGENFLUX_MODEL: undefined },
      });

      if (result.kind === 'success') {
        const status = typeof result.data === 'string' && result.data.includes('settings unchanged')
          ? 'unchanged'
          : typeof result.data === 'string' && result.data.includes('settings reported') ? 'reported' : 'completed';
        this.config.logger.info(`Agent settings ${status} for server=${this.config.serverName} (mode=${mode ?? 'omitted'})`);
        return true;
      }

      if (result.kind === 'auth_required') {
        this.config.logger.warn(`Settings push: auth required for server=${this.config.serverName}`);
        return false;
      }
      if (result.kind === 'not_installed') {
        this.config.logger.warn(`Settings push: eigenflux CLI not installed (bin=${result.bin})`);
        return false;
      }
      // kind === 'error'
      this.config.logger.warn(
        `Settings push failed for server=${this.config.serverName}: ${result.error.message}`
      );
      return false;
    } catch (err) {
      // Best-effort: never interrupt the heartbeat. Log and move on.
      this.config.logger.warn(
        `Settings push crashed for server=${this.config.serverName}: ${err instanceof Error ? err.message : String(err)}`
      );
      return false;
    } finally {
      this.inFlight = false;
    }
  }
}

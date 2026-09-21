import { execEigenflux } from './cli-executor';
import { Logger } from './logger';

export interface HeartbeatPlanRunnerConfig {
  eigenfluxBin: string;
  eigenfluxHome: string;
  serverName: string;
  logger: Logger;
}

export interface HeartbeatExecutionPlan {
  schema_version: 'eigenflux_heartbeat_plan.v1';
  agent_prompt: string;
  wake_on_empty: boolean;
}

function isHeartbeatExecutionPlan(value: unknown): value is HeartbeatExecutionPlan {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const plan = value as Record<string, unknown>;
  return plan.schema_version === 'eigenflux_heartbeat_plan.v1' &&
    typeof plan.agent_prompt === 'string' && plan.agent_prompt.trim().length > 0 &&
    typeof plan.wake_on_empty === 'boolean';
}

export class EigenFluxHeartbeatPlanRunner {
  private inFlight = false;

  constructor(private readonly config: HeartbeatPlanRunnerConfig) {}

  /** Fetch the current CLI instructions and delivery decision for this server. */
  async run(): Promise<HeartbeatExecutionPlan | null> {
    if (this.inFlight) {
      this.config.logger.debug('Heartbeat plan skipped because a run is already in flight');
      return null;
    }

    this.inFlight = true;
    try {
      const result = await execEigenflux<unknown>(
        this.config.eigenfluxBin,
        [
          '--homedir',
          this.config.eigenfluxHome,
          '--server',
          this.config.serverName,
          'heartbeat',
          'plan',
          '--format',
          'json',
        ],
        { logger: this.config.logger }
      );

      if (result.kind === 'success') {
        if (isHeartbeatExecutionPlan(result.data)) {
          return result.data;
        }
        this.config.logger.warn('Heartbeat plan returned an invalid execution contract; update the EigenFlux CLI');
        return null;
      }
      if (result.kind === 'auth_required') {
        this.config.logger.warn('Heartbeat plan requires EigenFlux authentication');
        return null;
      }
      if (result.kind === 'not_installed') {
        this.config.logger.warn(`Heartbeat plan: eigenflux CLI not installed (bin=${result.bin})`);
        return null;
      }
      this.config.logger.warn(`Heartbeat plan failed: ${result.error.message}`);
      return null;
    } catch (error) {
      this.config.logger.warn(
        `Heartbeat plan crashed: ${error instanceof Error ? error.message : String(error)}`
      );
      return null;
    } finally {
      this.inFlight = false;
    }
  }
}

import type { OpenClawPluginApi } from 'openclaw/plugin-sdk/plugin-entry';
import type { RoutingConfig } from './config';
import type { Logger } from './logger';

type ModelEvent = { model: string; sessionKey?: string };
type ModelContext = { agentId?: string; sessionKey?: string };
type ModelRuntime = {
  routing: RoutingConfig;
  settingsReporter: { observeModel(model: string): void };
};

function belongsToRoute(route: RoutingConfig, event: ModelEvent, context: ModelContext): boolean {
  if (event.sessionKey && context.sessionKey && event.sessionKey !== context.sessionKey) return false;
  const sessionKey = event.sessionKey || context.sessionKey;
  const sessionAgent = sessionKey?.match(/^agent:([^:]+):/i)?.[1];
  if (sessionAgent && context.agentId && sessionAgent !== context.agentId) return false;
  const agentId = sessionAgent || context.agentId;
  if (agentId && agentId !== route.agentId) return false;
  if (route.routeOverrides.sessionKey) {
    return !!sessionKey && sessionKey === route.sessionKey;
  }
  return agentId === route.agentId;
}

/** Observe host metadata only; reporting keeps the existing Feed cadence. */
export function registerRuntimeModelHooks(
  api: Pick<OpenClawPluginApi, 'on'>,
  getRuntimes: () => readonly ModelRuntime[],
  logger: Logger,
): void {
  if (typeof api.on !== 'function') return;
  const observe = (event: ModelEvent, context: ModelContext): void => {
    if (typeof event.model !== 'string' || !event.model.trim()) return;
    for (const runtime of getRuntimes()) {
      if (belongsToRoute(runtime.routing, event, context)) {
        runtime.settingsReporter.observeModel(event.model);
      }
    }
  };
  try {
    // Start events follow resolved model selection, including fallback attempts.
    // A late end event from an earlier call must not replace the newer model.
    api.on('model_call_started', observe);
  } catch {
    // Older hosts can still report their known model through the Skills/CLI path.
    logger.debug('Runtime model hook unavailable: model_call_started');
  }
}

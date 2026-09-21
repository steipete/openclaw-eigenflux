import { registerRuntimeModelHooks } from './runtime-model';
import { EigenFluxSettingsReporter } from './settings-reporter';
import { EigenFluxPollingClient } from './polling-client';
import { Logger } from './logger';
import type { RoutingConfig } from './config';
import { execEigenflux } from './cli-executor';

jest.mock('./cli-executor');
const execMock = execEigenflux as jest.MockedFunction<typeof execEigenflux>;
const logger = new Logger({ info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() });

function runtime(agentId: string, sessionKey = 'main') {
  const routing: RoutingConfig = {
    agentId, sessionKey,
    routeOverrides: { sessionKey: sessionKey !== 'main', agentId: true,
      replyChannel: false, replyTo: false, replyAccountId: false },
  };
  return { routing, settingsReporter: new EigenFluxSettingsReporter({
    serverName: agentId, eigenfluxBin: 'eigenflux', logger,
  }) };
}

test('host model events stay scoped and reach both normal Feed and settings reports', async () => {
  execMock.mockReset();
  const own = runtime('owner');
  const other = runtime('other');
  const on = jest.fn();
  const inheritedModel = process.env.EIGENFLUX_MODEL;
  registerRuntimeModelHooks({ on } as any, () => [own, other], logger);
  expect(on).toHaveBeenCalledTimes(1);
  expect(on.mock.calls[0][0]).toBe('model_call_started');
  const observe = on.mock.calls[0][1];
  observe({ model: 'current-model', sessionKey: 'agent:owner:main' }, {});
  expect(execMock).not.toHaveBeenCalled();
  expect(own.settingsReporter.getObservedModel()).toBe('current-model');
  expect(other.settingsReporter.getObservedModel()).toBeUndefined();
  expect(process.env.EIGENFLUX_MODEL).toBe(inheritedModel);

  // Feed carries the observation even while baseline accounts cannot write settings.
  execMock.mockResolvedValue({ kind: 'success', data: { items: [], notifications: [], has_more: false } });
  const feed = new EigenFluxPollingClient({
    serverName: 'owner', eigenfluxBin: 'eigenflux', logger,
    resolveModel: () => own.settingsReporter.getObservedModel(),
    resolvePollIntervalSec: async () => 600,
    onFeedPolled: async () => {}, onAuthRequired: async () => {},
  });
  await feed.pollOnce();
  expect(execMock).toHaveBeenLastCalledWith('eigenflux', expect.arrayContaining(['feed', 'poll', '-s', 'owner']),
    expect.objectContaining({ env: { EIGENFLUX_MODEL: 'current-model' } }));
  await own.settingsReporter.report();
  expect(execMock).toHaveBeenLastCalledWith('eigenflux',
    ['settings', 'push', '--mode', 'plugin', '-s', 'owner', '--model', 'current-model'],
    expect.objectContaining({ env: { EIGENFLUX_MODEL: undefined } }));

  observe({ model: 'fallback-model', sessionKey: 'agent:owner:main' }, {});
  await feed.pollOnce();
  expect(execMock).toHaveBeenLastCalledWith('eigenflux', expect.any(Array),
    expect.objectContaining({ env: { EIGENFLUX_MODEL: 'fallback-model' } }));
  await other.settingsReporter.report();
  expect(execMock).toHaveBeenLastCalledWith('eigenflux',
    ['settings', 'push', '--mode', 'plugin', '-s', 'other'], expect.any(Object));
});

test('explicit session routes reject other sessions, conflicting identities, and unknown ownership', () => {
  const own = runtime('owner', 'agent:owner:feishu:direct:bound');
  const on = jest.fn();
  registerRuntimeModelHooks({ on } as any, () => [own], logger);
  const observe = on.mock.calls[0][1];
  for (const [event, context] of [
    [{ model: 'other-session', sessionKey: 'agent:owner:main' }, { agentId: 'owner' }],
    [{ model: 'other-agent', sessionKey: own.routing.sessionKey }, { agentId: 'other' }],
    [{ model: 'conflict', sessionKey: own.routing.sessionKey }, { sessionKey: 'agent:other:main' }],
    [{ model: 'unknown' }, {}],
    [{ model: 'unknown-session' }, { agentId: 'owner' }],
  ]) observe(event, context);
  expect(own.settingsReporter.getObservedModel()).toBeUndefined();
  observe({ model: 'bound-model', sessionKey: own.routing.sessionKey }, {});
  expect(own.settingsReporter.getObservedModel()).toBe('bound-model');
  observe({ model: 'bad\nmodel', sessionKey: own.routing.sessionKey }, {});
  expect(own.settingsReporter.getObservedModel()).toBe('bound-model');
});

test('default routes require a verified Agent identity and stopped runtimes receive nothing', () => {
  const own = runtime('owner');
  let active = [own];
  const on = jest.fn();
  registerRuntimeModelHooks({ on } as any, () => active, logger);
  const observe = on.mock.calls[0][1];
  observe({ model: 'unattributed', sessionKey: 'main' }, {});
  expect(own.settingsReporter.getObservedModel()).toBeUndefined();
  observe({ model: 'current-model' }, { agentId: 'owner' });
  expect(own.settingsReporter.getObservedModel()).toBe('current-model');
  active = [];
  observe({ model: 'stopped-model' }, { agentId: 'owner' });
  expect(own.settingsReporter.getObservedModel()).toBe('current-model');
});

test('older hosts without metadata hooks retain the Skills path without failing registration', () => {
  expect(() => registerRuntimeModelHooks({} as any, () => [], logger)).not.toThrow();
  expect(() => registerRuntimeModelHooks({ on: () => { throw new Error('unsupported hook'); } } as any,
    () => [], logger)).not.toThrow();
});

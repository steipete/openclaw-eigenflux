import { execEigenflux } from './cli-executor';
import { EigenFluxHeartbeatPlanRunner } from './heartbeat-plan-runner';
import { Logger } from './logger';

jest.mock('./cli-executor');
const execMock = execEigenflux as jest.MockedFunction<typeof execEigenflux>;
const logger = new Logger({ debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() });
const config = { eigenfluxBin: '/opt/eigenflux', eigenfluxHome: '/stable/home', serverName: 'alpha', logger };
const plan = { schema_version: 'eigenflux_heartbeat_plan.v1', agent_prompt: 'CENTRAL PLAN', wake_on_empty: false };

beforeEach(() => execMock.mockReset());

test('loads the structured plan for the stable Home and explicit server', async () => {
  execMock.mockResolvedValue({ kind: 'success', data: plan });
  await expect(new EigenFluxHeartbeatPlanRunner(config).run()).resolves.toEqual(plan);
  expect(execMock).toHaveBeenCalledWith('/opt/eigenflux', [
    '--homedir', '/stable/home', '--server', 'alpha', 'heartbeat', 'plan', '--format', 'json',
  ], { logger });
});

test.each([
  'free-form old plan', null, {},
  { ...plan, schema_version: 'unknown' },
  { ...plan, agent_prompt: '' },
  { ...plan, agent_prompt: 42 },
  { ...plan, wake_on_empty: undefined },
  { ...plan, wake_on_empty: 'false' },
])('rejects an invalid central execution contract: %j', async (data) => {
  execMock.mockResolvedValue({ kind: 'success', data });
  await expect(new EigenFluxHeartbeatPlanRunner(config).run()).resolves.toBeNull();
});

test('accepts the central completed-mode delivery choice without inspecting prompt wording', async () => {
  const completed = { ...plan, wake_on_empty: true, agent_prompt: 'baseline mentioned as historical context' };
  execMock.mockResolvedValue({ kind: 'success', data: completed });
  await expect(new EigenFluxHeartbeatPlanRunner(config).run()).resolves.toEqual(completed);
});

test('plan failure returns no replacement business instructions', async () => {
  execMock.mockResolvedValue({ kind: 'error', error: new Error('offline'), exitCode: 2, stderr: 'offline' });
  await expect(new EigenFluxHeartbeatPlanRunner(config).run()).resolves.toBeNull();
});

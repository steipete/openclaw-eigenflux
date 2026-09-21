import { EigenFluxProfileRefresher, type ProfileRefresherConfig } from './profile-refresher';
import { Logger } from './logger';
import { execEigenflux } from './cli-executor';

jest.mock('./cli-executor');
const execMock = execEigenflux as jest.MockedFunction<typeof execEigenflux>;
const context = { memoryDirs: ['/host/memory'], sessionSnippets: ['Host conversation context'] };

function setup(overrides: Partial<ProfileRefresherConfig> = {}) {
  const logger = new Logger({ info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() });
  const config: ProfileRefresherConfig = {
    serverName: 'alpha', eigenfluxBin: '/bin/eigenflux', logger,
    collectContext: () => context,
    onRefreshPrompt: jest.fn().mockResolvedValue(undefined),
    onAuthRequired: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  };
  return { config, adapter: new EigenFluxProfileRefresher(config) };
}

beforeEach(() => execMock.mockReset());

test('heartbeat passes host context to the CLI and delivers its task verbatim', async () => {
  const { config, adapter } = setup();
  execMock.mockResolvedValue({ kind: 'success', data: 'CENTRAL TASK: read current Skills' });
  adapter.start();
  await adapter.tick();
  expect(execMock).toHaveBeenCalledWith('/bin/eigenflux', [
    'profile', 'refresh-task', '-s', 'alpha', '--format', 'agent',
    '--memory-dir', '/host/memory', '--session-snippet', 'Host conversation context',
  ], { logger: config.logger, parseJson: false });
  expect(config.onRefreshPrompt).toHaveBeenCalledWith('CENTRAL TASK: read current Skills');
  expect(execMock).toHaveBeenCalledTimes(1);
});

test('CLI empty output is a quiet skip, with no status-prompt chain', async () => {
  const { config, adapter } = setup();
  execMock.mockResolvedValue({ kind: 'success', data: '' });
  adapter.start();
  await adapter.tick();
  expect(config.onRefreshPrompt).not.toHaveBeenCalled();
  expect(config.onAuthRequired).not.toHaveBeenCalled();
  expect(execMock).toHaveBeenCalledTimes(1);
});

test('every host tick asks the CLI without a plugin due-time cache', async () => {
  const { config, adapter } = setup();
  execMock.mockResolvedValueOnce({ kind: 'success', data: '' });
  execMock.mockResolvedValueOnce({ kind: 'success', data: 'NOW DUE' });
  adapter.start();
  await adapter.tick();
  await adapter.tick();
  expect(execMock).toHaveBeenCalledTimes(2);
  expect(config.onRefreshPrompt).toHaveBeenCalledWith('NOW DUE');
});

test('explicit manual refresh forces the central task even while the background adapter is stopped', async () => {
  const { config, adapter } = setup();
  execMock.mockResolvedValue({ kind: 'success', data: 'TASK' });
  await adapter.tick();
  expect(execMock).not.toHaveBeenCalled();
  await adapter.triggerNow();
  expect(execMock).toHaveBeenCalledTimes(1);
  expect(execMock.mock.calls[0][1]).toEqual([
    'profile', 'refresh-task', '-s', 'alpha', '--format', 'agent', '--force',
    '--memory-dir', '/host/memory', '--session-snippet', 'Host conversation context',
  ]);
  expect(config.onRefreshPrompt).toHaveBeenCalledWith('TASK');
});

test('concurrent heartbeats share one request and one delivery', async () => {
  let resolveRequest!: (value: any) => void;
  execMock.mockReturnValue(new Promise((resolve) => { resolveRequest = resolve; }));
  const { config, adapter } = setup();
  adapter.start();
  const first = adapter.tick();
  const second = adapter.tick();
  await Promise.resolve();
  resolveRequest({ kind: 'success', data: 'TASK' });
  await Promise.all([first, second]);
  expect(execMock).toHaveBeenCalledTimes(1);
  expect(config.onRefreshPrompt).toHaveBeenCalledTimes(1);
});

test.each(['skip', 'error'])('manual requests queue one force after an in-flight background %s', async (outcome) => {
  let finishBackground!: (value: any) => void;
  execMock.mockReturnValueOnce(new Promise((resolve) => { finishBackground = resolve; }));
  execMock.mockResolvedValueOnce({ kind: 'success', data: 'FORCED TASK' });
  const { config, adapter } = setup();
  adapter.start();
  const background = adapter.tick();
  const firstManual = adapter.triggerNow();
  const secondManual = adapter.triggerNow();
  await Promise.resolve();
  expect(execMock).toHaveBeenCalledTimes(1);
  expect(execMock.mock.calls[0][1]).not.toContain('--force');
  finishBackground(outcome === 'skip'
    ? { kind: 'success', data: '' }
    : { kind: 'error', error: new Error('unavailable'), exitCode: 1, stderr: 'unavailable' });
  await Promise.all([background, firstManual, secondManual]);
  expect(execMock).toHaveBeenCalledTimes(2);
  expect(execMock.mock.calls[1][1]).toContain('--force');
  expect(config.onRefreshPrompt).toHaveBeenCalledTimes(1);
  expect(config.onRefreshPrompt).toHaveBeenCalledWith('FORCED TASK');
});

test('manual requests and heartbeats share an in-flight forced task', async () => {
  let finishRequest!: (value: any) => void;
  execMock.mockReturnValueOnce(new Promise((resolve) => { finishRequest = resolve; }));
  const { config, adapter } = setup();
  adapter.start();
  const firstManual = adapter.triggerNow();
  const background = adapter.tick();
  const secondManual = adapter.triggerNow();
  await Promise.resolve();
  finishRequest({ kind: 'success', data: 'FORCED TASK' });
  await Promise.all([firstManual, background, secondManual]);
  expect(execMock).toHaveBeenCalledTimes(1);
  expect(execMock.mock.calls[0][1]).toContain('--force');
  expect(config.onRefreshPrompt).toHaveBeenCalledTimes(1);
});

test('stopping during a CLI request suppresses late background delivery', async () => {
  let resolveRequest!: (value: any) => void;
  execMock.mockReturnValue(new Promise((resolve) => { resolveRequest = resolve; }));
  const { config, adapter } = setup();
  adapter.start();
  const pending = adapter.tick();
  await Promise.resolve();
  adapter.stop();
  resolveRequest({ kind: 'success', data: 'TASK' });
  await pending;
  expect(config.onRefreshPrompt).not.toHaveBeenCalled();
});

test('missing host context is passed to the CLI for its decision', async () => {
  const { adapter } = setup({ collectContext: undefined });
  execMock.mockResolvedValue({ kind: 'success', data: '' });
  adapter.start();
  await adapter.tick();
  expect(execMock.mock.calls[0][1]).toEqual([
    'profile', 'refresh-task', '-s', 'alpha', '--format', 'agent',
  ]);
});

test.each(['auth_required', 'error', 'not_installed'] as const)('%s does not interrupt the heartbeat', async (kind) => {
  const { config, adapter } = setup();
  execMock.mockResolvedValue(kind === 'auth_required'
    ? { kind, stderr: 'expired' }
    : kind === 'not_installed' ? { kind, bin: '/bin/eigenflux' }
      : { kind, error: new Error('scope unavailable'), exitCode: 2, stderr: 'scope unavailable' });
  adapter.start();
  await expect(adapter.tick()).resolves.toBeUndefined();
  expect(config.onRefreshPrompt).not.toHaveBeenCalled();
  expect(config.onAuthRequired).toHaveBeenCalledTimes(kind === 'auth_required' ? 1 : 0);
});

test('context collection and delivery errors stay within the host adapter', async () => {
  const { adapter } = setup({
    collectContext: async () => { throw new Error('host unavailable'); },
    onRefreshPrompt: async () => { throw new Error('host unavailable'); },
  });
  execMock.mockResolvedValue({ kind: 'success', data: 'TASK' });
  adapter.start();
  await expect(adapter.tick()).resolves.toBeUndefined();
  expect(execMock).toHaveBeenCalledTimes(1);
});

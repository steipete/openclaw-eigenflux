import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import type { OpenClawPluginApi } from 'openclaw/plugin-sdk/plugin-entry';
import { Logger } from './logger';

const readStoredNotificationRouteMock = jest.fn();
const writeStoredNotificationRouteMock = jest.fn();
jest.mock('./session-route-memory', () => ({
  readStoredNotificationRoute: (...args: any[]) => readStoredNotificationRouteMock(...args),
  writeStoredNotificationRoute: (...args: any[]) => writeStoredNotificationRouteMock(...args),
}));

import { EigenFluxNotifier } from './notifier';

function createLogger(): Logger {
  return new Logger({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  });
}

function createApi(overrides: Partial<OpenClawPluginApi> = {}): OpenClawPluginApi {
  return {
    id: 'eigenflux',
    name: 'EigenFlux',
    source: '/tmp/eigenflux',
    config: {},
    pluginConfig: {},
    runtime: {} as unknown as OpenClawPluginApi['runtime'],
    logger: {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    },
    registerService: jest.fn(),
    ...overrides,
  } as unknown as OpenClawPluginApi;
}

function createConfig() {
  return {
    sessionKey: 'agent:main:feishu:direct:ou_123',
    agentId: 'main',
    replyChannel: 'feishu',
    replyTo: 'user:ou_123',
    openclawCliBin: 'openclaw',
  };
}

describe('EigenFluxNotifier', () => {
  beforeEach(() => {
    readStoredNotificationRouteMock.mockReset();
    writeStoredNotificationRouteMock.mockReset();
    readStoredNotificationRouteMock.mockResolvedValue(undefined);
    writeStoredNotificationRouteMock.mockResolvedValue(true);
  });

  test('prefers runtime.subagent delivery when available', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-subagent' });

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(true);
    expect(run).toHaveBeenCalledWith({
      sessionKey: 'agent:main:feishu:direct:ou_123',
      message: '[EIGENFLUX_TEST] payload',
      deliver: true,
      idempotencyKey: expect.any(String),
      lane: 'eigenflux-bg',
    });
  });

  test('silent delivery runs the subagent with deliver:false and skips heartbeat', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-silent' });
    const enqueueSystemEvent = jest.fn().mockReturnValue(true);
    const requestHeartbeatNow = jest.fn();

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run },
          system: { enqueueSystemEvent, requestHeartbeatNow },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await expect(
      notifier.deliver('[EIGENFLUX_TEST] silent', { silent: true })
    ).resolves.toBe(true);

    // Agent loop still runs (so it can call the CLI) — but with delivery off.
    expect(run).toHaveBeenCalledWith({
      sessionKey: 'agent:main:feishu:direct:ou_123',
      message: '[EIGENFLUX_TEST] silent',
      deliver: false,
      idempotencyKey: expect.any(String),
      lane: 'eigenflux-bg',
    });
    // Heartbeat fallback surfaces in the user's main session: must NOT fire.
    expect(enqueueSystemEvent).not.toHaveBeenCalled();
    expect(requestHeartbeatNow).not.toHaveBeenCalled();
  });

  test('on waitForRun timeout: reports failure without fallback (older host, no tasks API)', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-subagent-pending' });
    const waitForRun = jest.fn().mockResolvedValue({ status: 'timeout' });
    const runCommandWithTimeout = jest.fn();

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run, waitForRun },
          system: { runCommandWithTimeout },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(false);
    expect(run).toHaveBeenCalledTimes(1);
    expect(waitForRun).toHaveBeenCalledTimes(1);
    // Fallbacks must not run — retrying via CLI would re-run the loop and dup-deliver.
    expect(runCommandWithTimeout).not.toHaveBeenCalled();
  });

  test.each(['async', 'legacy'])('on waitForRun timeout: cancels using %s task reads', async (reader) => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-stuck' });
    const waitForRun = jest.fn().mockResolvedValue({ status: 'timeout' });
    const cancel = jest.fn().mockResolvedValue({ found: true, cancelled: true });
    // list() returns the run keyed by runId; its `id` is the taskId cancel needs.
    const tasks = [
      { id: 'task-other', runId: 'run-unrelated', startedAt: Date.now() },
      { id: 'task-stuck', runId: 'run-stuck', startedAt: Date.now() - 20_000 },
    ];
    let releaseList!: () => void;
    const pendingList = new Promise<typeof tasks>((resolve) => {
      releaseList = () => resolve(tasks);
    });
    let markListStarted!: () => void;
    const listStarted = new Promise<void>((resolve) => { markListStarted = resolve; });
    const list = jest.fn().mockImplementation(() => {
      markListStarted();
      return reader === 'async' ? pendingList : tasks;
    });
    const bindSession = jest.fn().mockReturnValue({ list, cancel });
    const runCommandWithTimeout = jest.fn();

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run, waitForRun },
          tasks: {
            ...(reader === 'async' ? { async: { runs: { bindSession } } } : {}),
            runs: { bindSession },
          },
          system: { runCommandWithTimeout },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    const delivery = notifier.deliver('[EIGENFLUX_TEST] payload');
    await Promise.race([
      listStarted,
      delivery.then(() => { throw new Error('Delivery skipped the task lookup'); }),
    ]);
    if (reader === 'async') expect(cancel).not.toHaveBeenCalled();
    releaseList();
    await expect(delivery).resolves.toBe(false);
    expect(bindSession).toHaveBeenCalledWith({ sessionKey: 'agent:main:feishu:direct:ou_123' });
    expect(waitForRun).toHaveBeenCalledWith({ runId: 'run-stuck', timeoutMs: 480_000 });
    // Cancels the matching run by its taskId — not the unrelated one.
    expect(cancel).toHaveBeenCalledWith({ taskId: 'task-stuck', cfg: expect.anything() });
    // Still no CLI fallback (would dup-deliver).
    expect(runCommandWithTimeout).not.toHaveBeenCalled();
  });

  test.each(['empty', 'rejected'])('does not retry an async %s task lookup for cancellation', async (result) => {
    const list = jest.fn();
    if (result === 'rejected') list.mockRejectedValue(new Error('task storage unavailable'));
    else list.mockResolvedValue([]);
    const legacyList = jest.fn().mockReturnValue([{ id: 'task-stuck', runId: 'run-stuck' }]);
    const cancel = jest.fn();
    const api = createApi();
    Object.assign(api.runtime, {
      subagent: {
        run: jest.fn().mockResolvedValue({ runId: 'run-stuck' }),
        waitForRun: jest.fn().mockResolvedValue({ status: 'timeout' }),
      },
      tasks: {
        async: { runs: { bindSession: () => ({ list }) } },
        runs: { bindSession: () => ({ list: legacyList, cancel }) },
      },
    });
    const notifier = new EigenFluxNotifier(api, createLogger(), createConfig());
    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(false);
    expect(list).toHaveBeenCalledTimes(1);
    expect(legacyList).not.toHaveBeenCalled();
    expect(cancel).not.toHaveBeenCalled();
  });

  test('recognizes completion even when the task registry never exposes the run', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-completed' });
    const waitForRun = jest.fn().mockResolvedValue({ status: 'ok' });
    const cancel = jest.fn();
    const list = jest.fn().mockResolvedValue([]);
    const bindSession = jest.fn().mockReturnValue({ list, cancel });

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run, waitForRun },
          tasks: { async: { runs: { bindSession } }, runs: { bindSession } },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] completed payload')).resolves.toBe(true);
    expect(waitForRun).toHaveBeenCalledWith({ runId: 'run-completed', timeoutMs: 480_000 });
    expect(bindSession).not.toHaveBeenCalled();
    expect(list).not.toHaveBeenCalled();
    expect(cancel).not.toHaveBeenCalled();
  });

  test('falls back to runtime command agent when waitForRun reports error', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-subagent-fail' });
    const waitForRun = jest
      .fn()
      .mockResolvedValue({ status: 'error', error: 'channel delivery failed' });
    const runCommandWithTimeout = jest.fn().mockResolvedValue({
      code: 0,
      stdout: 'ok',
      stderr: '',
    });

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run, waitForRun },
          system: { runCommandWithTimeout },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(true);
    expect(runCommandWithTimeout).toHaveBeenCalledWith(
      expect.arrayContaining(['openclaw', 'agent', '--deliver']),
      { timeoutMs: 15000 }
    );
  });

  test('falls back to runtime command agent when runtime.subagent is unavailable', async () => {
    const runCommandWithTimeout = jest.fn().mockResolvedValue({
      code: 0,
      stdout: 'ok',
      stderr: '',
    });

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          system: { runCommandWithTimeout },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(true);
    expect(runCommandWithTimeout).toHaveBeenCalledWith(
      [
        'openclaw',
        'agent',
        '--message',
        '[EIGENFLUX_TEST] payload',
        '--agent',
        'main',
        '--deliver',
        '--reply-channel',
        'feishu',
        '--reply-to',
        'user:ou_123',
      ],
      { timeoutMs: 15000 }
    );
  });

  test('falls back to runtime heartbeat when command path is unavailable', async () => {
    const enqueueSystemEvent = jest.fn().mockReturnValue(true);
    const requestHeartbeatNow = jest.fn();
    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          system: {
            enqueueSystemEvent,
            requestHeartbeatNow,
          },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(true);
    expect(enqueueSystemEvent).toHaveBeenCalledWith('[EIGENFLUX_TEST] payload', {
      sessionKey: 'agent:main:feishu:direct:ou_123',
      deliveryContext: {
        channel: 'feishu',
        to: 'user:ou_123',
      },
    });
    expect(requestHeartbeatNow).toHaveBeenCalledWith({
      reason: 'plugin:eigenflux',
      coalesceMs: 0,
      agentId: 'main',
      sessionKey: 'agent:main:feishu:direct:ou_123',
    });
  });

  test('deliverToMainSession recovers reply target from remembered route when config has none', async () => {
    // The feed poller's config carries no reply target; without recovery the
    // enqueued system event would have deliveryContext=none and the main
    // session's heartbeat reply would never reach the user.
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), 'eigenflux-main-session-'));
    const sessionStorePath = path.join(stateDir, 'sessions.json');
    fs.writeFileSync(sessionStorePath, JSON.stringify({}), 'utf-8');

    readStoredNotificationRouteMock.mockResolvedValue({
      sessionKey: 'agent:main:feishu:direct:ou_123',
      agentId: 'main',
      replyChannel: 'feishu',
      replyTo: 'user:ou_123',
      replyAccountId: 'default',
    });

    const enqueueSystemEvent = jest.fn().mockReturnValue(true);
    const requestHeartbeatNow = jest.fn();
    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          system: { enqueueSystemEvent, requestHeartbeatNow },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      {
        ...createConfig(),
        sessionKey: 'main',
        replyChannel: undefined,
        replyTo: undefined,
        replyAccountId: undefined,
        sessionStorePath,
      }
    );

    await expect(notifier.deliverToMainSession('[EIGENFLUX_TEST] payload')).resolves.toBe(true);
    // The event must be enqueued under the CANONICAL session key from the
    // remembered route, NOT the config alias "main": the host bridge does not
    // canonicalize on enqueue, while every consumer (heartbeat runner,
    // user-message runs) drains the canonical key — an alias-keyed event would
    // never be drained. Reply target is borrowed from the same route.
    expect(enqueueSystemEvent).toHaveBeenCalledWith('[EIGENFLUX_TEST] payload', {
      sessionKey: 'agent:main:feishu:direct:ou_123',
      deliveryContext: {
        channel: 'feishu',
        to: 'user:ou_123',
        accountId: 'default',
      },
    });
    expect(requestHeartbeatNow).toHaveBeenCalledWith({
      reason: 'plugin:eigenflux',
      coalesceMs: 0,
      agentId: 'main',
      sessionKey: 'agent:main:feishu:direct:ou_123',
    });

    fs.rmSync(stateDir, { recursive: true, force: true });
  });

  describe('isMainRouteBusy', () => {
    function createBusyProbe(runtime: Record<string, unknown>) {
      return new EigenFluxNotifier(
        createApi({ runtime: runtime as unknown as OpenClawPluginApi['runtime'] }),
        createLogger(),
        createConfig()
      );
    }

    test('terminal task history alone does NOT count as busy', async () => {
      const list = jest.fn().mockResolvedValue([
        { id: 't1', runId: 'r1', status: 'succeeded', endedAt: 111 },
        { id: 't2', runId: 'r2', status: 'timed_out', endedAt: 222 },
      ]);
      const notifier = createBusyProbe({
        tasks: { async: { runs: { bindSession: () => ({ list }) } } },
      });
      await expect(notifier.isMainRouteBusy(90_000)).resolves.toBe(false);
    });

    test.each(['async', 'legacy'])('a task without endedAt counts as busy with %s reads', async (reader) => {
      const tasks = [
        { id: 't1', runId: 'r1', status: 'succeeded', endedAt: 111 },
        { id: 't2', runId: 'r2', status: 'running' }, // no endedAt → live
      ];
      const list = jest.fn().mockImplementation(() => reader === 'async' ? Promise.resolve(tasks) : tasks);
      const notifier = createBusyProbe({
        tasks: {
          ...(reader === 'async' ? { async: { runs: { bindSession: () => ({ list }) } } } : {}),
          runs: { bindSession: () => ({ list }) },
        },
      });
      await expect(notifier.isMainRouteBusy(90_000)).resolves.toBe(true);
    });

    test.each(['empty', 'rejected'])('an async %s task result does not retry a legacy busy read', async (result) => {
      const list = jest.fn();
      if (result === 'rejected') list.mockRejectedValue(new Error('task storage unavailable'));
      else list.mockResolvedValue([]);
      const legacyList = jest.fn().mockReturnValue([{ id: 'legacy-active-task' }]);
      const notifier = createBusyProbe({
        tasks: {
          async: { runs: { bindSession: () => ({ list }) } },
          runs: { bindSession: () => ({ list: legacyList }) },
        },
      });
      await expect(notifier.isMainRouteBusy(90_000)).resolves.toBe(false);
      expect(legacyList).not.toHaveBeenCalled();
    });

    test('fresh session updatedAt counts as busy; stale does not', async () => {
      const getSessionEntry = jest
        .fn()
        .mockReturnValueOnce({ updatedAt: Date.now() - 5_000 })   // fresh
        .mockReturnValueOnce({ updatedAt: Date.now() - 120_000 }); // stale
      const notifier = createBusyProbe({
        agent: { session: { getSessionEntry } },
      });
      await expect(notifier.isMainRouteBusy(90_000)).resolves.toBe(true);
      await expect(notifier.isMainRouteBusy(90_000)).resolves.toBe(false);
    });

    test('awaits async session activity before falling back to an idle result', async () => {
      let releaseEntry!: (entry: { updatedAt: number }) => void;
      const pendingEntry = new Promise<{ updatedAt: number }>((resolve) => {
        releaseEntry = resolve;
      });
      let markReadStarted!: () => void;
      const readStarted = new Promise<void>((resolve) => { markReadStarted = resolve; });
      const getSessionEntryAsync = jest.fn().mockImplementation(() => {
        markReadStarted();
        return pendingEntry;
      });
      const getSessionEntry = jest.fn().mockReturnValue(undefined);
      const notifier = createBusyProbe({
        agent: { session: { getSessionEntryAsync, getSessionEntry } },
      });
      const busy = notifier.isMainRouteBusy(90_000);
      await Promise.race([
        readStarted,
        busy.then(() => { throw new Error('Busy check finished before the async session read'); }),
      ]);
      releaseEntry({ updatedAt: Date.now() });
      await expect(busy).resolves.toBe(true);
      expect(getSessionEntry).not.toHaveBeenCalled();
    });

    test.each(['missing', 'rejected'])('an async %s result does not retry a sync read', async (result) => {
      const getSessionEntryAsync = jest.fn();
      if (result === 'rejected') {
        getSessionEntryAsync.mockRejectedValue(new Error('session storage unavailable'));
      } else {
        getSessionEntryAsync.mockResolvedValue(undefined);
      }
      const getSessionEntry = jest.fn().mockReturnValue({ updatedAt: Date.now() });
      const notifier = createBusyProbe({
        agent: { session: { getSessionEntryAsync, getSessionEntry } },
      });
      await expect(notifier.isMainRouteBusy(90_000)).resolves.toBe(false);
      expect(getSessionEntry).not.toHaveBeenCalled();
    });

    test('no busy APIs at all → idle (degrades to immediate push)', async () => {
      const notifier = createBusyProbe({});
      await expect(notifier.isMainRouteBusy(90_000)).resolves.toBe(false);
    });

    test('a rejected task lookup still checks recent session activity', async () => {
      const list = jest.fn().mockRejectedValue(new Error('task storage unavailable'));
      const getSessionEntry = jest.fn().mockReturnValue({ updatedAt: Date.now() });
      const notifier = createBusyProbe({
        tasks: { async: { runs: { bindSession: () => ({ list }) } } },
        agent: { session: { getSessionEntry } },
      });
      await expect(notifier.isMainRouteBusy(90_000)).resolves.toBe(true);
      expect(list).toHaveBeenCalledTimes(1);
    });
  });

  test('prefers direct session over newer group session when scanning session store', async () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), 'eigenflux-session-store-'));
    const sessionStorePath = path.join(stateDir, 'sessions.json');
    fs.writeFileSync(
      sessionStorePath,
      JSON.stringify({
        'agent:main:main': {
          updatedAt: 100,
          deliveryContext: { channel: 'webchat' },
        },
        'agent:main:feishu:direct:ou_older': {
          updatedAt: 200,
          deliveryContext: {
            channel: 'feishu',
            to: 'user:ou_older',
            accountId: 'default',
          },
        },
        'agent:main:feishu:group:oc_latest': {
          updatedAt: 300,
          deliveryContext: {
            channel: 'feishu',
            to: 'chat:oc_latest',
            accountId: 'default',
          },
        },
      }),
      'utf-8'
    );

    const run = jest.fn().mockResolvedValue({ runId: 'run-external-session' });
    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      {
        ...createConfig(),
        sessionKey: 'main',
        replyChannel: undefined,
        replyTo: undefined,
        replyAccountId: undefined,
        sessionStorePath,
      }
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(true);
    expect(run).toHaveBeenCalledWith({
      sessionKey: 'agent:main:feishu:direct:ou_older',
      message: '[EIGENFLUX_TEST] payload',
      deliver: true,
      idempotencyKey: expect.any(String),
      lane: 'eigenflux-bg',
    });

    fs.rmSync(stateDir, { recursive: true, force: true });
  });

  test('normalizes explicit reply targets from session store before CLI fallback', async () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), 'eigenflux-session-store-'));
    const sessionStorePath = path.join(stateDir, 'sessions.json');
    fs.writeFileSync(
      sessionStorePath,
      JSON.stringify({
        'agent:main:feishu:direct:ou_123': {
          updatedAt: 300,
          deliveryContext: {
            channel: 'feishu',
            to: 'user:ou_123',
            accountId: 'default',
          },
        },
      }),
      'utf-8'
    );

    const runCommandWithTimeout = jest.fn().mockResolvedValue({
      code: 0,
      stdout: 'ok',
      stderr: '',
    });

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          system: { runCommandWithTimeout },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      {
        ...createConfig(),
        sessionKey: 'main',
        replyChannel: 'feishu',
        replyTo: 'user:ou_123',
        sessionStorePath,
      }
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(true);
    expect(runCommandWithTimeout).toHaveBeenCalledWith(
      [
        'openclaw',
        'agent',
        '--message',
        '[EIGENFLUX_TEST] payload',
        '--agent',
        'main',
        '--deliver',
        '--reply-channel',
        'feishu',
        '--reply-to',
        'user:ou_123',
        '--reply-account',
        'default',
      ],
      { timeoutMs: 15000 }
    );

    fs.rmSync(stateDir, { recursive: true, force: true });
  });

  test('remembers the resolved route after a successful delivery', async () => {
    const runtimeStoreMock = { get: jest.fn(), set: jest.fn() };
    const run = jest.fn().mockResolvedValue({ runId: 'run-subagent-memory' });
    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      {
        ...createConfig(),
        store: runtimeStoreMock,
        serverName: 'eigenflux',
      }
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(true);

    expect(writeStoredNotificationRouteMock).toHaveBeenCalledTimes(1);
    const [store, server, route] = writeStoredNotificationRouteMock.mock.calls[0];
    expect(store).toBe(runtimeStoreMock);
    expect(server).toBe('eigenflux');
    expect(route).toMatchObject({
      sessionKey: 'agent:main:feishu:direct:ou_123',
      agentId: 'main',
      replyChannel: 'feishu',
      replyTo: 'user:ou_123',
    });
  });

  test('re-resolves fresh route and retries delivery when remembered route fails', async () => {
    readStoredNotificationRouteMock.mockResolvedValue({
      sessionKey: 'agent:main:feishu:direct:ou_stale',
      agentId: 'main',
      replyChannel: 'feishu',
      replyTo: 'user:ou_stale',
      replyAccountId: 'default',
      updatedAt: 1,
    });

    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), 'eigenflux-retry-'));
    const sessionStorePath = path.join(stateDir, 'sessions.json');
    fs.writeFileSync(
      sessionStorePath,
      JSON.stringify({
        'agent:main:feishu:direct:ou_fresh': {
          updatedAt: 500,
          deliveryContext: {
            channel: 'feishu',
            to: 'user:ou_fresh',
            accountId: 'default',
          },
        },
      }),
      'utf-8'
    );

    const run = jest
      .fn()
      .mockRejectedValueOnce(new Error('session expired'))
      .mockResolvedValueOnce({ runId: 'run-fresh' });

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      {
        ...createConfig(),
        eigenfluxBin: 'eigenflux',
        serverName: 'eigenflux',
        sessionKey: 'main',
        replyChannel: undefined,
        replyTo: undefined,
        replyAccountId: undefined,
        sessionStorePath,
      }
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(true);

    expect(run).toHaveBeenCalledTimes(2);
    expect(run.mock.calls[0][0].sessionKey).toBe('agent:main:feishu:direct:ou_stale');
    expect(run.mock.calls[1][0].sessionKey).toBe('agent:main:feishu:direct:ou_fresh');

    expect(writeStoredNotificationRouteMock).toHaveBeenCalledTimes(1);
    const [, , savedRoute] = writeStoredNotificationRouteMock.mock.calls[0];
    expect(savedRoute.sessionKey).toBe('agent:main:feishu:direct:ou_fresh');

    fs.rmSync(stateDir, { recursive: true, force: true });
  });

  test('does not overwrite remembered external routes with an internal main fallback', async () => {
    readStoredNotificationRouteMock.mockResolvedValue({
      sessionKey: 'agent:main:feishu:group:oc_saved',
      agentId: 'main',
      replyChannel: 'feishu',
      replyTo: 'chat:oc_saved',
      replyAccountId: 'default',
      updatedAt: 1,
    });

    const run = jest.fn().mockResolvedValue({ runId: 'run-subagent-main' });
    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      {
        ...createConfig(),
        eigenfluxBin: 'eigenflux',
        serverName: 'eigenflux',
        sessionKey: 'main',
        agentId: 'main',
        replyChannel: undefined,
        replyTo: undefined,
        replyAccountId: undefined,
      }
    );

    await expect(notifier.deliver('[EIGENFLUX_TEST] payload')).resolves.toBe(true);

    expect(writeStoredNotificationRouteMock).not.toHaveBeenCalled();
    expect(run).toHaveBeenCalledWith(
      expect.objectContaining({ sessionKey: 'agent:main:feishu:group:oc_saved' })
    );
  });

  test('persists a legacy agent:<id>:main DM route after delivery', async () => {
    const runSpy = jest.fn().mockResolvedValue({ runId: 'run-1' });
    const waitSpy = jest.fn().mockResolvedValue({ status: 'ok' });
    const api = createApi({
      runtime: {
        subagent: { run: runSpy, waitForRun: waitSpy },
      } as any,
    });

    const notifier = new EigenFluxNotifier(api, createLogger(), {
      ...createConfig(),
      sessionKey: 'agent:main:main',
      replyChannel: 'feishu',
      replyTo: 'user:ou_legacy',
      replyAccountId: 'default',
      openclawCliBin: 'openclaw',
      routeOverrides: {
        sessionKey: true,
        agentId: true,
        replyChannel: true,
        replyTo: true,
        replyAccountId: true,
      },
    } as any);

    const ok = await notifier.deliver('hello');
    expect(ok).toBe(true);
    expect(writeStoredNotificationRouteMock).toHaveBeenCalled();
    const savedRoute = writeStoredNotificationRouteMock.mock.calls[0][2];
    expect(savedRoute.sessionKey).toBe('agent:main:main');
    expect(savedRoute.replyTo).toBe('user:ou_legacy');
  });

  test('does NOT persist a route with an internal sessionKey (heartbeat)', async () => {
    const runSpy = jest.fn().mockResolvedValue({ runId: 'run-2' });
    const waitSpy = jest.fn().mockResolvedValue({ status: 'ok' });
    const api = createApi({
      runtime: {
        subagent: { run: runSpy, waitForRun: waitSpy },
      } as any,
    });

    const notifier = new EigenFluxNotifier(api, createLogger(), {
      ...createConfig(),
      sessionKey: 'agent:main:heartbeat',
      replyChannel: 'feishu',
      replyTo: 'user:ou_x',
      replyAccountId: 'default',
      openclawCliBin: 'openclaw',
      routeOverrides: {
        sessionKey: true,
        agentId: true,
        replyChannel: true,
        replyTo: true,
        replyAccountId: true,
      },
    } as any);

    await notifier.deliver('hello');
    expect(writeStoredNotificationRouteMock).not.toHaveBeenCalled();
  });

  test('does NOT persist a route missing replyChannel', async () => {
    const runSpy = jest.fn().mockResolvedValue({ runId: 'run-3' });
    const waitSpy = jest.fn().mockResolvedValue({ status: 'ok' });
    const api = createApi({
      runtime: {
        subagent: { run: runSpy, waitForRun: waitSpy },
      } as any,
    });

    const notifier = new EigenFluxNotifier(api, createLogger(), {
      ...createConfig(),
      sessionKey: 'agent:main:main',
      replyChannel: undefined,
      replyTo: undefined,
      replyAccountId: undefined,
      openclawCliBin: 'openclaw',
      routeOverrides: {
        sessionKey: true,
        agentId: true,
        replyChannel: true,
        replyTo: true,
        replyAccountId: true,
      },
    } as any);

    await notifier.deliver('hello');
    expect(writeStoredNotificationRouteMock).not.toHaveBeenCalled();
  });

  test('uses one-shot session derived from targetSessionKey prefix', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-oneshot' });
    const deleteSession = jest.fn().mockResolvedValue(undefined);

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run, deleteSession },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await expect(
      notifier.deliver('[EIGENFLUX_FEED_PAYLOAD] test', {
        targetSessionKey: 'eigenflux:feed:eigenflux',
      })
    ).resolves.toBe(true);

    // sessionKey should start with the prefix but have a random suffix
    const callArgs = run.mock.calls[0]?.[0];
    expect(callArgs.sessionKey).toMatch(/^eigenflux:feed:eigenflux:\d+-[a-f0-9]{8}$/);
    expect(callArgs.message).toBe('[EIGENFLUX_FEED_PAYLOAD] test');
    expect(callArgs.deliver).toBe(true);
  });

  test('uses a stable persistent PM session and conversation-specific lane without cleanup', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-pm' });
    const deleteSession = jest.fn().mockResolvedValue(undefined);

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run, deleteSession },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await expect(
      notifier.deliver('[EIGENFLUX_MSG_PAYLOAD] test', {
        persistentSessionKey: 'eigenflux:pm:server:conversation',
        lane: 'eigenflux-pm:server:conversation',
      })
    ).resolves.toBe(true);

    expect(run).toHaveBeenCalledWith({
      sessionKey: 'eigenflux:pm:server:conversation',
      message: '[EIGENFLUX_MSG_PAYLOAD] test',
      deliver: true,
      idempotencyKey: expect.any(String),
      lane: 'eigenflux-pm:server:conversation',
    });
    expect(deleteSession).not.toHaveBeenCalled();
    expect(writeStoredNotificationRouteMock).not.toHaveBeenCalled();
  });

  test('deletes one-shot session after successful delivery', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-cleanup' });
    const deleteSession = jest.fn().mockResolvedValue(undefined);

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run, deleteSession },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await notifier.deliver('[EIGENFLUX_FEED_PAYLOAD] test', {
      targetSessionKey: 'eigenflux:feed:eigenflux',
    });

    expect(deleteSession).toHaveBeenCalledTimes(1);
    const deletedKey = deleteSession.mock.calls[0]?.[0]?.sessionKey;
    expect(deletedKey).toMatch(/^eigenflux:feed:eigenflux:\d+-[a-f0-9]{8}$/);
    expect(deleteSession.mock.calls[0]?.[0]?.deleteTranscript).toBe(true);
  });

  test('deletes one-shot session even when delivery fails', async () => {
    // No subagent available — delivery fails
    const deleteSession = jest.fn().mockResolvedValue(undefined);

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { deleteSession },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    const result = await notifier.deliver('[EIGENFLUX_FEED_PAYLOAD] test', {
      targetSessionKey: 'eigenflux:feed:eigenflux',
    });

    expect(result).toBe(false);
    // Session should still be cleaned up
    expect(deleteSession).toHaveBeenCalledTimes(1);
  });

  test('does NOT remember route when targetSessionKey is used', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-no-remember' });
    const deleteSession = jest.fn().mockResolvedValue(undefined);

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run, deleteSession },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await notifier.deliver('[EIGENFLUX_FEED_PAYLOAD] test', {
      targetSessionKey: 'eigenflux:feed:eigenflux',
    });

    expect(writeStoredNotificationRouteMock).not.toHaveBeenCalled();
  });

  test('skips heartbeat fallbacks when targetSessionKey is provided', async () => {
    const runtimeCommand = jest.fn().mockResolvedValue({ code: 1, stderr: 'fail' });
    const enqueueSystemEvent = jest.fn().mockReturnValue(true);
    const requestHeartbeatNow = jest.fn();
    const deleteSession = jest.fn().mockResolvedValue(undefined);

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { deleteSession },
          system: {
            runCommandWithTimeout: runtimeCommand,
            enqueueSystemEvent,
            requestHeartbeatNow,
          },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    await notifier.deliver('[EIGENFLUX_FEED_PAYLOAD] test', {
      targetSessionKey: 'eigenflux:feed:eigenflux',
    });

    expect(enqueueSystemEvent).not.toHaveBeenCalled();
    expect(requestHeartbeatNow).not.toHaveBeenCalled();
  });

  test('gracefully degrades when deleteSession is not available on runtime', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-no-delete' });
    // subagent has run but NO deleteSession
    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    const result = await notifier.deliver('[EIGENFLUX_FEED_PAYLOAD] test', {
      targetSessionKey: 'eigenflux:feed:eigenflux',
    });

    // Delivery should succeed even without deleteSession
    expect(result).toBe(true);
    expect(run).toHaveBeenCalledTimes(1);
  });

  test('deliver returns original result when deleteSession throws', async () => {
    const run = jest.fn().mockResolvedValue({ runId: 'run-delete-throws' });
    const deleteSession = jest.fn().mockRejectedValue(new Error('delete RPC failed'));

    const notifier = new EigenFluxNotifier(
      createApi({
        runtime: {
          subagent: { run, deleteSession },
        } as unknown as OpenClawPluginApi['runtime'],
      }),
      createLogger(),
      createConfig()
    );

    // Delivery should still return true — deleteSession failure is swallowed
    const result = await notifier.deliver('[EIGENFLUX_FEED_PAYLOAD] test', {
      targetSessionKey: 'eigenflux:feed:eigenflux',
    });

    expect(result).toBe(true);
    expect(deleteSession).toHaveBeenCalledTimes(1);
  });

  describe('seeds deliveryContext for one-shot feed sessions', () => {
    test('writes the resolved route into the one-shot session store before the run', async () => {
      const run = jest.fn().mockResolvedValue({ runId: 'run-seed' });
      // Assert seeding happens BEFORE the run (so the deliver:true run can read it).
      const callOrder: string[] = [];
      const patchSessionEntry = jest.fn(async (_params: any) => {
        callOrder.push('seed');
        return null;
      });
      const resolveStorePath = jest.fn(() => '/tmp/sessions/main.json');

      run.mockImplementationOnce(async (_params: any) => {
        callOrder.push('run');
        return { runId: 'run-seed' };
      });

      const notifier = new EigenFluxNotifier(
        createApi({
          config: { session: { store: 'sessions' } } as any,
          runtime: {
            subagent: { run },
            agent: { session: { resolveStorePath, patchSessionEntry } },
          } as unknown as OpenClawPluginApi['runtime'],
        }),
        createLogger(),
        { ...createConfig(), replyAccountId: 'default' }
      );

      await expect(
        notifier.deliver('[EIGENFLUX_FEED_PAYLOAD] x', { targetSessionKey: 'eigenflux:feed:eigenflux' })
      ).resolves.toBe(true);

      expect(resolveStorePath).toHaveBeenCalledWith('sessions', { agentId: 'main' });
      expect(patchSessionEntry).toHaveBeenCalledTimes(1);

      // Row-scoped patch targets the one-shot key and merges deliveryContext (+ last* mirror).
      const params = patchSessionEntry.mock.calls[0][0];
      const sessionKey = run.mock.calls[0][0].sessionKey;
      expect(sessionKey).toMatch(/^eigenflux:feed:eigenflux:/);
      expect(params.sessionKey).toBe(sessionKey);
      expect(params.agentId).toBe('main');
      expect(params.storePath).toBe('/tmp/sessions/main.json');
      const patch = params.update({}, { existingEntry: undefined });
      expect(patch).toMatchObject({
        deliveryContext: { channel: 'feishu', to: 'user:ou_123', accountId: 'default' },
        lastChannel: 'feishu',
        lastTo: 'user:ou_123',
        lastAccountId: 'default',
      });

      expect(callOrder).toEqual(['seed', 'run']);
    });

    test('delivery still proceeds when the session-store API is unavailable', async () => {
      const run = jest.fn().mockResolvedValue({ runId: 'run-no-seed' });

      const notifier = new EigenFluxNotifier(
        createApi({
          runtime: {
            subagent: { run },
            // no runtime.agent.session
          } as unknown as OpenClawPluginApi['runtime'],
        }),
        createLogger(),
        createConfig()
      );

      await expect(
        notifier.deliver('[EIGENFLUX_FEED_PAYLOAD] y', { targetSessionKey: 'eigenflux:feed:eigenflux' })
      ).resolves.toBe(true);
      expect(run).toHaveBeenCalledTimes(1);
    });
  });
});

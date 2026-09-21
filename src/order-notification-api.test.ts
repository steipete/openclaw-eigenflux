import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createOrderNotificationRequest } from './order-notification-api';
import { OrderNotifications } from './order-notifications';
import { execEigenflux } from './cli-executor';
import { Logger } from './logger';

jest.mock('./cli-executor', () => ({ execEigenflux: jest.fn() }));
const cli = jest.mocked(execEigenflux);
const notification = {
  notification_id: '358908387632611329', source_type: 'commission_order',
  payload: { order_id: '358906519414112256', to_state: 'in_progress' },
};
const logger = new Logger({ info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() });
const response = (status: number, data?: unknown) => ({
  ok: status === 200, status, json: async () => ({ data }),
} as Response);

let home: string;
let credentialsPath: string;
let fetchMock: jest.SpyInstance;
let request: ReturnType<typeof createOrderNotificationRequest>;
function credentials(token: string, agent = '42') {
  fs.writeFileSync(credentialsPath, JSON.stringify({ agent_id: agent, access_token: token, expires_at: 1 }));
}

beforeEach(() => {
  home = fs.mkdtempSync(path.join(os.tmpdir(), 'order-auth-'));
  credentialsPath = path.join(home, 'servers', 'production', 'agent-v2-credentials.json');
  fs.mkdirSync(path.dirname(credentialsPath), { recursive: true });
  credentials('expired-token');
  cli.mockReset().mockImplementation(async () => {
    credentials('renewed-token');
    return { kind: 'success', data: {} };
  });
  fetchMock = jest.spyOn(globalThis, 'fetch');
  request = createOrderNotificationRequest({
    eigenfluxBin: '/configured/eigenflux', eigenfluxHome: home,
    serverName: 'production', endpoint: 'https://example.test/', agentID: '42', logger,
  });
});
afterEach(() => {
  fetchMock.mockRestore();
  fs.rmSync(home, { recursive: true, force: true });
});

test('expired polling credentials recover through CLI and deliver a paid order exactly once', async () => {
  fetchMock.mockImplementation(async (_url, options) => {
    if (options.headers.Authorization === 'Bearer expired-token') return response(401);
    if (options.method === 'POST') return response(200, { acknowledged: 1 });
    return response(200, { notifications: [notification], has_more: false });
  });
  const deliver = jest.fn().mockResolvedValue(true);
  const queue = new OrderNotifications(request, deliver);
  await queue.reconcile();
  await queue.reconcile();
  expect(deliver).toHaveBeenCalledTimes(1);
  expect(deliver).toHaveBeenCalledWith(notification, expect.any(Object), expect.any(Function));
  expect(cli).toHaveBeenCalledTimes(1);
  expect(cli).toHaveBeenCalledWith('/configured/eigenflux', [
    '--homedir', home, '--server', 'production', 'profile', 'show', '--format', 'json',
  ], expect.objectContaining({ logger }));
  expect(fetchMock.mock.calls.filter(([, o]) => o.method === 'POST')).toHaveLength(1);
});

test('401 during ACK refreshes authentication without resubmitting delivery, including after restart', async () => {
  fetchMock.mockImplementation(async (_url, options) =>
    options.headers.Authorization === 'Bearer expired-token' ? response(401) : response(200, { acknowledged: 1 }));
  const deliver = jest.fn().mockResolvedValue(true);
  const file = path.join(home, 'queue.json');
  const event = { type: 'notification_push', data: { notifications: [notification] } };
  await new OrderNotifications(request, deliver, file).handle(event);
  await new OrderNotifications(request, deliver, file).handle(event);
  expect(deliver).toHaveBeenCalledTimes(1);
  expect(cli).toHaveBeenCalledTimes(1);
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

test('failed CLI authentication leaves an undelivered order pending and bounds retries', async () => {
  cli.mockResolvedValue({ kind: 'auth_required', stderr: 'reauthentication required' });
  fetchMock.mockResolvedValue(response(401));
  const deliver = jest.fn();
  await expect(new OrderNotifications(request, deliver).reconcile()).rejects.toThrow(/authentication/i);
  expect(deliver).not.toHaveBeenCalled();
  expect(cli).toHaveBeenCalledTimes(1);
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

test('a second 401 stops rather than looping', async () => {
  fetchMock.mockResolvedValue(response(401));
  await expect(request('/notifications/pending?limit=50')).rejects.toThrow('HTTP 401');
  expect(fetchMock).toHaveBeenCalledTimes(2);
  expect(cli).toHaveBeenCalledTimes(1);
});

test('CLI identity rotation cannot ACK an old account notification under the new identity', async () => {
  cli.mockImplementation(async () => {
    credentials('new-account-token', '43');
    return { kind: 'success', data: {} };
  });
  fetchMock.mockResolvedValue(response(401));
  await expect(request('/notifications/ack', { notifications: [notification] })).rejects.toThrow('identity changed');
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

test('non-authentication HTTP failures do not invoke credential recovery', async () => {
  fetchMock.mockResolvedValue(response(503));
  await expect(request('/notifications/pending?limit=50')).rejects.toThrow('HTTP 503');
  expect(cli).not.toHaveBeenCalled();
});

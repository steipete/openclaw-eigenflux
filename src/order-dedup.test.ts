import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { OrderNotifications } from './order-notifications';
import { EigenFluxNotifier } from './notifier';
import { Logger } from './logger';

const n = { notification_id: '358858910188175361', source_type: 'commission_order', payload: { order_id: '358858910112677888' } };
const event = { type: 'notification_push', data: { notifications: [n] } };
const logger = new Logger({ info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() });

function transport(run: jest.Mock, waitForRun: jest.Mock, fallback = jest.fn()) {
  const notifier = new EigenFluxNotifier({ runtime: { subagent: { run, waitForRun }, system: { runCommandWithTimeout: fallback } }, config: {} } as any, logger,
    { sessionKey: 'main', agentId: 'main', openclawCliBin: 'openclaw' });
  jest.spyOn(notifier as any, 'resolveRoute').mockResolvedValue({ route: { agentId: 'main', sessionKey: 'main', replyChannel: 'feishu', replyTo: 'test' } });
  jest.spyOn(notifier as any, 'seedOneShotDeliveryContext').mockResolvedValue(undefined);
  return (item: any, receipt: any, checkpoint: any) => notifier.deliverOrder('order payload', { key: 'server:agent:' + item.notification_id, receipt, checkpoint });
}

test('wait failure after host acceptance resumes the same run after restart without fallback or duplicate', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'order-dedup-'));
  try {
    const run = jest.fn().mockResolvedValue({ runId: 'accepted-run' });
    const wait = jest.fn().mockRejectedValueOnce(new Error('agent.wait unavailable during gateway startup')).mockResolvedValue({ status: 'ok' });
    const fallback = jest.fn();
    const request = jest.fn().mockResolvedValue({ notifications: [], has_more: false });
    const file = join(dir, 'queue.json');
    const before = new OrderNotifications(request, transport(run, wait, fallback), file);
    await expect(before.handle(event)).rejects.toThrow();
    expect(request).not.toHaveBeenCalled();
    const after = new OrderNotifications(request, transport(run, wait, fallback), file);
    await after.handle(event);
    expect(run).toHaveBeenCalledTimes(1);
    expect(wait.mock.calls.map(([args]) => args.runId)).toEqual(['accepted-run', 'accepted-run']);
    expect(fallback).not.toHaveBeenCalled();
    const again = new OrderNotifications(request, transport(run, wait, fallback), file);
    await again.handle(event);
    expect(run).toHaveBeenCalledTimes(1);
    expect(wait).toHaveBeenCalledTimes(2);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('ambiguous submit is held across restart, never auto-replayed or ACKed', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'order-dedup-'));
  try {
    const run = jest.fn().mockRejectedValue(new Error('connection closed after submission'));
    const wait = jest.fn();
    const request = jest.fn();
    const file = join(dir, 'queue.json');
    await expect(new OrderNotifications(request, transport(run, wait), file).handle(event)).rejects.toThrow();
    await expect(new OrderNotifications(request, transport(run, wait), file).handle(event)).rejects.toThrow();
    expect(run).toHaveBeenCalledTimes(1);
    expect(request).not.toHaveBeenCalled();
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('ACK failure after delivery survives restart without rerunning the Agent', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'order-dedup-'));
  try {
    const run = jest.fn().mockResolvedValue({ runId: 'done' });
    const wait = jest.fn().mockResolvedValue({ status: 'ok' });
    const request = jest.fn().mockRejectedValueOnce(new Error('ACK unavailable')).mockResolvedValue({});
    const file = join(dir, 'queue.json');
    await expect(new OrderNotifications(request, transport(run, wait), file).handle(event)).rejects.toThrow('ACK unavailable');
    await new OrderNotifications(request, transport(run, wait), file).handle(event);
    expect(run).toHaveBeenCalledTimes(1);
    expect(wait).toHaveBeenCalledTimes(1);
    expect(request).toHaveBeenCalledTimes(2);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('timeouts keep querying one run while unrelated notifications continue', async () => {
  const run = jest.fn().mockResolvedValueOnce({ runId: 'slow-run' }).mockResolvedValueOnce({ runId: 'other-run' });
  const wait = jest.fn().mockImplementation(async ({ runId }) => ({ status: runId === 'slow-run' ? 'timeout' : 'ok' }));
  const request = jest.fn().mockResolvedValue({ notifications: [], has_more: false });
  const handler = new OrderNotifications(request, transport(run, wait));
  const other = { ...n, notification_id: '358858954874290177' };
  await expect(handler.handle({ type: 'notification_push', data: { notifications: [n, other] } })).rejects.toThrow();
  await expect(handler.handle(event)).rejects.toThrow();
  expect(run).toHaveBeenCalledTimes(2);
  expect(request).toHaveBeenCalledWith('/notifications/ack', { notifications: [{ notification_id: other.notification_id, source_type: 'commission_order' }] });
  expect(request).not.toHaveBeenCalledWith('/notifications/ack', { notifications: [{ notification_id: n.notification_id, source_type: 'commission_order' }] });
});

test('legacy records without receipts are held rather than blindly replayed', async () => {
  const fs = await import('node:fs');
  const dir = mkdtempSync(join(tmpdir(), 'order-dedup-'));
  try {
    const file = join(dir, 'queue.json');
    fs.writeFileSync(file, JSON.stringify([n]));
    const run = jest.fn();
    const wait = jest.fn();
    const request = jest.fn();
    await expect(new OrderNotifications(request, transport(run, wait), file).handle(event)).rejects.toThrow();
    expect(run).not.toHaveBeenCalled();
    expect(request).not.toHaveBeenCalled();
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

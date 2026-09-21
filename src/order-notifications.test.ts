import { OrderNotifications } from './order-notifications';

const notification = { notification_id: '358858910188175361', source_type: 'commission_order', payload: { order_id: '358858910112677888', recipient_role: 'seller', to_state: 'pending_payment' } };
const event = { type: 'notification_push', data: { notifications: [notification] } };
function setup() {
  const deliver = jest.fn().mockResolvedValue(true);
  const request = jest.fn().mockResolvedValue({ notifications: [], has_more: false });
  const handler = new OrderNotifications(request, deliver);
  return { handler, request, deliver };
}
test('delivers an order before acknowledging it; duplicate pushes do not deliver twice', async () => {
  const { handler, request, deliver } = setup();
  await Promise.all([handler.handle(event), handler.handle(event)]);
  expect(deliver).toHaveBeenCalledTimes(1);
  expect(deliver).toHaveBeenCalledWith(notification, expect.any(Object), expect.any(Function));
  expect(request).toHaveBeenCalledWith('/notifications/ack', { notifications: [{ notification_id: notification.notification_id, source_type: 'commission_order' }] });
  expect(deliver.mock.invocationCallOrder[0]).toBeLessThan(request.mock.invocationCallOrder[0]);
});
test('failed delivery remains retryable and is never acknowledged', async () => {
  const { handler, request, deliver } = setup();
  deliver.mockResolvedValueOnce(false);
  await expect(handler.handle(event)).rejects.toThrow('delivery');
  expect(request).not.toHaveBeenCalled();
  await handler.reconcile();
  expect(deliver).toHaveBeenCalledTimes(2);
});
test('ACK failure retries without delivering to the agent again', async () => {
  const { handler, request, deliver } = setup();
  request.mockRejectedValueOnce(new Error('ack unavailable'));
  await expect(handler.handle(event)).rejects.toThrow('ack unavailable');
  await handler.reconcile();
  expect(deliver).toHaveBeenCalledTimes(1);
  expect(request.mock.calls.filter(([url]) => url === '/notifications/ack')).toHaveLength(2);
});
test('accepts the newer CLI envelope and recovers all pending pages', async () => {
  const { handler, request, deliver } = setup();
  await handler.handle({ type: 'commission_order_notification', notification });
  request.mockResolvedValueOnce({ notifications: [], has_more: true, next_cursor: 'page2' })
    .mockResolvedValueOnce({ notifications: [{ ...notification, notification_id: '358858954874290177' }], has_more: false });
  await handler.reconcile();
  expect(request).toHaveBeenCalledWith('/notifications/pending?limit=50&cursor=page2');
  expect(deliver).toHaveBeenCalledTimes(2);
});
test('ignores PM events and never acknowledges other notification sources', async () => {
  const { handler, request, deliver } = setup();
  await handler.handle({ type: 'pm_push', data: { messages: [{ content: 'hi' }] } });
  await handler.handle({ type: 'notification_push', data: { notifications: [{ ...notification, source_type: 'system' }] } });
  expect(deliver).not.toHaveBeenCalled();
  expect(request).not.toHaveBeenCalled();
});
test('idle runtime never polls; stream delivery remains active after restart', async () => {
  jest.useFakeTimers();
  const { handler, request, deliver } = setup();
  try {
    handler.start();
    await jest.advanceTimersByTimeAsync(300000);
    expect(request).not.toHaveBeenCalled();
    expect(deliver).not.toHaveBeenCalled();
    expect(jest.getTimerCount()).toBe(0);
    handler.stop();
    handler.start();
    await handler.handle(event);
    expect(deliver).toHaveBeenCalledTimes(1);
    expect(request).toHaveBeenCalledTimes(1);
    expect(request.mock.calls[0][0]).toBe('/notifications/ack');
    handler.stop();
    await jest.advanceTimersByTimeAsync(300000);
    expect(request).toHaveBeenCalledTimes(1);
  } finally { handler.stop(); jest.useRealTimers(); }
});
test('rejects a stalled recovery cursor without looping forever', async () => {
  const { handler, request } = setup();
  request.mockResolvedValue({ notifications: [], has_more: true, next_cursor: 'same' });
  await expect(handler.reconcile()).rejects.toThrow('cursor did not advance');
  expect(request).toHaveBeenCalledTimes(2);
});
test('replays failed delivery from the local queue after a process restart', async () => {
  const fs = await import('node:fs');
  const os = await import('node:os');
  const path = await import('node:path');
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'order-notifications-'));
  const file = path.join(directory, 'pending.json');
  const request = jest.fn().mockResolvedValue({ notifications: [], has_more: false });
  try {
    const before = new OrderNotifications(request, async () => false, file);
    await expect(before.handle(event)).rejects.toThrow('delivery');
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).entries[0].notification).toEqual(notification);
    const deliver = jest.fn().mockResolvedValue(true);
    const after = new OrderNotifications(request, deliver, file);
    await after.reconcile();
    expect(deliver).toHaveBeenCalledWith(notification, expect.any(Object), expect.any(Function));
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).entries).toEqual([]);
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
});

import * as fs from 'node:fs';
import * as path from 'node:path';

/** Order delivery is serialized across stream wake-ups and explicit recovery.
 * Pending records stay on the server until the notifier accepts them.
 */
export interface OrderNotification {
  notification_id: string;
  source_type: string;
  payload: Record<string, unknown>;
  [key: string]: unknown;
}
export type OrderDeliveryReceipt = { status: 'queued' | 'submitting' | 'running' | 'delivered' | 'failed'; runId?: string };
type Entry = { notification: OrderNotification; receipt: OrderDeliveryReceipt };
type Request = (path: string, body?: unknown) => Promise<any>;

export class OrderNotifications {
  private tail: Promise<void> = Promise.resolve();
  private pending = new Map<string, Entry>();
  private completed = new Map<string, number>();
  private recovery?: Promise<void>;
  private stopped = false;

  start(): void {
    this.stopped = false;
  }

  stop(): void {
    this.stopped = true;
  }

  constructor(private request: Request, private deliver: (notification: OrderNotification, receipt: OrderDeliveryReceipt, checkpoint: (receipt: OrderDeliveryReceipt) => void) => Promise<boolean>, private storageFile?: string) {
    if (storageFile && fs.existsSync(storageFile)) {
      const saved = JSON.parse(fs.readFileSync(storageFile, 'utf8'));
      if (Array.isArray(saved)) {
        // Legacy queues cannot tell whether the host accepted a run. Hold them
        // for reconciliation instead of replaying potentially delivered work.
        for (const record of saved) {
          this.add(record);
          const item = this.pending.get(record.notification_id);
          if (item) item.receipt = { status: 'submitting' };
        }
      } else {
        if (saved.version !== 1 || !Array.isArray(saved.entries) || !Array.isArray(saved.completed)) throw new Error('Invalid Order delivery queue');
        for (const [id, at] of saved.completed) {
          if (typeof id !== 'string' || typeof at !== 'number') throw new Error('Invalid Order delivery receipt');
          this.completed.set(id, at);
        }
        for (const entry of saved.entries) {
          if (!entry.receipt || !['queued', 'submitting', 'running', 'delivered', 'failed'].includes(entry.receipt.status) ||
              (entry.receipt.status === 'running' && typeof entry.receipt.runId !== 'string')) throw new Error('Invalid Order delivery receipt');
          this.add(entry.notification);
          const item = this.pending.get(entry.notification.notification_id);
          if (item) item.receipt = entry.receipt;
        }
      }
    }
  }

  private persist(): void {
    if (!this.storageFile) return;
    fs.mkdirSync(path.dirname(this.storageFile), { recursive: true, mode: 0o700 });
    const temporary = this.storageFile + '.tmp';
    fs.writeFileSync(temporary, JSON.stringify({ version: 1, entries: [...this.pending.values()], completed: [...this.completed] }), { mode: 0o600 });
    fs.renameSync(temporary, this.storageFile);
  }

  private serialize(work: () => Promise<void>): Promise<void> {
    const result = this.tail.then(work);
    this.tail = result.catch(() => {});
    return result;
  }

  private add(value: unknown): void {
    if (!value || typeof value !== 'object') return;
    const n = value as OrderNotification;
    if (n.source_type !== 'commission_order') return;
    if (typeof n.notification_id !== 'string' || !/^[1-9]\d*$/.test(n.notification_id) || !n.payload || typeof n.payload !== 'object') {
      throw new Error('Invalid Order notification');
    }
    if (!this.completed.has(n.notification_id) && !this.pending.has(n.notification_id)) {
      this.pending.set(n.notification_id, { notification: n, receipt: { status: 'queued' } });
    }
  }

  private async flush(): Promise<void> {
    this.persist();
    let firstError: unknown;
    for (const [id, item] of this.pending) {
      if (this.stopped) return;
      try {
        if (item.receipt.status !== 'delivered') {
          if (!await this.deliver(item.notification, item.receipt, (receipt) => {
            item.receipt = receipt;
            this.persist(); // Must succeed before submission or any further action.
          })) throw new Error(`Order notification delivery pending reconciliation: ${id}`);
          item.receipt = { ...item.receipt, status: 'delivered' };
          this.persist(); // ACK failures/restarts must never rerun a completed Agent.
        }
        await this.request('/notifications/ack', { notifications: [{ notification_id: id, source_type: 'commission_order' }] });
        this.pending.delete(id);
        this.completed.set(id, Date.now());
        this.persist();
      } catch (error) { firstError ??= error; }
    }
    if (firstError) throw firstError;
  }

  handle(event: { type: string; data?: Record<string, unknown>; notification?: unknown }): Promise<void> {
    return this.serialize(async () => {
      if (event.type === 'commission_order_notification') this.add(event.notification);
      else if (event.type === 'notification_push') {
        for (const n of (Array.isArray(event.data?.notifications) ? event.data.notifications : [])) this.add(n);
      } else return;
      await this.flush();
      if (event.data?.has_more) await this.pull();
    });
  }

  reconcile(): Promise<void> {
    if (this.recovery) return this.recovery;
    this.recovery = this.serialize(async () => {
      let failure: unknown;
      try { await this.flush(); } catch (error) { failure = error; }
      await this.pull();
      if (failure) throw failure;
    })
      .finally(() => { this.recovery = undefined; });
    return this.recovery;
  }

  private async pull(): Promise<void> {
    let cursor = '';
    const seen = new Set<string>();
    let failure: unknown;
    do {
      if (this.stopped) return;
      const page = await this.request('/notifications/pending?limit=50' + (cursor ? '&cursor=' + encodeURIComponent(cursor) : ''));
      if (!page || !Array.isArray(page.notifications)) throw new Error('Invalid pending notifications response');
      for (const n of page.notifications) this.add(n);
      try { await this.flush(); } catch (error) { failure ??= error; }
      if (!page.has_more) {
        if (failure) throw failure;
        return;
      }
      if (typeof page.next_cursor !== 'string' || !page.next_cursor || seen.has(page.next_cursor)) throw new Error('Notification cursor did not advance');
      cursor = page.next_cursor;
      seen.add(cursor);
    } while (true);
  }
}

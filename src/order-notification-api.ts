import * as fs from 'node:fs';
import * as path from 'node:path';
import type { Logger } from './logger';
import { execEigenflux } from './cli-executor';

export interface OrderNotificationApiConfig {
  eigenfluxBin: string;
  eigenfluxHome: string;
  serverName: string;
  endpoint: string;
  agentID: string;
  logger: Logger;
}

/** HTTP transport for hosts; credentials remain owned by the CLI. */
export function createOrderNotificationRequest(config: OrderNotificationApiConfig) {
  const credentialsPath = path.join(config.eigenfluxHome, 'servers', config.serverName, 'agent-v2-credentials.json');
  const send = async (resource: string, body?: unknown): Promise<Response> => {
    // Re-read after CLI recovery and before every request, including ACKs.
    if (!fs.existsSync(credentialsPath)) throw new Error('Agent V2 credentials required for Order notifications');
    const credentials = JSON.parse(fs.readFileSync(credentialsPath, 'utf8'));
    if (String(credentials.agent_id ?? '') !== config.agentID) throw new Error('Order notification identity changed; restart the plugin');
    if (typeof credentials.access_token !== 'string' || !credentials.access_token) throw new Error('Missing Agent V2 access token');
    return fetch(config.endpoint.replace(/\/$/, '') + '/api/v2' + resource, {
      method: body === undefined ? 'GET' : 'POST',
      headers: { Authorization: `Bearer ${credentials.access_token}`, 'Content-Type': 'application/json' },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(15000),
      redirect: 'error',
    });
  };
  return async (resource: string, body?: unknown): Promise<unknown> => {
    let response = await send(resource, body);
    if (response.status === 401) {
      // The installed CLI has no notifications command yet. Its read-only
      // profile command uses the shared authenticated client, including expiry
      // refresh, forced-401 recovery, signing, and cross-process refresh locks.
      // Keep all credential writes and authentication classification in the CLI.
      await response.body?.cancel();
      const result = await execEigenflux(config.eigenfluxBin, [
        '--homedir', config.eigenfluxHome, '--server', config.serverName,
        'profile', 'show', '--format', 'json',
      ], { logger: config.logger, timeout: 60000 });
      if (result.kind === 'auth_required') throw new Error('Order notification authentication required');
      if (result.kind === 'not_installed') throw new Error('Order notification authentication recovery requires the EigenFlux CLI');
      if (result.kind === 'error') throw new Error(`Order notification authentication recovery failed: ${result.error.message}`);
      response = await send(resource, body); // Identity guard runs again; retry only once.
    }
    if (!response.ok) throw new Error(`Order notification API HTTP ${response.status}`);
    const envelope = await response.json() as { code?: number; data: unknown };
    if (envelope.code !== undefined && envelope.code !== 0) throw new Error(`Order notification API code ${envelope.code}`);
    return envelope.data;
  };
}

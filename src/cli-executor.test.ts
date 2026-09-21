import { execEigenflux } from './cli-executor';

describe('execEigenflux', () => {
  test('per-child model overrides preserve parent metadata without mutating the Gateway', async () => {
    const savedModel = process.env.EIGENFLUX_MODEL;
    const savedChannel = process.env.EIGENFLUX_CHANNEL;
    process.env.EIGENFLUX_MODEL = 'unrelated-model';
    process.env.EIGENFLUX_CHANNEL = 'openclaw';
    try {
      const script = 'console.log(JSON.stringify({model:process.env.EIGENFLUX_MODEL,channel:process.env.EIGENFLUX_CHANNEL}))';
      expect(await execEigenflux(process.execPath, ['-e', script]))
        .toEqual({ kind: 'success', data: { channel: 'openclaw' } });
      expect(await execEigenflux(process.execPath, ['-e', script], { env: { EIGENFLUX_CHANNEL: 'scoped-channel' } }))
        .toEqual({ kind: 'success', data: { channel: 'scoped-channel' } });
      expect(await execEigenflux(process.execPath, ['-e', script], { env: { EIGENFLUX_MODEL: 'owned-model' } }))
        .toEqual({ kind: 'success', data: { model: 'owned-model', channel: 'openclaw' } });
      expect(await execEigenflux(process.execPath, ['-e', script], { env: { EIGENFLUX_MODEL: undefined } }))
        .toEqual({ kind: 'success', data: { channel: 'openclaw' } });
      expect(process.env.EIGENFLUX_MODEL).toBe('unrelated-model');
    } finally {
      if (savedModel === undefined) delete process.env.EIGENFLUX_MODEL; else process.env.EIGENFLUX_MODEL = savedModel;
      if (savedChannel === undefined) delete process.env.EIGENFLUX_CHANNEL; else process.env.EIGENFLUX_CHANNEL = savedChannel;
    }
  });
  test('returns not_installed when the binary cannot be spawned (ENOENT)', async () => {
    const result = await execEigenflux(
      '/tmp/definitely-not-a-real-binary-eigenflux-xyz',
      ['server', 'list', '--format', 'json']
    );
    expect(result.kind).toBe('not_installed');
    if (result.kind === 'not_installed') {
      expect(result.bin).toBe('/tmp/definitely-not-a-real-binary-eigenflux-xyz');
    }
  });
});

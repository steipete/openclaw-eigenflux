import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  buildAuthRequiredPromptTemplate,
  buildFeedPayloadPromptTemplate,
  buildHeartbeatExecutionPromptTemplate,
  buildPmStreamEventPromptTemplate,
  type EigenFluxPromptServerContext,
} from './agent-prompt-templates';

describe('agent prompt templates', () => {
  const context: EigenFluxPromptServerContext = {
    serverName: 'alpha',
    eigenfluxHome: '/tmp/.eigenflux',
  };

  test('routes first connection and account recovery to their Skills with server context', () => {
    const prompt = buildAuthRequiredPromptTemplate({ context });

    expect(prompt).toContain('[EIGENFLUX_AUTH_REQUIRED]');
    expect(prompt).toContain('homedir=/tmp/.eigenflux');
    expect(prompt).toContain('server=alpha');
    expect(prompt).toContain('EigenFlux authentication is required.');
    expect(prompt).toContain('first connection, load the installed ef-onboarding Skill');
    expect(prompt).toContain('existing account, load the installed ef-profile Skill');
    expect(prompt).toContain('https://github.com/phronesis-io/eigenflux/blob/main/skills/install.md');
    expect(prompt).toContain('Keep the homedir and server above');
    expect(prompt).not.toContain('auth login');
  });

  test('includes stderr detail in auth-required prompt when provided', () => {
    const prompt = buildAuthRequiredPromptTemplate({
      context,
      stderr: 'token expired at 2026-01-01',
    });

    expect(prompt).toContain('detail=token expired at 2026-01-01');
  });

  test('builds feed payload prompt with server context and skill reference', () => {
    const prompt = buildFeedPayloadPromptTemplate(
      {
        code: 0,
        msg: 'ok',
        data: {
          items: [],
          has_more: false,
          notifications: [],
        },
      },
      context
    );

    expect(prompt).toContain('[EIGENFLUX_FEED_PAYLOAD]');
    expect(prompt).toContain('homedir=/tmp/.eigenflux');
    expect(prompt).toContain('server=alpha');
    expect(prompt).toContain('ef-broadcast skill');
  });

  test('builds an executable heartbeat around the plugin-owned feed payload', () => {
    const prompt = buildHeartbeatExecutionPromptTemplate(
      'EIGENFLUX HEARTBEAT PLAN\nExecute in this exact order: Commands → Feed → Attention.',
      { code: 0, msg: 'ok', data: { items: [], has_more: false, notifications: [] } },
      context
    );

    expect(prompt).toContain('[EIGENFLUX_HEARTBEAT]');
    expect(prompt).toContain('do not run another feed poll');
    expect(prompt).toContain('Attention');
    expect(prompt).toContain('[EIGENFLUX_FEED_PAYLOAD]');
  });

  test('missing central contract keeps the payload and current Skill reference without inventing rules', () => {
    const root = mkdtempSync(join(tmpdir(), 'eigenflux-claw-missing-skills-'));
    const previous = process.env.EIGENFLUX_SKILLS_DIR;
    process.env.EIGENFLUX_SKILLS_DIR = root;
    try {
      const prompt = buildFeedPayloadPromptTemplate(
        { code: 0, msg: 'ok', data: { items: [{ item_id: '17', broadcast_type: 'info', updated_at: 1 }], has_more: false, notifications: [] } },
        context
      );
      expect(prompt).toContain('ef-broadcast skill');
      expect(prompt).toContain('"item_id": "17"');
      expect(prompt).not.toContain('OUTPUT CONTRACT');
      expect(prompt).not.toContain('Submit feedback');
    } finally {
      if (previous === undefined) delete process.env.EIGENFLUX_SKILLS_DIR;
      else process.env.EIGENFLUX_SKILLS_DIR = previous;
      rmSync(root, { recursive: true, force: true });
    }
  });

  test('preserves a baseline plan without appending a complete-operation stage list', () => {
    const plan = 'CENTRAL PLAN: consume baseline Feed; skip unavailable stages.';
    const prompt = buildHeartbeatExecutionPromptTemplate(
      plan,
      { code: 0, msg: 'ok', data: { items: [], has_more: false, notifications: [], output_contract: '' } },
      context
    );
    expect(prompt).toContain(plan);
    expect(prompt).toContain('do not run another feed poll');
    expect(prompt).not.toContain('Complete Attention');
    expect(prompt).not.toContain('Run Commands first');
  });

  test('reads the fallback contract from the CLI-synced host Skills directory', () => {
    const root = mkdtempSync(join(tmpdir(), 'eigenflux-claw-skills-'));
    const contractDir = join(root, 'ef-broadcast', 'references');
    mkdirSync(contractDir, { recursive: true });
    writeFileSync(join(contractDir, 'contract.md'), 'SYNCED HOST CONTRACT');
    const previous = process.env.EIGENFLUX_SKILLS_DIR;
    process.env.EIGENFLUX_SKILLS_DIR = root;

    try {
      const prompt = buildFeedPayloadPromptTemplate(
        { code: 0, msg: 'ok', data: { items: [], has_more: false, notifications: [] } },
        context
      );
      expect(prompt).toContain('SYNCED HOST CONTRACT');
      writeFileSync(join(contractDir, 'contract.md'), 'UPDATED CENTRAL CONTRACT');
      const updated = buildFeedPayloadPromptTemplate(
        { code: 0, msg: 'ok', data: { items: [], has_more: false, notifications: [] } },
        context
      );
      expect(updated).toContain('UPDATED CENTRAL CONTRACT');
      expect(updated).not.toContain('SYNCED HOST CONTRACT');
    } finally {
      if (previous === undefined) delete process.env.EIGENFLUX_SKILLS_DIR;
      else process.env.EIGENFLUX_SKILLS_DIR = previous;
      rmSync(root, { recursive: true, force: true });
    }
  });

  test('prefers the backend-delivered output_contract over the synced host copy', () => {
    const prompt = buildFeedPayloadPromptTemplate(
      {
        code: 0,
        msg: 'ok',
        data: {
          items: [],
          has_more: false,
          notifications: [],
          output_contract: 'SERVER CONTRACT vTest — follow these rules. 📡 Powered by EigenFlux',
        },
      },
      context
    );

    // The delivered copy leads the prompt; the synced host copy stays out
    // ("OUTPUT CONTRACT" heads contract.md / fallback, not the server copy).
    expect(prompt).toContain('SERVER CONTRACT vTest');
    expect(prompt).not.toContain('OUTPUT CONTRACT');
    expect(prompt.indexOf('SERVER CONTRACT vTest')).toBeLessThan(prompt.indexOf('Payload:'));
    // output_contract never leaks into the echoed payload JSON.
    const payloadBlock = prompt.slice(prompt.indexOf('Payload:'));
    expect(payloadBlock).not.toContain('output_contract');
  });

  test('explicit empty output_contract injects no rules and no fallback', () => {
    // A present-but-empty field is the server saying "this payload needs no
    // output rules" (the common empty-poll case) — falling back would reinstate
    // the very rules the server withheld.
    const prompt = buildFeedPayloadPromptTemplate(
      {
        code: 0,
        msg: 'ok',
        data: { items: [], has_more: false, notifications: [], output_contract: '' },
      },
      context
    );

    expect(prompt).not.toContain('OUTPUT CONTRACT');
    expect(prompt).toContain('Payload:');
    const payloadBlock = prompt.slice(prompt.indexOf('Payload:'));
    expect(payloadBlock).not.toContain('output_contract');
  });

  test('builds pm stream event prompt with server context and skill reference', () => {
    const prompt = buildPmStreamEventPromptTemplate(
      {
        type: 'pm_push',
        data: {
          messages: [
            {
              msg_id: '1',
              conv_id: '1',
              sender_id: '2',
              content: 'hi',
              created_at: 1760000000000,
            },
          ],
        },
      },
      context
    );

    expect(prompt).toContain('[EIGENFLUX_MSG_PAYLOAD]');
    expect(prompt).toContain('homedir=/tmp/.eigenflux');
    expect(prompt).toContain('server=alpha');
    expect(prompt).toContain('private message(s)');
    expect(prompt).toContain('ef-communication skill to process them');
  });

  test('summarizes incoming friend requests', () => {
    const prompt = buildPmStreamEventPromptTemplate(
      {
        type: 'pm_push',
        data: {
          messages: [],
          friend_requests: [
            {
              request_id: '9',
              from_uid: '2',
              from_name: 'Monster',
              greeting: 'hi',
              created_at: 1760000000000,
            },
          ],
        },
      },
      context
    );

    expect(prompt).toContain('[EIGENFLUX_MSG_PAYLOAD]');
    expect(prompt).toContain('incoming friend request(s)');
    expect(prompt).toContain('ef-communication skill to process them');
  });

  test('summarizes friend_accepted events', () => {
    const prompt = buildPmStreamEventPromptTemplate(
      {
        type: 'friend_accepted',
        data: { friend_uid: '2' },
      },
      context
    );

    expect(prompt).toContain('[EIGENFLUX_MSG_PAYLOAD]');
    expect(prompt).toContain('friend request response(s)');
  });
});

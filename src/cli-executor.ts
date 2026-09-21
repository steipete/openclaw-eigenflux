/**
 * Generic helper to run `eigenflux` CLI commands as one-shot subprocesses.
 */

import { execFile } from 'child_process';
import { Logger } from './logger';

const EXIT_AUTH_REQUIRED = 4;
const DEFAULT_TIMEOUT_MS = 30_000;

export type CliResult<T> =
  | { kind: 'success'; data: T }
  | { kind: 'auth_required'; stderr: string }
  | { kind: 'not_installed'; bin: string }
  | { kind: 'error'; error: Error; exitCode: number | null; stderr: string };

export interface ExecOptions {
  timeout?: number;
  cwd?: string;
  logger?: Logger;
  /** Per-child metadata overrides; never mutate the Gateway environment. */
  env?: NodeJS.ProcessEnv;
  /**
   * When false, stdout is returned as a raw string instead of being parsed as
   * JSON. Useful for CLI commands whose stdout is a human-readable status line
   * (e.g. `eigenflux config set`). Defaults to true.
   */
  parseJson?: boolean;
}

/** The Gateway serves multiple Agents; model identity must be scoped per call. */
export function cliEnvironment(overrides?: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  return { ...process.env, EIGENFLUX_MODEL: undefined, ...overrides };
}

export function execEigenflux<T>(
  bin: string,
  args: string[],
  options?: ExecOptions
): Promise<CliResult<T>> {
  const timeout = options?.timeout ?? DEFAULT_TIMEOUT_MS;
  const logger = options?.logger;

  return new Promise((resolve) => {
    logger?.debug(`execEigenflux: ${bin} ${args.join(' ')}`);

    execFile(
      bin,
      args,
      {
        timeout,
        maxBuffer: 10 * 1024 * 1024,
        encoding: 'utf-8',
        env: cliEnvironment(options?.env),
        ...(options?.cwd ? { cwd: options.cwd } : {}),
      },
      (error, stdout, stderr) => {
        if (error) {
          const exitCode = (error as NodeJS.ErrnoException & { code?: number | string }).code;
          if (exitCode === 'ENOENT') {
            logger?.warn(`execEigenflux: binary not found: ${bin}`);
            resolve({ kind: 'not_installed', bin });
            return;
          }
          const numericExit =
            typeof exitCode === 'number'
              ? exitCode
              : error.killed
                ? null
                : (error as any).status ?? null;

          if (numericExit === EXIT_AUTH_REQUIRED) {
            logger?.warn(`execEigenflux auth required: ${stderr.trim()}`);
            resolve({ kind: 'auth_required', stderr: stderr.trim() });
            return;
          }

          logger?.error(`execEigenflux failed (exit=${numericExit}): ${stderr.trim() || error.message}`);
          resolve({
            kind: 'error',
            error: new Error(stderr.trim() || error.message),
            exitCode: numericExit,
            stderr: stderr.trim(),
          });
          return;
        }

        const trimmed = stdout.trim();
        if (!trimmed) {
          resolve({
            kind: 'success',
            data: undefined as unknown as T,
          });
          return;
        }

        if (options?.parseJson === false) {
          resolve({ kind: 'success', data: trimmed as unknown as T });
          return;
        }

        try {
          const data = JSON.parse(trimmed) as T;
          resolve({ kind: 'success', data });
        } catch (parseError) {
          logger?.error(`execEigenflux JSON parse error: ${(parseError as Error).message}`);
          resolve({
            kind: 'error',
            error: new Error(`Failed to parse CLI output: ${(parseError as Error).message}`),
            exitCode: 0,
            stderr: '',
          });
        }
      }
    );
  });
}

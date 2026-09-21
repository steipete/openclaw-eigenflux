# AGENTS.md

Read [Skill maintenance instructions](skills/AGENTS.md) before changing Skills integration or plugin prompts.

This repository is the EigenFlux OpenClaw plugin. The repository root contains `openclaw.plugin.json`, `package.json`, and the plugin entry points.

## Responsibility boundary

Keep business decisions in the central EigenFlux CLI and dynamically synchronized Skills. Let the CLI own authentication and capability classification, onboarding behavior, due-time decisions, action selection, output contracts, and persistent business state. Keep plugin prompts limited to host context, dynamic instruction references, and delivery facts. Do not copy business rules into prompts, tool descriptions, inline fallbacks, or timers. Migrate a business workflow only after its central CLI entry point and output contract are available and tested.

Keep OpenClaw service registration, process lifecycle, host context extraction, Skills snapshot refresh, session routing, lane isolation, delivery, and transport backpressure in this repository. Do not build a plugin-specific onboarding or permission state machine.

## Runtime integration

- Send trusted current-model metadata through the CLI as `X-Client-Model`; persist and read `model`, without `model_name` aliases or default-configuration inference.

- Discover configured servers through the CLI and preserve the stable Agent Home and explicit server on every operation.
- Request `heartbeat plan --format json` on each poll. Validate `schema_version`, `agent_prompt`, and `wake_on_empty`; pass the prompt through and use the returned delivery decision.
- Call `feed poll` once per cycle and attach its payload to the Agent turn. Preserve server `output_contract` text, use the current CLI-synced contract when the field is absent, and retain no embedded contract copy.
- Sync signed Skills through the CLI at startup. Each heartbeat plan refreshes compatible Skills; refresh the OpenClaw Skills snapshot after a valid plan.
- Drive `profile refresh-task --format agent` from the existing heartbeat, passing host memory paths and extracted session snippets. Deliver nonempty stdout through the normal Agent route; let the current Skills select visible output or NO_REPLY. Let the CLI decide eligibility, cadence, and follow-up work.
- Keep PM streaming and feedback flush adapters thin. Let the CLI own remote requests, queue validation, batching, and deduplication.
- Register `/eigenflux auth|profile|refresh|servers|feed|pm|here|version` as host command adapters.

## Validation

Run `pnpm build` and `pnpm test`. Cover central instruction pass-through, missing or invalid central output, quiet skips, and host lifecycle behavior without invoking live EigenFlux services.

## Maintenance

When releasing a plugin version, run `pnpm bump-version <version>` to synchronize package, manifest, and runtime versions. Pure central Skill changes do not require a plugin release.

The Claude Code plugin lives in https://github.com/phronesis-io/eigenflux-claude-plugin.

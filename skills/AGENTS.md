# Skill Maintenance

- Treat [EigenFlux Skills](https://github.com/phronesis-io/eigenflux/tree/main/skills) as the sole source of Skill business instructions.
- Change Skill business behavior in the Eigenflux repository and follow its [skills/AGENTS.md](https://github.com/phronesis-io/eigenflux/blob/main/skills/AGENTS.md).
- Use the CLI to dynamically sync signed, compatible Skills bundles. Use `~/.agents/skills` by default and honor an explicit sync target.
- Keep this directory for maintenance instructions only. Keep Skill copies out of this repository.
- Limit plugin prompts and tool descriptions to host context, CLI arguments, and delivery. Obtain business instructions from the current CLI plan, server `output_contract`, and currently synced Skills. Keep business rules out of inline fallbacks and plugin timers.
- Delegate eligibility, cadence, action decisions, and persistent business state to the CLI. Preserve host process management, context extraction, and delivery adapters. Validate the central replacement before removing a plugin workflow.
- Publish pure Skill changes through the central Skills release. Keep plugin versions unchanged and skip plugin publication for those changes.

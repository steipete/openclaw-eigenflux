# Changelog

## Unreleased

- Prefer asynchronous OpenClaw session reads for feed busy checks while retaining support for hosts with only the synchronous getter.
- Preserve task cancellation and busy detection on older OpenClaw hosts by using their legacy task reader only when the async task API is unavailable.

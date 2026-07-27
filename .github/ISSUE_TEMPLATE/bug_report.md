---
name: Bug report
about: Report a defect in the engine, launcher, or client
title: "[bug] "
labels: [bug]
---

## Summary

A clear and concise description of the bug.

## Affected component

- [ ] `adbe-protocol` (SBE schema / codecs)
- [ ] `adbe-core` (engine, handlers, dedup, snapshot, telemetry)
- [ ] `adbe-launcher` (cluster bootstrap)
- [ ] `adbe-client` (Edge SDK)
- [ ] `adbe-examples`
- [ ] build / CI / docs

## Environment (required)

- JDK version (`java -version`):
- Aeron version:
- Agrona version:
- OS and kernel (`uname -srm`):
- Commit / tag:
- Deployment: single-node / multi-node / in-process test

## Reproduction (required)

Minimal, deterministic steps to reproduce. A failing unit or integration test,
or an exact sequence of commands, is strongly preferred.

```
1.
2.
3.
```

## Expected behaviour

What you expected to happen.

## Actual behaviour

What actually happened. Include exact status codes, balances, and any
`DistinctErrorLog` output.

## Logs and evidence

Relevant logs, stack traces, or metric snapshots. For a suspected
non-determinism or double-apply issue, please include the command stream and
both nodes' snapshots if available.

## Notes

- If this is a suspected security vulnerability, do NOT file a public issue.
  Follow [SECURITY.md](../../SECURITY.md) instead.
- For a suspected performance regression, please attach JMH before/after numbers
  against `benchmark-baseline.txt`.

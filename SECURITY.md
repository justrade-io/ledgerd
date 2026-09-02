# Security Policy

## Supported versions

LEDGERD is pre-1.0 and under active development. Security fixes are applied to the
`main` branch only. There is no long-term-support branch yet.

## Reporting a vulnerability

Please report security vulnerabilities privately. Do not open a public issue for
a suspected vulnerability.

- Preferred: use GitHub's private vulnerability reporting via the repository's
  **Security > Report a vulnerability** tab.
- Alternatively, email the maintainer at **brian.pham.ptt@gmail.com** with the
  details below.

Please include:

- a description of the issue and its impact;
- steps to reproduce (a minimal test case is ideal);
- affected component (`protocol`, `core`, `launcher`,
  `write-client`, `read`, `examples`) and commit or version;
- your environment: JDK version, Aeron/Agrona version, and OS.

## What to expect

- Acknowledgement of your report within a few business days.
- An initial assessment and, where applicable, a coordinated fix and disclosure
  timeline.
- Credit for the report if you would like it, once a fix is available.

## Scope

The deterministic core executes commands that arrive through the replicated log;
authentication, authorization, and rate limiting are Edge concerns owned by other
bounded contexts (see [docs/decisions/0004-edge-client-context.md](docs/decisions/0004-edge-client-context.md))
and are out of scope for this repository.

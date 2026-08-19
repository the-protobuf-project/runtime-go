# Security Policy

## Scope

This repository publishes Go libraries under Apache-2.0. It is a set of
components, not a finished product or a hosted service. Downstream integrators
remain responsible for the security posture, certification, and regulatory
obligations of whatever they build with it; this document only describes how to
report a vulnerability here and what to expect in return.

## Reporting a vulnerability

**Do not open a public issue for a security report.** Public issues are the
wrong channel — they disclose the problem before a fix exists.

Use one of the following:

1. **GitHub Private Vulnerability Reporting** (preferred) — go to the
   [Security tab](https://github.com/the-protobuf-project/runtime-go/security)
   and choose *Report a vulnerability*. This opens a private advisory visible
   only to you and the maintainers.

2. **Email** — TODO(maintainer): add a dedicated security contact address.
   A private, monitored mailbox is needed here; the project's public discussion
   list must not be used, since anything sent there is world-readable and would
   disclose the report on arrival.

Please include, as far as you can determine them:

- affected module(s) and version or commit
- a description of the flaw and its impact
- reproduction steps or a proof of concept
- any suggested remediation

Reports in any language are welcome. If you would like credit in the resulting
advisory, say so and give the name or handle you would like used.

## What to expect

| Stage | Target |
| --- | --- |
| Acknowledgement of your report | TODO(maintainer): define |
| Initial assessment and severity triage | TODO(maintainer): define |
| Status update cadence while a fix is developed | TODO(maintainer): define |
| Fix released and advisory published | TODO(maintainer): define |

These targets are unset deliberately rather than filled with numbers the
project has not committed to. The maintainer must choose values that are
actually achievable for a single-maintainer project before this table is
published as a commitment.

## Disclosure process

1. You report privately through one of the channels above.
2. The report is acknowledged and triaged. If it is not a vulnerability, the
   reasoning is explained and the report is closed.
3. If it is, a fix is developed in private. You may be asked to help validate
   it.
4. A patched version is released for every supported module affected.
5. A GitHub Security Advisory is published, and a CVE is requested through
   GitHub's CNA where the issue warrants one.
6. Credit is given to the reporter unless anonymity is requested.

This project follows coordinated disclosure: please give the maintainer a
reasonable opportunity to ship a fix before disclosing publicly. If a report
goes unanswered past the acknowledgement target above, you are free to
disclose — silence is not a request for indefinite embargo.

## Supported versions

**No versions have been released yet.** This repository is a `go.work`
multi-module workspace with no root module; each module is versioned and tagged
independently as `<directory>/vX.Y.Z`, for example `ulid/v0.1.0`. There are
currently no tags, so nothing here is under a support commitment.

Once the first tags are published, this table will list each module's supported
line:

| Module | Latest release | Supported |
| --- | --- | --- |
| `agents` | unreleased | — |
| `blockchain` | unreleased | — |
| `cache` | unreleased | — |
| `database` | unreleased | — |
| `grpc` | unreleased | — |
| `interfaces` | unreleased | — |
| `network` | unreleased | — |
| `observability` | unreleased | — |
| `streams` | unreleased | — |
| `ulid` | unreleased | — |

The `database/examples` and `streams/examples` modules are runnable examples
(`package main`), not importable libraries, and are not covered by this policy.

### v0.x carries no compatibility guarantee

Every module here is at major version zero. Under
[Semantic Versioning](https://semver.org/#spec-item-4), **v0.x.y is explicitly
outside the compatibility guarantees that apply from v1.0.0 onward**: any
release may change or remove API without a major-version bump, and a minor
bump may break your build.

Practically, for a consumer pinning these modules:

- Pin exact versions. Do not rely on `^0.x` style ranges.
- Expect that upgrading a minor version may require code changes.
- Security fixes are published against the latest release of a module only.
  There are no long-term support branches at v0.x, and older v0.x lines do not
  receive backported patches.

If you need a stable API surface or a backport commitment, say so on the issue
tracker — that requires a v1.0.0 release, which is a deliberate decision the
project has not yet made.

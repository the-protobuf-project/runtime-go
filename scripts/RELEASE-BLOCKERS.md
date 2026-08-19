# Release blockers

Findings from auditing every `go.work` module for standalone consumability.

A tag alone does not make a module consumable. When someone runs
`go get github.com/the-protobuf-project/runtime-go/<mod>@v0.1.0`, Go reads that
module's own `go.mod` and **ignores its `replace` directives** — `replace` only
applies in the main module. So a require that resolves only inside the
workspace fails immediately for the consumer.

**Status: all 12 modules build, vet and test cleanly with `GOWORK=off`.** What
remains is the require-version problem, which `go build` cannot surface locally
because the `replace` directives cover it.

Modules safe to tag today: `agents`, `network`, `ulid`, `observability`.

## 1. Placeholder versions that do not exist on the proxy

`v0.0.0-00010101000000-000000000000` is what Go writes when a require is
satisfied purely by a workspace or `replace`. It is not a real version and
resolves nowhere.

| Module | Unresolvable requires |
| --- | --- |
| `blockchain` | `observability` |
| `cache` | `observability`, `ulid` |
| `database` | `cache`, `observability`, `ulid` |
| `grpc` | `agents`, `observability` |
| `interfaces` | `observability` |
| `streams` | `observability`, `ulid` |
| `database/examples` | `cache`, `observability`, `ulid` |
| `streams/examples` | `observability`, `ulid` |

## 2. Bare `v0.0.0` requires

Equally unresolvable — no `v0.0.0` tag exists in this repo.

| Module | Require |
| --- | --- |
| `blockchain` | `database v0.0.0` |
| `interfaces` | `database v0.0.0` |
| `database/examples` | `database v0.0.0` |
| `streams/examples` | `streams v0.0.0` |

## 3. Resolved

- **The telemetry SDK path.** Modules briefly required
  `the-protobuf-project/opentelemetry/opentelemetry-go`, which does not exist.
  The real module is `the-protobuf-project/telemetry/telemetry-go` — the
  `opentelementry` repo was renamed to `telemetry`. All requires now resolve.
- **Incomplete `go.sum` files.** `GOWORK=off go mod tidy` per module added the
  entries `go.work.sum` had been supplying. This was 9 of 13 modules.
- **`replace`-masked API skew in `observability`.** It required a published
  `telemetry` that predated the `Logger` contract, hidden by a local `replace`.
  The `runtime-go/telemetry` module is no longer developed in this repo; every
  module now pins the published `v0.0.0-20260818025400-e63524c03160`, which
  carries the full contract, and no `replace` masks it. `observability` is
  releasable as a result.

## Suggested order

1. Tag `agents`, `network`, `ulid`, `observability` at `v0.1.0`.
2. In `cache` and `streams`, drop the `observability`/`ulid` replaces and pin
   the `v0.1.0` versions just tagged. Tag both.
3. Work outward — `database` → `blockchain`/`interfaces`/`grpc` → the two
   `examples` modules — dropping each `replace` and pinning real versions as
   dependencies get tagged.
4. Consider whether `database/examples` and `streams/examples` need tags at all
   — both are `package main` and neither is importable as a library.

# Release blockers

Findings from auditing every `go.work` module for standalone consumability.
Nothing here has been changed — each item needs a maintainer decision.

A tag alone does not make a module consumable. When someone runs
`go get github.com/the-protobuf-project/runtime-go/<mod>@v0.1.0`, Go reads that
module's own `go.mod` and **ignores its `replace` directives** — `replace` only
applies in the main module. So a require that resolves only inside the
workspace fails immediately for the consumer.

Modules safe to tag today: `agents`, `network`, `telemetry`, `ulid`.

## 1. Placeholder versions that do not exist on the proxy

`v0.0.0-00010101000000-000000000000` is what Go writes when a require is
satisfied purely by a workspace or `replace`. It is not a real version and
resolves nowhere.

| Module | Unresolvable requires |
| --- | --- |
| `cache` | `observability`, `ulid` |
| `database` | `cache`, `ulid` |
| `grpc` | `agents`, `observability` |
| `streams` | `observability`, `ulid` |
| `database/examples` | `cache`, `ulid` |
| `streams/examples` | `ulid` |

## 2. Bare `v0.0.0` requires

Equally unresolvable — no `v0.0.0` tag exists in this repo.

| Module | Require |
| --- | --- |
| `blockchain` | `database v0.0.0` |
| `interfaces` | `database v0.0.0` |
| `database/examples` | `database v0.0.0` |
| `streams/examples` | `streams v0.0.0` |

## 3. `replace`-masked API skew in `observability`

`observability/go.mod` carries, with its own comment:

```
// telemetry is versioned alongside this module in runtime-go; the published
// version predates the Logger contract. Drop this once it is released.
replace github.com/the-protobuf-project/runtime-go/telemetry => ../telemetry
```

The require pins `telemetry v0.0.0-20260722084318-b90e81eeadb7`, a real
published version that **lacks the `Logger` contract**. In the workspace the
`replace` hides this. A consumer gets the published telemetry and fails to
compile. Releasing `telemetry/v0.1.0` first, then repointing this require, is
the fix.

## 4. The `opentelemetry` dependency does not exist

Per the requested rename, these modules now require:

```
github.com/the-protobuf-project/opentelemetry/opentelemetry-go
```

That repository does not exist yet (the published one is
`.../opentelementry/opentelementry-go`, with the typo). Affected:
`observability`, `grpc`, `cache`, `streams`. These cannot build at all until
the sibling repo is renamed and republished. See the `TODO(maintainer)` notes
in `observability/go.mod` and `grpc/go.mod`.

## Suggested order

1. Rename + republish the `opentelementry` repo as `opentelemetry`; re-resolve
   the requires and regenerate the affected `go.sum` files.
2. Tag `telemetry`, `ulid`, `agents`, `network` at `v0.1.0`.
3. In `observability`, drop the `telemetry` replace and require
   `telemetry v0.1.0`. Tag `observability/v0.1.0`.
4. Repeat outward through `cache`/`streams` -> `database` ->
   `blockchain`/`interfaces`/`grpc` -> the two `examples` modules, dropping each
   `replace` and pinning real versions as its dependencies get tagged.
5. Consider whether `database/examples` and `streams/examples` need tags at all
   — both are `package main` and neither is importable as a library.

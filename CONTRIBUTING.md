# Contributing

Thanks for your interest in runtime-go. This document covers how changes get
reviewed and merged.

For **security vulnerabilities, do not open an issue or a pull request** —
follow [SECURITY.md](SECURITY.md) instead.

## Sign-off: DCO or CLA

TODO(maintainer): decide whether this project requires a Developer Certificate
of Origin sign-off, a Contributor License Agreement, or neither, and state the
decision here.

Until that is decided, **contributions cannot be accepted from anyone other
than the maintainer**, because the provenance terms for third-party code have
not been established. This is the one item that genuinely blocks outside
contribution — it is called out here rather than left implicit.

The three options, briefly:

- **DCO** — contributors add `Signed-off-by:` to each commit (`git commit -s`),
  attesting they have the right to submit the work. Low friction; enforceable
  with a GitHub Action. This is the common choice for Apache-2.0 projects.
- **CLA** — contributors sign an agreement granting the project explicit
  rights. Heavier process, usually chosen when relicensing may be needed later.
- **Neither** — relies solely on the inbound=outbound convention of Apache-2.0
  § 5. Simplest, but leaves provenance undocumented.

## Repository layout

This is a `go.work` multi-module workspace with **no root module**. Each
directory listed in `go.work` is an independently versioned Go module, released
under a directory-prefixed tag (`ulid/v0.1.0`). `tools/docgen` is a separate
module deliberately kept outside the workspace.

## Development

Build and test inside the workspace as usual:

```sh
go build ./...
go test ./... -race
```

**Before opening a PR, also verify your module builds standalone.** The
workspace resolves sibling modules from disk, which hides missing requirements
in a module's own `go.mod` — a consumer running `go get` on that module alone
would get a build failure. CI enforces this in `.github/workflows/modules.yml`:

```sh
cd <module>
GOWORK=off go build ./...
GOWORK=off go test ./...
GOWORK=off go mod verify
GOWORK=off go mod tidy   # must produce no diff
```

If adding a requirement is what makes this pass, say so in the PR description
and name the module that was under-specified. Do not add requirements silently.

## Code standards

- **Every file stays under 200 lines** — Go source, tests and Markdown alike.
  Split on real seams (a type and its methods, one lifecycle phase, one
  protocol), never mid-function. Check before you push:
  `find . -name '*.go' -o -name '*.md' | xargs wc -l | awk '$1>=200'`
- **Every package carries a `doc.go`.** CI fails without one.
- **Regenerate the README package table** with `make docs` when you change a
  package doc comment. `make docs-check` runs in CI and fails if it is stale.
- `golangci-lint` must pass; config is in `.golangci.yml`.
- Tests run with `-race`.

## Pull requests

1. Branch from `main`. Do not push directly to `main`.
2. Keep the change focused; unrelated cleanups belong in their own PR.
3. Write a description that says what changed and why, not just what.
4. All CI checks must pass — the `CI gate` job plus the per-module
   `Isolated build` jobs.
5. Every PR needs review approval from a code owner (see
   [.github/CODEOWNERS](.github/CODEOWNERS)) before merge.
6. History is not rewritten on `main`. Do not force-push a merged branch.

## Commits

Commit messages follow the conventional-commit style already used in this
repository, for example:

```
feat(cache): add Redis scheduled-delivery provider
fix(streams): release the subscriber socket on context cancel
chore(deps/grpc): bump google.golang.org/grpc
```

## Releases

Releases are cut by the maintainer. Module tags are prepared by
`scripts/tag-release.sh`, which never pushes; see
[scripts/RELEASE-BLOCKERS.md](scripts/RELEASE-BLOCKERS.md) for the current
state of what is and is not releasable.

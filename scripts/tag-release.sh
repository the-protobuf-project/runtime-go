#!/usr/bin/env bash
# Prepare per-module release tags for the runtime-go workspace.
#
# This repo is a go.work multi-module workspace with no root go.mod, so every
# module is released under its own directory-prefixed tag: <dir>/vX.Y.Z. The Go
# module proxy derives the module path from that prefix, so the tag prefix and
# the declared module path must agree exactly.
#
# This script NEVER pushes. By default it only prints what it would do.
#   ./scripts/tag-release.sh            # dry run (default)
#   ./scripts/tag-release.sh --create   # create annotated tags locally
# Pushing is left to the maintainer; the exact command is printed at the end.
#
# !! READ scripts/RELEASE-BLOCKERS.md BEFORE TAGGING ANYTHING !!
# Most modules below cannot currently be consumed via `go get` even once
# tagged. Tiers 2-5 are marked BLOCKED and are skipped unless you pass
# --include-blocked, which you should not do until their go.mod files are
# fixed.

set -euo pipefail

VERSION="v0.1.0"
CREATE=0
INCLUDE_BLOCKED=0

for arg in "$@"; do
  case "$arg" in
    --create)          CREATE=1 ;;
    --include-blocked) INCLUDE_BLOCKED=1 ;;
    -h|--help)         sed -n '2,18p' "$0"; exit 0 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# Release order. A module must be tagged only after every runtime-go module it
# requires, because its go.mod has to pin those by real version first.
#
#   tier  module              requires (runtime-go siblings)
#   ----  ------              ------------------------------
#   0     agents              -
#   0     network             -
#   0     telemetry           -
#   0     ulid                -
#   1     observability       telemetry
#   2     cache               observability, telemetry, ulid
#   2     streams             observability, telemetry, ulid
#   3     database            cache, telemetry, ulid
#   4     blockchain          database, telemetry
#   4     interfaces          database, telemetry
#   4     grpc                agents, observability, telemetry
#   5     database/examples   cache, database, telemetry, ulid
#   5     streams/examples    streams, observability, telemetry, ulid
# ---------------------------------------------------------------------------

# Modules whose go.mod resolves entirely from the public proxy today.
READY=(
  agents
  network
  telemetry
  ulid
)

# Modules with at least one sibling require that cannot resolve outside the
# workspace -- either the placeholder v0.0.0-00010101000000-000000000000 or a
# bare v0.0.0. Tagging these produces artifacts that fail `go get` immediately.
BLOCKED=(
  observability
  cache
  streams
  database
  blockchain
  interfaces
  grpc
  database/examples
  streams/examples
)

fail() { echo "error: $*" >&2; exit 1; }

# --- preconditions --------------------------------------------------------
[ -f go.work ] || fail "run from the repo root (go.work not found)"

if [ "$CREATE" -eq 1 ]; then
  [ -z "$(git status --porcelain)" ] || fail "working tree is dirty; commit or stash first"
  branch=$(git rev-parse --abbrev-ref HEAD)
  [ "$branch" = "main" ] || fail "expected branch main, on '$branch'"
fi

# Verify each tag prefix matches the module path the directory declares.
verify_path() {
  local dir="$1"
  local want="github.com/the-protobuf-project/runtime-go/${dir}"
  local got
  got=$(awk '/^module /{print $2; exit}' "${dir}/go.mod")
  [ "$got" = "$want" ] || fail "module path mismatch in ${dir}/go.mod: declared '${got}', tag prefix implies '${want}'"
}

tag_one() {
  local dir="$1"
  local tag="${dir}/${VERSION}"

  verify_path "$dir"

  if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
    echo "  skip   ${tag} (already exists)"
    return
  fi

  if [ "$CREATE" -eq 1 ]; then
    git tag -a "$tag" -m "${dir} ${VERSION}"
    echo "  tagged ${tag}"
  else
    echo "  git tag -a ${tag} -m '${dir} ${VERSION}'"
  fi
}

# --- run ------------------------------------------------------------------
if [ "$CREATE" -eq 1 ]; then
  echo "Creating local tags (nothing is pushed):"
else
  echo "DRY RUN. Commands that would run (pass --create to apply):"
fi

echo
echo "Ready to release:"
for dir in "${READY[@]}"; do tag_one "$dir"; done

echo
if [ "$INCLUDE_BLOCKED" -eq 1 ]; then
  echo "BLOCKED modules (--include-blocked given; these will fail 'go get'):"
  for dir in "${BLOCKED[@]}"; do tag_one "$dir"; done
else
  echo "Blocked modules (skipped; see scripts/RELEASE-BLOCKERS.md):"
  for dir in "${BLOCKED[@]}"; do echo "  skip   ${dir}/${VERSION} (unresolvable sibling require)"; done
fi

echo
echo "Nothing has been pushed. To publish the tags created above, run:"
for dir in "${READY[@]}"; do echo "  git push origin ${dir}/${VERSION}"; done
echo
echo "Or all at once:  git push origin --tags"

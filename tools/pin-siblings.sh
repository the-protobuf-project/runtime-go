#!/usr/bin/env bash
# Pins every module's requirements on its runtime-go siblings to one commit.
#
# Inside this repository go.work and the replace directives point each module
# at its sibling's directory, so the required versions are never read here. A
# consumer (`go get github.com/the-protobuf-project/runtime-go/grpc@main`) sees
# no workspace and ignores a dependency's replaces, so those required versions
# are what it builds against: left stale, it gets an old sibling that lacks
# what the module now uses, and the build fails. Run this after a merge that
# changes an API a sibling uses, and commit the result.
#
# Usage: tools/pin-siblings.sh [commit]   (default: origin/main)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
commit=$(git rev-parse "${1:-origin/main}")
stamp=$(TZ=UTC git log -1 --format=%cd --date=format-local:%Y%m%d%H%M%S "$commit")
version="v0.0.0-${stamp}-${commit:0:12}"
prefix=github.com/the-protobuf-project/runtime-go

for mod in $(git ls-files '*go.mod'); do
	for dep in $(go mod edit -json "$mod" | jq -r --arg p "$prefix/" '.Require[]?.Path | select(startswith($p))'); do
		dir=${dep#"$prefix"/}
		# Only a sibling that is a module at that commit; an import of a
		# package inside one is resolved through its module.
		git cat-file -e "$commit:$dir/go.mod" 2>/dev/null || continue
		go mod edit -require="$dep@$version" "$mod"
	done
done
echo "siblings pinned to $version"

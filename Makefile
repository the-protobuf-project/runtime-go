# runtime-go developer tasks.

.PHONY: docs docs-check

## docs: regenerate the README package table from every package's doc.go
docs:
	cd tools/docgen && GOWORK=off go run .

## docs-check: fail if a package lacks a doc.go or the README table is stale (CI)
docs-check:
	cd tools/docgen && GOWORK=off go run . -check

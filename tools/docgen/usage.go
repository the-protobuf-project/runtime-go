package main

import (
	"fmt"
	"go/doc"
	"go/doc/comment"
	"strings"
)

// firstCode returns the first code block in a package's doc comment, the
// example the package leads with, or "" when its doc shows none.
func firstCode(dp *doc.Package) string {
	for _, b := range dp.Parser().Parse(dp.Doc).Content {
		if c, ok := b.(*comment.Code); ok {
			return strings.TrimRight(c.Text, "\n")
		}
	}
	return ""
}

// writeStart renders the top of a module's box: how to install the module and,
// when its lead package's doc shows one, how to use it.
func writeStart(b *strings.Builder, modulePath string, lead pkg) {
	if modulePath != "" {
		fmt.Fprintf(b, "**Install**\n\n```sh\ngo get %s\n```\n\n", modulePath)
	}
	if lead.usage != "" {
		fmt.Fprintf(b, "**Usage**\n\n```go\n%s\n```\n\n", lead.usage)
	}
}

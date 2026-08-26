// Command fcischeck fails the build if a Swarm core package uses a
// non-deterministic or I/O-bound call. Run it over ./internal/core/... in CI
// and in the local edit hook. It is the structural guarantee that cores are
// pure: the clock and randomness are injected as data, never read in place.
//
// It complements depguard (which bans whole imports); fcischeck bans specific
// impure *calls* from packages that are otherwise legal for their types — e.g.
// time.Duration is fine, but time.Now() is not.
//
// Usage: fcischeck ./internal/core/...   (dirs, optionally suffixed with /...)
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// Imports a pure core may never use. depguard also covers these; fcischeck
// enforces them too so the core-purity guarantee holds on one verified tool.
var bannedImports = map[string]string{
	"net":                        "core is pure — no networking; put it in internal/shell",
	"net/http":                   "core is pure — no networking",
	"os":                         "core is pure — no filesystem or env access",
	"os/exec":                    "core is pure — no process execution",
	"database/sql":               "core is pure — no database access",
	"math/rand":                  "randomness must be injected as data, not drawn inside a core",
	"crypto/rand":                "randomness must be injected as data, not drawn inside a core",
	"google.golang.org/grpc":     "core is pure — no RPC; put it in internal/shell",
	"github.com/nats-io/nats.go": "core is pure — no message bus; put it in internal/shell",
}

// Any import under one of these prefixes is banned in core.
var bannedImportPrefixes = map[string]string{
	"github.com/msivraj/swarm/internal/shell": "core must never import shell (the dependency points the other way)",
}

// Specific impure calls from packages that are otherwise allowed for their types.
var bannedCalls = map[string]map[string]string{
	"time": {
		"Now":   "read the clock in the shell and pass the Instant in as data",
		"Since": "compute durations from injected Instants, not the wall clock",
		"Until": "compute durations from injected Instants, not the wall clock",
	},
}

func main() {
	roots := os.Args[1:]
	if len(roots) == 0 {
		roots = []string{"./internal/core/..."}
	}
	fset := token.NewFileSet()
	violations := 0
	for _, root := range roots {
		root = strings.TrimSuffix(root, "...")
		root = strings.TrimSuffix(root, string(filepath.Separator))
		if root == "" {
			root = "."
		}
		_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if perr != nil {
				fmt.Fprintf(os.Stderr, "%s: parse error: %v\n", path, perr)
				violations++
				return nil
			}
			violations += checkFile(fset, f)
			return nil
		})
	}
	if violations > 0 {
		fmt.Fprintf(os.Stderr, "\nfcischeck: %d FCIS violation(s) — cores must be pure\n", violations)
		os.Exit(1)
	}
}

func checkFile(fset *token.FileSet, f *ast.File) int {
	n := 0
	aliases := map[string]string{} // local import name -> package path
	for _, imp := range f.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		name := path[strings.LastIndex(path, "/")+1:]
		if imp.Name != nil {
			name = imp.Name.Name
		}
		switch name {
		case ".":
			fmt.Fprintf(os.Stderr, "%s: fcis: dot-imports are banned in core (they hide the origin of calls)\n", fset.Position(imp.Pos()))
			n++
			continue
		case "_":
			continue
		}
		aliases[name] = path
		if msg, bad := bannedImports[path]; bad {
			fmt.Fprintf(os.Stderr, "%s: fcis: import %q — %s\n", fset.Position(imp.Pos()), path, msg)
			n++
			continue
		}
		for prefix, msg := range bannedImportPrefixes {
			if path == prefix || strings.HasPrefix(path, prefix+"/") {
				fmt.Fprintf(os.Stderr, "%s: fcis: import %q — %s\n", fset.Position(imp.Pos()), path, msg)
				n++
				break
			}
		}
	}
	ast.Inspect(f, func(node ast.Node) bool {
		sel, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		x, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		path, ok := aliases[x.Name]
		if !ok {
			return true
		}
		if funcs, banned := bannedCalls[path]; banned {
			if msg, bad := funcs[sel.Sel.Name]; bad {
				fmt.Fprintf(os.Stderr, "%s: fcis: %s.%s — %s\n", fset.Position(sel.Pos()), x.Name, sel.Sel.Name, msg)
				n++
			}
		}
		return true
	})
	return n
}

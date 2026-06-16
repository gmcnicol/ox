// Package filter evaluates per-watcher filter expressions against record values.
//
// In production this will compile CEL programs; for v0 the interface is
// deliberately thin so the watch engine never imports CEL directly.
package filter

import (
	"encoding/json"
	"fmt"
)

// Filter is a compiled, re-usable predicate.
// A nil *Filter always returns true (match-all).
type Filter struct {
	expr string
	// compiled cel.Program goes here in the real impl
}

// Compile validates and compiles a CEL expression.
// An empty expression returns a match-all filter.
func Compile(expr string) (*Filter, error) {
	if expr == "" {
		return &Filter{}, nil
	}
	// TODO: replace with cel.NewEnv / cel.Program once cel-go is vendored.
	return &Filter{expr: expr}, nil
}

// Match evaluates the filter against a JSON-encoded value.
// A nil filter always matches.
func (f *Filter) Match(value []byte) (bool, error) {
	if f == nil || f.expr == "" {
		return true, nil
	}

	// Stub: decode JSON and do basic field==value matching so tests can run
	// before cel-go is wired in.  The real implementation delegates to a
	// compiled cel.Program here.
	var record map[string]any
	if err := json.Unmarshal(value, &record); err != nil {
		return false, fmt.Errorf("filter: unmarshal: %w", err)
	}

	// For tests a "where" query of the form   field==value   is parsed naively.
	// TODO: remove when cel-go is available.
	return stubMatch(f.expr, record), nil
}

// stubMatch is a temporary stand-in for CEL evaluation.
// It only understands equality: `field == "value"`.
func stubMatch(expr string, record map[string]any) bool {
	var key, val string
	if _, err := fmt.Sscanf(expr, `%s == "%s"`, &key, &val); err != nil {
		// can't parse → allow through so unknown queries don't silently drop
		return true
	}
	// strip trailing quote that Sscanf captures
	if len(val) > 0 && val[len(val)-1] == '"' {
		val = val[:len(val)-1]
	}
	v, ok := record[key]
	if !ok {
		return false
	}
	return fmt.Sprint(v) == val
}

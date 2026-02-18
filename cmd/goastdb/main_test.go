package main

import (
	"strings"
	"testing"

	"github.com/Yacobolo/goastdb/pkg/astdb/governance"
)

func TestResolveDuckDBPath_Default(t *testing.T) {
	t.Parallel()

	got := resolveDuckDBPath("/repo", "")
	want := "/repo/.goast/ast.db"
	if got != want {
		t.Fatalf("unexpected duckdb path: got=%q want=%q", got, want)
	}
}

func TestResolveDuckDBPath_Explicit(t *testing.T) {
	t.Parallel()

	got := resolveDuckDBPath("/repo", "/tmp/custom.db")
	want := "/tmp/custom.db"
	if got != want {
		t.Fatalf("unexpected duckdb path: got=%q want=%q", got, want)
	}
}

func TestFormatTable(t *testing.T) {
	t.Parallel()

	table := governance.Table{
		Columns: []string{"kind", "n"},
		Rows: [][]any{
			{"*ast.Ident", 10},
			{"*ast.CallExpr", 5},
		},
	}
	s := formatTable(table)
	if !strings.Contains(s, "| kind") || !strings.Contains(s, "| n") {
		t.Fatalf("table missing headers: %s", s)
	}
	if !strings.Contains(s, "*ast.Ident") {
		t.Fatalf("table missing row values: %s", s)
	}
}

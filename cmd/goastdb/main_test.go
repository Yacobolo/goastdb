package main

import (
	"reflect"
	"testing"
)

func TestParseIDs(t *testing.T) {
	t.Parallel()

	got := parseIDs([]string{" A,B ", "A", "", " C "})
	want := []string{"A", "B", "C"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected parsed IDs: got=%v want=%v", got, want)
	}
}

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

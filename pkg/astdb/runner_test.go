package astdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRun_BuildAndReuse(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dbPath := filepath.Join(root, ".tmp", "goastdb", "ast.duckdb")
	writeGoFile(t, filepath.Join(root, "main.go"), "package main\n\nfunc main() {}\n")

	opts := DefaultOptions()
	opts.RepoRoot = root
	opts.DuckDBPath = dbPath
	opts.Mode = "build"
	opts.QueryBench = false
	opts.KeepOutputFiles = true

	res1, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if res1.Sync.Action != "rebuild" {
		t.Fatalf("expected rebuild action, got %q", res1.Sync.Action)
	}

	opts.Mode = "query"
	res2, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if res2.Sync.Action != "reuse" {
		t.Fatalf("expected reuse action, got %q", res2.Sync.Action)
	}
	if res2.Sync.FilesCount == 0 || res2.Sync.NodesCount == 0 {
		t.Fatalf("expected non-zero db counts, got files=%d nodes=%d", res2.Sync.FilesCount, res2.Sync.NodesCount)
	}
}

func TestRun_SubdirEscapeRejected(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	opts := DefaultOptions()
	opts.RepoRoot = root
	opts.Subdir = "../outside"
	opts.QueryBench = false
	_, err := Run(context.Background(), opts)
	if err == nil {
		t.Fatal("expected error for escaping subdir")
	}
}

func writeGoFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
}

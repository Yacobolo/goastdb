package governance

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Yacobolo/goastdb/pkg/astdb"
)

func TestValidateRule_InvalidSeverity(t *testing.T) {
	t.Parallel()
	err := ValidateRule(Rule{
		ID:          "R1",
		Category:    "style",
		Severity:    "nope",
		Description: "invalid",
		QuerySQL:    "SELECT 1",
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestRunner_RunSelectedRule(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dbPath := filepath.Join(root, ".tmp", "goastdb", "ast.duckdb")
	writeFile(t, filepath.Join(root, "main.go"), "package main\n\nfunc testProdReadyFunction() {}\n")

	opts := astdb.DefaultOptions()
	opts.RepoRoot = root
	opts.DuckDBPath = dbPath
	opts.Mode = "build"
	opts.QueryBench = false
	if _, err := astdb.Run(context.Background(), opts); err != nil {
		t.Fatalf("build ast db: %v", err)
	}

	runner := NewRunner(dbPath)
	rule := Rule{
		ID:          "FIND_TEST_PROD_READY_FUNCTION",
		Category:    "testing",
		Severity:    "warning",
		Description: "find specific function",
		Enabled:     true,
		QuerySQL: `
SELECT
  f.path AS file_path,
  n.node_text AS symbol,
  'matched function identifier' AS detail,
  n.start_line AS line
FROM nodes n
JOIN files f ON f.file_id = n.file_id
WHERE n.kind = '*ast.Ident' AND n.node_text = 'testProdReadyFunction'
`,
	}
	if err := runner.UpsertRules(context.Background(), []Rule{rule}); err != nil {
		t.Fatalf("upsert rule: %v", err)
	}

	violations, err := runner.Run(context.Background(), RunOptions{RuleIDs: []string{rule.ID}})
	if err != nil {
		t.Fatalf("run rules: %v", err)
	}
	if len(violations) == 0 {
		t.Fatal("expected at least one violation")
	}
	if violations[0].RuleID != rule.ID {
		t.Fatalf("unexpected rule id: %q", violations[0].RuleID)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
}

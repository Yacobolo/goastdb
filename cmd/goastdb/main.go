package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/Yacobolo/goastdb/pkg/astdb"
	"github.com/Yacobolo/goastdb/pkg/astdb/explore"
	"github.com/Yacobolo/goastdb/pkg/astdb/governance"
)

type outputEnvelope struct {
	Result astdb.Result     `json:"result"`
	Table  governance.Table `json:"table"`
	Helper *explore.Query   `json:"helper,omitempty"`
	Mode   string           `json:"mode"`
}

func main() {
	if len(os.Args) < 2 {
		printRootUsage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "query":
		runQueryCommand(os.Args[2:])
	case "helper":
		runHelperCommand(os.Args[2:])
	case "-h", "--help", "help":
		printRootUsage()
	default:
		log.Fatalf("unknown command %q\n\n%s", os.Args[1], rootUsageText())
	}
}

func runQueryCommand(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	repo := fs.String("repo", ".", "repository root to scan")
	duckdbPath := fs.String("duckdb", "", "duckdb output path (default <repo>/.goast/ast.db)")
	format := fs.String("format", "text", "output format: text|json")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: goastdb query [flags] <sql>")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Executes one SQL query against the AST database.")
		fmt.Fprintln(os.Stderr)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if len(fs.Args()) != 1 {
		fs.Usage()
		os.Exit(2)
	}

	sqlQuery := fs.Args()[0]
	result, table := executeQuery(*repo, resolveDuckDBPath(*repo, *duckdbPath), sqlQuery)
	printQueryOutput(*format, outputEnvelope{Mode: "query", Result: result, Table: table})
}

func runHelperCommand(args []string) {
	fs := flag.NewFlagSet("helper", flag.ExitOnError)
	repo := fs.String("repo", ".", "repository root to scan")
	duckdbPath := fs.String("duckdb", "", "duckdb output path (default <repo>/.goast/ast.db)")
	format := fs.String("format", "text", "output format: text|json")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: goastdb helper [flags] list")
		fmt.Fprintln(os.Stderr, "       goastdb helper [flags] <id>")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Lists helper queries or executes one helper query by ID.")
		fmt.Fprintln(os.Stderr)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	if len(fs.Args()) == 0 || fs.Args()[0] == "list" {
		printHelperList(*format, explore.DefaultQueries())
		return
	}
	if len(fs.Args()) != 1 {
		fs.Usage()
		os.Exit(2)
	}

	helperID := strings.TrimSpace(fs.Args()[0])
	helpers, err := explore.SelectQueries([]string{helperID})
	if err != nil {
		log.Fatal(err)
	}
	helper := helpers[0]

	result, table := executeQuery(*repo, resolveDuckDBPath(*repo, *duckdbPath), helper.SQL)
	printQueryOutput(*format, outputEnvelope{Mode: "helper", Result: result, Table: table, Helper: &helper})
}

func executeQuery(repo, duckdbPath, sqlQuery string) (astdb.Result, governance.Table) {
	ctx := context.Background()
	opts := astdb.DefaultOptions()
	opts.RepoRoot = repo
	opts.DuckDBPath = duckdbPath
	opts.Mode = "query"
	opts.QueryBench = false

	result, err := astdb.Run(ctx, opts)
	if err != nil {
		log.Fatal(err)
	}

	runner := governance.NewRunner(opts.DuckDBPath)
	table, err := runner.QueryTable(ctx, sqlQuery)
	if err != nil {
		log.Fatal(err)
	}
	return result, table
}

func printHelperList(format string, queries []explore.Query) {
	if format == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(struct {
			HelperQueries []explore.Query `json:"helper_queries"`
		}{HelperQueries: queries}); err != nil {
			log.Fatal(err)
		}
		return
	}

	rows := make([][]any, 0, len(queries))
	for _, q := range queries {
		rows = append(rows, []any{q.ID, q.Description})
	}
	t := governance.Table{Columns: []string{"id", "description"}, Rows: rows}
	fmt.Println(formatTable(t))
	fmt.Printf("(%d rows)\n", len(rows))
}

func printQueryOutput(format string, out outputEnvelope) {
	if format != "text" && format != "json" {
		log.Fatalf("invalid -format %q (expected text or json)", format)
	}
	if format == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			log.Fatal(err)
		}
		return
	}

	fmt.Println(formatTable(out.Table))
	fmt.Printf("(%d rows)\n", len(out.Table.Rows))
}

func formatTable(t governance.Table) string {
	if len(t.Columns) == 0 {
		return "(no columns)"
	}

	const maxColWidth = 60
	widths := make([]int, len(t.Columns))
	for i, col := range t.Columns {
		widths[i] = minInt(maxColWidth, utf8.RuneCountInString(col))
	}

	cellRows := make([][]string, len(t.Rows))
	for r := range t.Rows {
		cellRows[r] = make([]string, len(t.Columns))
		for c := range t.Columns {
			var cell any
			if c < len(t.Rows[r]) {
				cell = t.Rows[r][c]
			}
			s := truncateCell(formatCell(cell), maxColWidth)
			cellRows[r][c] = s
			if w := utf8.RuneCountInString(s); w > widths[c] {
				widths[c] = minInt(maxColWidth, w)
			}
		}
	}

	var b strings.Builder
	b.WriteString(renderSeparator(widths))
	b.WriteString("\n")
	b.WriteString(renderRow(t.Columns, widths))
	b.WriteString("\n")
	b.WriteString(renderSeparator(widths))
	for _, row := range cellRows {
		b.WriteString("\n")
		b.WriteString(renderRow(row, widths))
	}
	b.WriteString("\n")
	b.WriteString(renderSeparator(widths))
	return b.String()
}

func renderSeparator(widths []int) string {
	var b strings.Builder
	b.WriteByte('+')
	for _, w := range widths {
		b.WriteString(strings.Repeat("-", w+2))
		b.WriteByte('+')
	}
	return b.String()
}

func renderRow(values []string, widths []int) string {
	var b strings.Builder
	b.WriteByte('|')
	for i, v := range values {
		pad := widths[i] - utf8.RuneCountInString(v)
		b.WriteByte(' ')
		b.WriteString(v)
		if pad > 0 {
			b.WriteString(strings.Repeat(" ", pad))
		}
		b.WriteString(" |")
	}
	return b.String()
}

func formatCell(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(v)
}

func truncateCell(s string, maxWidth int) string {
	if maxWidth <= 0 || utf8.RuneCountInString(s) <= maxWidth {
		return s
	}
	runes := []rune(s)
	if maxWidth <= 3 {
		return string(runes[:maxWidth])
	}
	return string(runes[:maxWidth-3]) + "..."
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func resolveDuckDBPath(repoRoot, duckdbPath string) string {
	if strings.TrimSpace(duckdbPath) != "" {
		return duckdbPath
	}
	return filepath.Join(repoRoot, ".goast", "ast.db")
}

func rootUsageText() string {
	return strings.TrimSpace(`goastdb indexes Go AST into DuckDB and executes SQL.

Usage:
  goastdb query [flags] <sql>
  goastdb helper [flags] list
  goastdb helper [flags] <id>

Examples:
  goastdb query "SELECT COUNT(*) AS files FROM files"
  goastdb helper list
  goastdb helper LARGE_FUNCTIONS_BY_LINES

Defaults:
  --repo defaults to current directory
  --duckdb defaults to <repo>/.goast/ast.db
`)
}

func printRootUsage() {
	fmt.Println(rootUsageText())
}

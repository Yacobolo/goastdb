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

	"github.com/Yacobolo/goastdb/pkg/astdb"
	"github.com/Yacobolo/goastdb/pkg/astdb/explore"
	"github.com/Yacobolo/goastdb/pkg/astdb/governance"
)

type queryRun struct {
	Name string           `json:"name"`
	SQL  string           `json:"sql"`
	Rows []governance.Row `json:"rows,omitempty"`
}

type outputEnvelope struct {
	Result        astdb.Result    `json:"result"`
	Runs          []queryRun      `json:"runs,omitempty"`
	HelperQueries []explore.Query `json:"helper_queries,omitempty"`
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
		fmt.Fprintln(os.Stderr, "Usage: goastdb query [flags] <sql> [<sql>...]")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Executes one or more raw SQL queries against the AST database.")
		fmt.Fprintln(os.Stderr, "")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	sqlQueries := fs.Args()
	if len(sqlQueries) == 0 {
		fs.Usage()
		os.Exit(2)
	}

	result, runs := executeQueries(*repo, resolveDuckDBPath(*repo, *duckdbPath), sqlQueries)
	printOutput(*format, outputEnvelope{Result: result, Runs: runs})
}

func runHelperCommand(args []string) {
	fs := flag.NewFlagSet("helper", flag.ExitOnError)

	repo := fs.String("repo", ".", "repository root to scan")
	duckdbPath := fs.String("duckdb", "", "duckdb output path (default <repo>/.goast/ast.db)")
	format := fs.String("format", "text", "output format: text|json")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: goastdb helper [flags] list")
		fmt.Fprintln(os.Stderr, "       goastdb helper [flags] <id[,id...]> [<id>...]")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Lists or executes built-in helper SQL queries.")
		fmt.Fprintln(os.Stderr, "")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}

	ids := parseIDs(fs.Args())
	if len(ids) == 0 || (len(ids) == 1 && ids[0] == "list") {
		printHelperList(*format, explore.DefaultQueries())
		return
	}

	helperQueries, err := explore.SelectQueries(ids)
	if err != nil {
		log.Fatal(err)
	}
	sqlQueries := make([]string, 0, len(helperQueries))
	names := make([]string, 0, len(helperQueries))
	for _, q := range helperQueries {
		sqlQueries = append(sqlQueries, q.SQL)
		names = append(names, q.ID)
	}

	result, runs := executeNamedQueries(*repo, resolveDuckDBPath(*repo, *duckdbPath), names, sqlQueries)
	printOutput(*format, outputEnvelope{Result: result, Runs: runs, HelperQueries: helperQueries})
}

func executeQueries(repo, duckdbPath string, sqlQueries []string) (astdb.Result, []queryRun) {
	names := make([]string, 0, len(sqlQueries))
	for i := range sqlQueries {
		names = append(names, fmt.Sprintf("raw_%d", i+1))
	}
	return executeNamedQueries(repo, duckdbPath, names, sqlQueries)
}

func executeNamedQueries(repo, duckdbPath string, names, sqlQueries []string) (astdb.Result, []queryRun) {
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
	runs := make([]queryRun, 0, len(sqlQueries))
	for i, sqlQuery := range sqlQueries {
		rows, err := runner.AdhocQuery(ctx, sqlQuery)
		if err != nil {
			log.Fatalf("query %s failed: %v", names[i], err)
		}
		runs = append(runs, queryRun{Name: names[i], SQL: sqlQuery, Rows: rows})
	}
	return result, runs
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
	fmt.Printf("helper queries: total=%d\n", len(queries))
	for _, q := range queries {
		fmt.Printf("helper=%s desc=%q\n", q.ID, q.Description)
	}
}

func printOutput(format string, out outputEnvelope) {
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

	res := out.Result
	fmt.Printf("scan: files=%d subdir=%q max_files=%d scan_ms=%d\n", res.ScanFiles, res.Subdir, res.MaxFiles, res.ScanElapsed.Milliseconds())
	fmt.Printf("build: action=%s reason=%q changed=%d parse_errors=%d parse_ms=%d load_ms=%d\n",
		res.Sync.Action,
		res.Sync.Reason,
		res.Sync.Changed,
		res.Sync.ParseErrors,
		res.Sync.ParseElapsed.Milliseconds(),
		res.Sync.LoadElapsed.Milliseconds(),
	)
	fmt.Printf("db: files=%d nodes=%d\n", res.Sync.FilesCount, res.Sync.NodesCount)

	if len(out.Runs) > 0 {
		fmt.Printf("sql runs=%d\n", len(out.Runs))
		for _, run := range out.Runs {
			fmt.Printf("run=%s rows=%d\n", run.Name, len(run.Rows))
			for i, row := range run.Rows {
				fmt.Printf("run=%s row[%d]=%v\n", run.Name, i+1, row)
			}
		}
	}
}

func parseIDs(args []string) []string {
	out := make([]string, 0, len(args))
	seen := make(map[string]struct{}, len(args))
	for _, arg := range args {
		parts := strings.Split(arg, ",")
		for _, part := range parts {
			id := strings.TrimSpace(part)
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
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
  goastdb query [flags] <sql> [<sql>...]
  goastdb helper [flags] list
  goastdb helper [flags] <id[,id...]> [<id>...]

Examples:
  goastdb query "SELECT COUNT(*) AS files FROM files"
  goastdb query "SELECT COUNT(*) AS files FROM files" "SELECT COUNT(*) AS nodes FROM nodes"
  goastdb helper list
  goastdb helper AST_KIND_DISTRIBUTION,FUNCTIONS_PER_FILE

Defaults:
  --repo defaults to current directory
  --duckdb defaults to <repo>/.goast/ast.db
`)
}

func printRootUsage() {
	fmt.Println(rootUsageText())
}

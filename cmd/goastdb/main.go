package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Yacobolo/goastdb/pkg/astdb"
	"github.com/Yacobolo/goastdb/pkg/astdb/governance"
)

type outputEnvelope struct {
	Result     astdb.Result           `json:"result"`
	AdhocRows  []governance.Row       `json:"adhoc_rows,omitempty"`
	Violations []governance.Violation `json:"violations,omitempty"`
	Rules      []governance.Rule      `json:"rules,omitempty"`
}

func main() {
	defaults := astdb.DefaultOptions()

	opts := astdb.Options{}
	var adhocSQL string
	var outputFormat string
	var governanceRun bool
	var governanceList bool
	var ruleIDsCSV string
	var timeout time.Duration
	flag.StringVar(&opts.RepoRoot, "repo", defaults.RepoRoot, "repository root to scan")
	flag.StringVar(&opts.Subdir, "subdir", defaults.Subdir, "optional subdirectory under repo root")
	flag.IntVar(&opts.MaxFiles, "max-files", defaults.MaxFiles, "optional cap for number of .go files (0 = all)")
	flag.IntVar(&opts.Workers, "workers", defaults.Workers, "parser worker count")
	flag.StringVar(&opts.DuckDBPath, "duckdb", defaults.DuckDBPath, "duckdb output path")
	flag.StringVar(&opts.Mode, "mode", defaults.Mode, "run mode: build|query|both")
	flag.BoolVar(&opts.Reuse, "reuse", defaults.Reuse, "reuse existing DB when fingerprint matches")
	flag.BoolVar(&opts.ForceRebuild, "force-rebuild", defaults.ForceRebuild, "force full rebuild")
	flag.BoolVar(&opts.QueryBench, "query-bench", defaults.QueryBench, "run built-in query benchmarks")
	flag.IntVar(&opts.QueryWarmup, "query-warmup", defaults.QueryWarmup, "warmup runs per query")
	flag.IntVar(&opts.QueryIters, "query-iters", defaults.QueryIters, "measured iterations per query")
	flag.BoolVar(&opts.KeepOutputFiles, "keep", defaults.KeepOutputFiles, "keep output DB file")
	flag.StringVar(&adhocSQL, "adhoc", "", "optional ad hoc SQL query to run after indexing")
	flag.BoolVar(&governanceRun, "governance", false, "run enabled governance rules")
	flag.BoolVar(&governanceList, "list-rules", false, "list governance rules")
	flag.StringVar(&ruleIDsCSV, "rules", "", "comma-separated rule IDs to run (default: all enabled)")
	flag.DurationVar(&timeout, "timeout", 0, "optional run timeout (e.g. 30s, 2m)")
	flag.StringVar(&outputFormat, "format", "text", "output format: text|json")
	flag.Parse()

	if outputFormat != "text" && outputFormat != "json" {
		log.Fatalf("invalid -format %q (expected text or json)", outputFormat)
	}

	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	result, err := astdb.Run(ctx, opts)
	if err != nil {
		log.Fatal(err)
	}

	runner := governance.NewRunner(opts.DuckDBPath)
	rules := make([]governance.Rule, 0)
	if governanceList || governanceRun {
		rules, err = runner.ListRules(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}

	violations := make([]governance.Violation, 0)
	if governanceRun {
		ruleIDs := parseRuleIDs(ruleIDsCSV)
		violations, err = runner.Run(ctx, governance.RunOptions{RuleIDs: ruleIDs})
		if err != nil {
			log.Fatal(err)
		}
	}

	adhocRows := make([]governance.Row, 0)
	if adhocSQL != "" {
		adhocRows, err = runner.AdhocQuery(ctx, adhocSQL)
		if err != nil {
			log.Fatal(err)
		}
	}

	if outputFormat == "json" {
		env := outputEnvelope{Result: result, AdhocRows: adhocRows, Violations: violations, Rules: rules}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(env); err != nil {
			log.Fatal(err)
		}
		return
	}

	fmt.Printf("scan: files=%d subdir=%q max_files=%d scan_ms=%d\n", result.ScanFiles, result.Subdir, result.MaxFiles, result.ScanElapsed.Milliseconds())
	fmt.Printf("build: action=%s reason=%q changed=%d parse_errors=%d parse_ms=%d load_ms=%d\n",
		result.Sync.Action,
		result.Sync.Reason,
		result.Sync.Changed,
		result.Sync.ParseErrors,
		result.Sync.ParseElapsed.Milliseconds(),
		result.Sync.LoadElapsed.Milliseconds(),
	)
	fmt.Printf("db: files=%d nodes=%d\n", result.Sync.FilesCount, result.Sync.NodesCount)

	if len(result.QueryResults) > 0 {
		fmt.Printf("queries: warmup=%d iters=%d\n", result.QueryWarmup, result.QueryIters)
		for i, q := range result.QueryResults {
			avgMS := float64(q.Elapsed.Milliseconds()) / float64(result.QueryIters)
			fmt.Printf("query[%d] %s: total_ms=%d avg_ms=%.3f\n", i+1, q.Name, q.Elapsed.Milliseconds(), avgMS)
		}
	}

	if governanceList {
		fmt.Printf("rules: total=%d\n", len(rules))
		for _, rule := range rules {
			fmt.Printf("rule=%s enabled=%t severity=%s category=%s desc=%q\n", rule.ID, rule.Enabled, rule.Severity, rule.Category, rule.Description)
		}
	}

	if governanceRun {
		fmt.Printf("governance violations=%d\n", len(violations))
		for i, v := range violations {
			fmt.Printf("violation[%d] rule=%s severity=%s file=%s line=%d detail=%q\n", i+1, v.RuleID, v.Severity, v.FilePath, v.Line, v.Detail)
		}
	}

	if len(adhocRows) > 0 {
		fmt.Printf("adhoc rows=%d\n", len(adhocRows))
		for i, row := range adhocRows {
			fmt.Printf("row[%d]=%v\n", i+1, row)
		}
	} else if adhocSQL != "" {
		fmt.Println("adhoc rows=0")
	}
}

func parseRuleIDs(csv string) []string {
	if strings.TrimSpace(csv) == "" {
		return nil
	}
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		id := strings.TrimSpace(p)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

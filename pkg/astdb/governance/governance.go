package governance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

type Rule struct {
	ID          string
	Category    string
	Severity    string
	Description string
	QuerySQL    string
	Enabled     bool
}

type Violation struct {
	RuleID    string
	Category  string
	Severity  string
	FilePath  string
	Symbol    string
	Detail    string
	Line      int
	RawValues map[string]any
}

type Row map[string]any

type Table struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows,omitempty"`
}

type RunOptions struct {
	RuleIDs []string
}

type Runner struct {
	duckDBPath string
}

func NewRunner(duckDBPath string) *Runner { return &Runner{duckDBPath: duckDBPath} }

func ValidateRule(rule Rule) error {
	rule.ID = strings.TrimSpace(rule.ID)
	rule.Category = strings.TrimSpace(rule.Category)
	rule.Severity = strings.ToLower(strings.TrimSpace(rule.Severity))
	rule.Description = strings.TrimSpace(rule.Description)
	rule.QuerySQL = strings.TrimSpace(rule.QuerySQL)

	if rule.ID == "" {
		return errors.New("rule id is required")
	}
	if rule.Category == "" {
		return fmt.Errorf("rule %s: category is required", rule.ID)
	}
	if rule.Description == "" {
		return fmt.Errorf("rule %s: description is required", rule.ID)
	}
	if rule.QuerySQL == "" {
		return fmt.Errorf("rule %s: query_sql is required", rule.ID)
	}
	switch rule.Severity {
	case "critical", "error", "warning", "info":
		return nil
	default:
		return fmt.Errorf("rule %s: invalid severity %q", rule.ID, rule.Severity)
	}
}

func (r *Runner) UpsertRules(ctx context.Context, rules []Rule) error {
	if len(rules) == 0 {
		return nil
	}
	db, err := sql.Open("duckdb", r.duckDBPath)
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS governance_rules (
	rule_id TEXT PRIMARY KEY,
	category TEXT NOT NULL,
	severity TEXT NOT NULL,
	description TEXT NOT NULL,
	query_sql TEXT NOT NULL,
	enabled BOOLEAN NOT NULL DEFAULT true,
	updated_unix BIGINT NOT NULL
)`); err != nil {
		return fmt.Errorf("ensure governance_rules table: %w", err)
	}

	stmt, err := db.PrepareContext(ctx, `
INSERT INTO governance_rules (rule_id, category, severity, description, query_sql, enabled, updated_unix)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(rule_id) DO UPDATE SET
	category=excluded.category,
	severity=excluded.severity,
	description=excluded.description,
	query_sql=excluded.query_sql,
	enabled=excluded.enabled,
	updated_unix=excluded.updated_unix`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	now := time.Now().Unix()
	for _, rule := range rules {
		rule.ID = strings.TrimSpace(rule.ID)
		rule.Category = strings.TrimSpace(rule.Category)
		rule.Severity = strings.ToLower(strings.TrimSpace(rule.Severity))
		rule.Description = strings.TrimSpace(rule.Description)
		rule.QuerySQL = strings.TrimSpace(rule.QuerySQL)
		if err := ValidateRule(rule); err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, rule.ID, rule.Category, rule.Severity, rule.Description, rule.QuerySQL, rule.Enabled, now); err != nil {
			return fmt.Errorf("upsert rule %s: %w", rule.ID, err)
		}
	}

	return nil
}

func (r *Runner) EnsureDefaultRules(ctx context.Context) error {
	return r.UpsertRules(ctx, defaultRules())
}

func (r *Runner) ListRules(ctx context.Context) ([]Rule, error) {
	if err := r.EnsureDefaultRules(ctx); err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", r.duckDBPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, `SELECT rule_id, category, severity, description, query_sql, enabled FROM governance_rules ORDER BY rule_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]Rule, 0)
	for rows.Next() {
		var r Rule
		if err := rows.Scan(&r.ID, &r.Category, &r.Severity, &r.Description, &r.QuerySQL, &r.Enabled); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (r *Runner) Run(ctx context.Context, opts RunOptions) ([]Violation, error) {
	rules, err := r.ListRules(ctx)
	if err != nil {
		return nil, err
	}
	selected := filterRules(rules, opts.RuleIDs)

	db, err := sql.Open("duckdb", r.duckDBPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	out := make([]Violation, 0)
	for _, rule := range selected {
		if !rule.Enabled {
			continue
		}
		rows, err := db.QueryContext(ctx, rule.QuerySQL)
		if err != nil {
			return nil, fmt.Errorf("rule %s: %w", rule.ID, err)
		}
		cols, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				_ = rows.Close()
				return nil, err
			}
			raw := make(map[string]any, len(cols))
			for i, col := range cols {
				raw[col] = normalize(vals[i])
			}
			out = append(out, Violation{
				RuleID:    rule.ID,
				Category:  rule.Category,
				Severity:  rule.Severity,
				FilePath:  asString(raw["file_path"]),
				Symbol:    asString(raw["symbol"]),
				Detail:    asString(raw["detail"]),
				Line:      asInt(raw["line"]),
				RawValues: raw,
			})
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}

	return out, nil
}

func (r *Runner) AdhocQuery(ctx context.Context, query string, args ...any) ([]Row, error) {
	db, err := sql.Open("duckdb", r.duckDBPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([]Row, 0)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(Row, len(cols))
		for i, c := range cols {
			row[c] = normalize(vals[i])
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (r *Runner) QueryTable(ctx context.Context, query string, args ...any) (Table, error) {
	db, err := sql.Open("duckdb", r.duckDBPath)
	if err != nil {
		return Table{}, err
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return Table{}, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return Table{}, err
	}
	out := Table{Columns: cols, Rows: make([][]any, 0)}

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return Table{}, err
		}
		row := make([]any, len(cols))
		for i := range cols {
			row[i] = normalize(vals[i])
		}
		out.Rows = append(out.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return Table{}, err
	}
	return out, nil
}

func filterRules(rules []Rule, ids []string) []Rule {
	if len(ids) == 0 {
		return rules
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	out := make([]Rule, 0, len(ids))
	for _, r := range rules {
		if _, ok := set[r.ID]; ok {
			out = append(out, r)
		}
	}
	return out
}

func normalize(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}

func asInt(v any) int {
	if v == nil {
		return 0
	}
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case int32:
		return int(x)
	case float64:
		return int(x)
	case string:
		i, _ := strconv.Atoi(strings.TrimSpace(x))
		return i
	default:
		return 0
	}
}

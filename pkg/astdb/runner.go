package astdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

const schemaVersion = "1"

type Options struct {
	RepoRoot        string
	Subdir          string
	MaxFiles        int
	Workers         int
	DuckDBPath      string
	Mode            string
	Reuse           bool
	ForceRebuild    bool
	QueryBench      bool
	QueryWarmup     int
	QueryIters      int
	KeepOutputFiles bool
}

func DefaultOptions() Options {
	return Options{
		RepoRoot:        ".",
		Workers:         runtime.NumCPU(),
		DuckDBPath:      "./.tmp/goastdb/ast.duckdb",
		Mode:            "both",
		Reuse:           true,
		QueryBench:      true,
		QueryWarmup:     2,
		QueryIters:      8,
		KeepOutputFiles: true,
	}
}

type Result struct {
	ScanFiles    int
	ScanElapsed  time.Duration
	Subdir       string
	MaxFiles     int
	Sync         SyncStats
	QueryWarmup  int
	QueryIters   int
	QueryResults []QueryResult
}

type SyncStats struct {
	Action       string
	Reason       string
	ParseElapsed time.Duration
	LoadElapsed  time.Duration
	Changed      int
	ParseErrors  int
	FilesCount   int64
	NodesCount   int64
}

type QueryResult struct {
	Name    string
	Elapsed time.Duration
}

type fileMeta struct {
	RelPath     string
	Size        int64
	ModUnixNano int64
}

type fileRow struct {
	ID         int64
	Path       string
	PkgName    string
	ParseError string
	Bytes      int64
}

type nodeRow struct {
	FileID        int64
	Ordinal       int
	ParentOrdinal int
	HasParent     bool
	Kind          string
	NodeText      string
	Pos           int
	End           int
	StartLine     int
	StartCol      int
	EndLine       int
	EndCol        int
	StartOffset   int
	EndOffset     int
}

type dbState struct {
	Exists            bool
	SchemaVersion     string
	SourceFingerprint string
	FilesCount        int64
	NodesCount        int64
}

type parseResult struct {
	File fileRow
	Rows []nodeRow
}

func Run(ctx context.Context, opts Options) (Result, error) {
	if err := normalizeAndValidateOptions(&opts); err != nil {
		return Result{}, err
	}
	mode := opts.Mode

	repoRoot, err := filepath.Abs(opts.RepoRoot)
	if err != nil {
		return Result{}, fmt.Errorf("resolve repo root: %w", err)
	}
	repoInfo, err := os.Stat(repoRoot)
	if err != nil {
		return Result{}, fmt.Errorf("stat repo root: %w", err)
	}
	if !repoInfo.IsDir() {
		return Result{}, fmt.Errorf("repo root is not a directory: %s", repoRoot)
	}

	if opts.Subdir != "" {
		subRoot := filepath.Join(repoRoot, opts.Subdir)
		subRootAbs, err := filepath.Abs(subRoot)
		if err != nil {
			return Result{}, fmt.Errorf("resolve subdir: %w", err)
		}
		rel, err := filepath.Rel(repoRoot, subRootAbs)
		if err != nil {
			return Result{}, fmt.Errorf("resolve subdir relation: %w", err)
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return Result{}, fmt.Errorf("subdir %q escapes repo root", opts.Subdir)
		}
	}

	dbPath, err := filepath.Abs(opts.DuckDBPath)
	if err != nil {
		return Result{}, fmt.Errorf("resolve db path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return Result{}, fmt.Errorf("create db dir: %w", err)
	}

	scanStart := time.Now()
	metas, err := collectGoFiles(repoRoot, opts.Subdir, opts.MaxFiles)
	if err != nil {
		return Result{}, err
	}
	if len(metas) == 0 {
		return Result{}, errors.New("no .go files found")
	}
	scanElapsed := time.Since(scanStart)

	fingerprint := sourceFingerprint(metas)
	state, err := inspectDuckDB(dbPath)
	if err != nil {
		return Result{}, err
	}

	rebuild := opts.ForceRebuild || !opts.Reuse || !state.Exists || state.SchemaVersion != schemaVersion || state.SourceFingerprint != fingerprint
	reason := "up-to-date"
	action := "reuse"
	if opts.ForceRebuild {
		reason = "force rebuild enabled"
	}
	if !opts.Reuse {
		reason = "reuse disabled"
	}
	if !state.Exists {
		reason = "database missing"
	}
	if state.Exists && state.SchemaVersion != schemaVersion {
		reason = "schema changed"
	}
	if state.Exists && state.SourceFingerprint != "" && state.SourceFingerprint != fingerprint {
		reason = "source changed"
	}

	res := Result{ScanFiles: len(metas), ScanElapsed: scanElapsed, Subdir: opts.Subdir, MaxFiles: opts.MaxFiles}

	if mode == "query" && !rebuild {
		res.Sync = SyncStats{Action: action, Reason: reason, FilesCount: state.FilesCount, NodesCount: state.NodesCount}
	} else {
		action = "rebuild"
		parseStart := time.Now()
		files, nodes, parseErrors := parseFiles(repoRoot, metas, opts.Workers)
		parseElapsed := time.Since(parseStart)

		loadStart := time.Now()
		if err := writeDatabase(ctx, dbPath, files, nodes, fingerprint); err != nil {
			return Result{}, err
		}
		loadElapsed := time.Since(loadStart)

		counts, err := inspectDuckDB(dbPath)
		if err != nil {
			return Result{}, err
		}

		res.Sync = SyncStats{
			Action:       action,
			Reason:       reason,
			Changed:      len(metas),
			ParseErrors:  parseErrors,
			ParseElapsed: parseElapsed,
			LoadElapsed:  loadElapsed,
			FilesCount:   counts.FilesCount,
			NodesCount:   counts.NodesCount,
		}
	}

	if opts.QueryBench && (mode == "both" || mode == "query") {
		qResults, err := benchmarkQueries(dbPath, defaultQueries(), opts.QueryWarmup, opts.QueryIters)
		if err != nil {
			return Result{}, err
		}
		res.QueryWarmup = max(0, opts.QueryWarmup)
		res.QueryIters = max(1, opts.QueryIters)
		res.QueryResults = qResults
	}

	if !opts.KeepOutputFiles {
		cleanupDuckDB(dbPath)
	}

	return res, nil
}

func normalizeAndValidateOptions(opts *Options) error {
	if opts == nil {
		return errors.New("options are required")
	}
	if strings.TrimSpace(opts.RepoRoot) == "" {
		opts.RepoRoot = "."
	}
	if strings.TrimSpace(opts.DuckDBPath) == "" {
		opts.DuckDBPath = "./.tmp/goastdb/ast.duckdb"
	}
	if opts.Workers <= 0 {
		opts.Workers = 1
	}
	if opts.MaxFiles < 0 {
		return fmt.Errorf("max-files must be >= 0")
	}
	if opts.QueryWarmup < 0 {
		return fmt.Errorf("query-warmup must be >= 0")
	}
	if opts.QueryIters <= 0 {
		return fmt.Errorf("query-iters must be > 0")
	}
	mode := strings.ToLower(strings.TrimSpace(opts.Mode))
	if mode == "" {
		mode = "both"
	}
	if mode != "build" && mode != "query" && mode != "both" {
		return fmt.Errorf("invalid mode %q", opts.Mode)
	}
	opts.Mode = mode
	opts.Subdir = strings.TrimSpace(filepath.Clean(opts.Subdir))
	if opts.Subdir == "." {
		opts.Subdir = ""
	}
	return nil
}

func collectGoFiles(repoRoot, subdir string, maxFiles int) ([]fileMeta, error) {
	skipDirs := map[string]struct{}{".git": {}, "vendor": {}, "node_modules": {}, "bin": {}, ".tmp": {}, "tmp": {}, ".cache": {}}
	root := repoRoot
	if subdir != "" {
		root = filepath.Join(repoRoot, subdir)
	}

	files := make([]fileMeta, 0, 2048)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if _, ok := skipDirs[d.Name()]; ok {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files = append(files, fileMeta{RelPath: filepath.ToSlash(rel), Size: info.Size(), ModUnixNano: info.ModTime().UnixNano()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk repo: %w", err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].RelPath < files[j].RelPath })
	if maxFiles > 0 && len(files) > maxFiles {
		files = files[:maxFiles]
	}
	return files, nil
}

func parseFiles(repoRoot string, metas []fileMeta, workers int) ([]fileRow, []nodeRow, int) {
	jobs := make(chan fileMeta)
	out := make(chan parseResult, len(metas))
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for meta := range jobs {
				out <- parseFile(repoRoot, meta)
			}
		}()
	}

	go func() {
		for _, meta := range metas {
			jobs <- meta
		}
		close(jobs)
		wg.Wait()
		close(out)
	}()

	files := make([]fileRow, 0, len(metas))
	nodes := make([]nodeRow, 0, len(metas)*256)
	parseErrors := 0
	for r := range out {
		if r.File.ParseError != "" {
			parseErrors++
		}
		files = append(files, r.File)
		nodes = append(nodes, r.Rows...)
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].FileID == nodes[j].FileID {
			return nodes[i].Ordinal < nodes[j].Ordinal
		}
		return nodes[i].FileID < nodes[j].FileID
	})

	return files, nodes, parseErrors
}

func parseFile(repoRoot string, meta fileMeta) parseResult {
	fileID := fileIDForPath(meta.RelPath)
	abs := filepath.Join(repoRoot, filepath.FromSlash(meta.RelPath))
	b, err := os.ReadFile(abs)
	if err != nil {
		return parseResult{File: fileRow{ID: fileID, Path: meta.RelPath, ParseError: err.Error()}}
	}
	fset := token.NewFileSet()
	parsed, parseErr := parser.ParseFile(fset, abs, b, parser.ParseComments|parser.AllErrors)
	row := fileRow{ID: fileID, Path: meta.RelPath, Bytes: int64(len(b))}
	if parseErr != nil {
		row.ParseError = parseErr.Error()
	}
	if parsed != nil && parsed.Name != nil {
		row.PkgName = parsed.Name.Name
	}
	if parsed == nil {
		return parseResult{File: row}
	}
	return parseResult{File: row, Rows: walkNodes(fset, fileID, parsed)}
}

func walkNodes(fset *token.FileSet, fileID int64, file *ast.File) []nodeRow {
	rows := make([]nodeRow, 0, 1024)
	stack := make([]int, 0, 256)
	ord := 0
	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			return true
		}
		ord++
		parentOrd := 0
		hasParent := false
		if len(stack) > 0 {
			parentOrd = stack[len(stack)-1]
			hasParent = true
		}
		sp := fset.PositionFor(n.Pos(), false)
		ep := fset.PositionFor(n.End(), false)
		so, eo := -1, -1
		if tf := fset.File(n.Pos()); tf != nil {
			so = tf.Offset(n.Pos())
			eo = tf.Offset(n.End())
		}
		rows = append(rows, nodeRow{
			FileID:        fileID,
			Ordinal:       ord,
			ParentOrdinal: parentOrd,
			HasParent:     hasParent,
			Kind:          fmt.Sprintf("%T", n),
			NodeText:      extractNodeText(n),
			Pos:           int(n.Pos()),
			End:           int(n.End()),
			StartLine:     sp.Line,
			StartCol:      sp.Column,
			EndLine:       ep.Line,
			EndCol:        ep.Column,
			StartOffset:   so,
			EndOffset:     eo,
		})
		stack = append(stack, ord)
		return true
	})
	return rows
}

func extractNodeText(n ast.Node) string {
	switch v := n.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.BasicLit:
		return v.Value
	case *ast.ImportSpec:
		if v.Path != nil {
			return v.Path.Value
		}
	}
	return ""
}

func writeDatabase(ctx context.Context, path string, files []fileRow, nodes []nodeRow, fingerprint string) error {
	cleanupDuckDB(path)
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open conn: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA threads=%d", runtime.NumCPU())); err != nil {
		return fmt.Errorf("set threads: %w", err)
	}

	if err := createSchema(ctx, conn); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, `BEGIN TRANSACTION`); err != nil {
		return err
	}
	rollback := func(e error) error {
		_, _ = conn.ExecContext(ctx, `ROLLBACK`)
		return e
	}

	err = conn.Raw(func(raw any) error {
		rawConn, ok := raw.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected raw conn %T", raw)
		}
		fa, err := duckdb.NewAppenderFromConn(rawConn, "", "files")
		if err != nil {
			return err
		}
		defer func() { _ = fa.Close() }()
		na, err := duckdb.NewAppenderFromConn(rawConn, "", "nodes")
		if err != nil {
			return err
		}
		defer func() { _ = na.Close() }()

		for _, f := range files {
			var pe any
			if f.ParseError != "" {
				pe = f.ParseError
			}
			if err := fa.AppendRow(f.ID, f.Path, f.PkgName, pe, f.Bytes); err != nil {
				return err
			}
		}
		for _, n := range nodes {
			var parent any
			if n.HasParent {
				parent = n.ParentOrdinal
			}
			if err := na.AppendRow(n.FileID, n.Ordinal, parent, n.Kind, n.NodeText, n.Pos, n.End, n.StartLine, n.StartCol, n.EndLine, n.EndCol, n.StartOffset, n.EndOffset); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return rollback(err)
	}

	if err := writeMeta(ctx, conn, fingerprint); err != nil {
		return rollback(err)
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return rollback(err)
	}
	return nil
}

func createSchema(ctx context.Context, conn *sql.Conn) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS files (file_id BIGINT PRIMARY KEY, path TEXT NOT NULL UNIQUE, pkg_name TEXT, parse_error TEXT, bytes BIGINT)`,
		`CREATE TABLE IF NOT EXISTS nodes (file_id BIGINT NOT NULL, ordinal INTEGER NOT NULL, parent_ordinal INTEGER, kind TEXT NOT NULL, node_text TEXT, pos INTEGER, "end" INTEGER, start_line INTEGER, start_col INTEGER, end_line INTEGER, end_col INTEGER, start_offset INTEGER, end_offset INTEGER, PRIMARY KEY(file_id, ordinal))`,
		`CREATE TABLE IF NOT EXISTS run_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS governance_rules (rule_id TEXT PRIMARY KEY, category TEXT NOT NULL, severity TEXT NOT NULL, description TEXT NOT NULL, query_sql TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT true, updated_unix BIGINT NOT NULL)`,
	}
	for _, stmt := range stmts {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func writeMeta(ctx context.Context, conn *sql.Conn, fingerprint string) error {
	items := map[string]string{
		"schema_version":     schemaVersion,
		"source_fingerprint": fingerprint,
		"updated_unix":       strconv.FormatInt(time.Now().Unix(), 10),
	}
	for k, v := range items {
		if _, err := conn.ExecContext(ctx, `INSERT INTO run_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, k, v); err != nil {
			return err
		}
	}
	return nil
}

func inspectDuckDB(path string) (dbState, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return dbState{}, nil
		}
		return dbState{}, fmt.Errorf("stat db: %w", err)
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return dbState{}, err
	}
	defer func() { _ = db.Close() }()
	state := dbState{Exists: true}
	if err := db.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&state.FilesCount); err != nil {
		return state, nil
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&state.NodesCount); err != nil {
		return state, nil
	}
	rows, err := db.Query(`SELECT key, value FROM run_meta`)
	if err != nil {
		return state, nil
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			continue
		}
		if k == "schema_version" {
			state.SchemaVersion = v
		}
		if k == "source_fingerprint" {
			state.SourceFingerprint = v
		}
	}
	return state, nil
}

type querySpec struct{ Name, SQL string }

func defaultQueries() []querySpec {
	return []querySpec{
		{Name: "count_nodes", SQL: `SELECT COUNT(*) FROM nodes`},
		{Name: "group_by_kind_top20", SQL: `SELECT kind, COUNT(*) AS n FROM nodes GROUP BY kind ORDER BY n DESC LIMIT 20`},
		{Name: "funcdecl_join_files", SQL: `SELECT f.path, COUNT(*) AS n FROM nodes n JOIN files f ON f.file_id=n.file_id WHERE n.kind='*ast.FuncDecl' GROUP BY f.path ORDER BY n DESC LIMIT 50`},
	}
}

func benchmarkQueries(path string, queries []querySpec, warmup, iters int) ([]QueryResult, error) {
	warmup = max(0, warmup)
	iters = max(1, iters)
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	out := make([]QueryResult, 0, len(queries))
	for _, q := range queries {
		for i := 0; i < warmup; i++ {
			if err := executeQuery(db, q.SQL); err != nil {
				return nil, err
			}
		}
		start := time.Now()
		for i := 0; i < iters; i++ {
			if err := executeQuery(db, q.SQL); err != nil {
				return nil, err
			}
		}
		out = append(out, QueryResult{Name: q.Name, Elapsed: time.Since(start)})
	}
	return out, nil
}

func executeQuery(db *sql.DB, q string) error {
	rows, err := db.Query(q)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
	}
	return rows.Err()
}

func sourceFingerprint(files []fileMeta) string {
	h := fnv.New64a()
	for _, f := range files {
		_, _ = h.Write([]byte(f.RelPath))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatInt(f.Size, 10)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatInt(f.ModUnixNano, 10)))
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("%x", h.Sum64())
}

func fileIDForPath(path string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(path))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

func cleanupDuckDB(path string) {
	_ = os.Remove(path)
	_ = os.Remove(path + ".wal")
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

# goastdb

`goastdb` indexes Go AST into DuckDB so you can run SQL-based architecture and governance checks at repository scale.

It is designed for two workflows:

1. Fast local iteration on code-smell and architecture queries.
2. Repeatable CI governance with machine-readable output.

## What it does

- Parses Go source with `go/ast` and writes node-level facts into DuckDB.
- Stores parent/child relationships and source locations for joins across files and symbols.
- Reuses an existing database when source fingerprint is unchanged.
- Runs benchmark queries in one process to amortize DB session overhead.
- Runs governance checks from SQL stored in `governance_rules`.

## Install

### Build from source

```bash
git clone <your-repo-url>
cd goastdb
go build -o bin/goastdb ./cmd/goastdb
```

### Run without installing

```bash
go run ./cmd/goastdb -repo .
```

## Quick start

```bash
# Build/reuse index and run built-in query benchmark
go run ./cmd/goastdb -repo . -duckdb ./.tmp/goastdb/ast.duckdb -mode both -reuse

# Build index only
go run ./cmd/goastdb -repo . -mode build -query-bench=false

# Query benchmark only (rebuilds if fingerprint changed)
go run ./cmd/goastdb -repo . -mode query
```

## Governance workflow

```bash
# List known rules (default rules are bootstrapped automatically)
go run ./cmd/goastdb -repo . -list-rules

# Run all enabled rules
go run ./cmd/goastdb -repo . -governance

# Run only selected rules
go run ./cmd/goastdb -repo . -governance -rules RULE_A,RULE_B
```

Rule severity must be one of: `critical`, `error`, `warning`, `info`.

## Ad hoc SQL

```bash
go run ./cmd/goastdb -repo . -mode build -reuse \
  -adhoc "SELECT kind, COUNT(*) AS n FROM nodes GROUP BY kind ORDER BY n DESC LIMIT 20"
```

## JSON output for CI

Use `-format json` for stable integration output.

```bash
go run ./cmd/goastdb -repo . -governance -format json
```

The JSON envelope includes:

- `result`: scan/build/query metrics
- `rules`: rule metadata (when using `-list-rules` or `-governance`)
- `violations`: governance results (when using `-governance`)
- `adhoc_rows`: result rows (when using `-adhoc`)

## Important flags

- `-repo`: repository root to scan
- `-subdir`: optional subdirectory under repo
- `-duckdb`: output DB path (default `./.tmp/goastdb/ast.duckdb`)
- `-mode`: `build | query | both`
- `-reuse`: reuse DB when fingerprint matches
- `-force-rebuild`: force full rebuild
- `-workers`: parse worker count
- `-max-files`: optional cap for `.go` files (`0` means all)
- `-query-bench`: run benchmark queries
- `-query-warmup`: warmup runs per benchmark query
- `-query-iters`: measured iterations per benchmark query
- `-timeout`: optional timeout (for example `2m`)
- `-format`: `text | json`

## Data model

- `files(file_id, path, pkg_name, parse_error, bytes)`
- `nodes(file_id, ordinal, parent_ordinal, kind, node_text, pos, end, start_line, start_col, end_line, end_col, start_offset, end_offset)`
- `run_meta(key, value)`
- `governance_rules(rule_id, category, severity, description, query_sql, enabled, updated_unix)`

## Production use notes

- Use one process per DB path to avoid DuckDB file lock conflicts.
- Keep DB files out of source control (`.tmp/` and `*.duckdb` are ignored).
- Prefer a persistent DB path in CI workers to maximize reuse.
- For very large repositories, set `-timeout` and tune `-workers`.

## CI and releases

- CI: `.github/workflows/ci.yml` runs `go test ./...` on push/PR.
- Releases: `.github/workflows/release.yml` builds cross-platform binaries on `v*` tags and publishes GitHub release artifacts with checksums.

Create a release by pushing a tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

# goastdb

`goastdb` indexes Go AST into DuckDB and lets you run SQL directly against your codebase.

It is optimized for day-to-day exploration: from repo root, defaults work without extra flags.

## Install

```bash
go install github.com/Yacobolo/goastdb/cmd/goastdb@latest
```

## Defaults

- Repo root: current directory (`.`)
- DB path: `<repo>/.goast/ast.db`
- Mode: query-first workflow (reuses DB, rebuilds when source changed)

## Commands

### Query

Run one or more raw SQL queries.

```bash
# single query
goastdb query "SELECT COUNT(*) AS files FROM files"

# multiple queries in one run
goastdb query \
  "SELECT COUNT(*) AS files FROM files" \
  "SELECT COUNT(*) AS nodes FROM nodes"
```

### Helper

List or run built-in helper queries.

```bash
# list helper query IDs
goastdb helper list

# run selected helper queries
goastdb helper AST_KIND_DISTRIBUTION,FUNCTIONS_PER_FILE
```

Helper IDs (overview + Go best-practice heuristics):

- `AST_KIND_DISTRIBUTION`
- `PACKAGE_FILE_COUNTS`
- `FILES_BY_NODE_COUNT`
- `FUNCTIONS_PER_FILE`
- `LARGE_FUNCTIONS_BY_LINES`
- `COMPLEX_FUNCTIONS_BY_BRANCHING`
- `SIGNATURES_WITH_MANY_FIELDS`
- `LARGE_STRUCT_TYPES`
- `LARGE_INTERFACES`
- `IMPORT_FREQUENCIES`
- `THIRD_PARTY_IMPORTS`
- `TOP_IDENTIFIERS`
- `BLANK_IDENTIFIER_USAGE`
- `PANIC_USAGE`
- `GO_ROUTINE_SPAWNS`
- `DEFER_HEAVY_FUNCTIONS`
- `INIT_FUNCTIONS`
- `TEST_FILE_NODE_DENSITY`
- `LITERAL_HEAVY_FILES`
- `PARSE_ERRORS`

## Shared flags

Both `query` and `helper` support:

- `--repo` repository root (default `.`)
- `--duckdb` DB path (default `<repo>/.goast/ast.db`)
- `--format` output format: `text|json`

## JSON output

```bash
goastdb query --format json "SELECT COUNT(*) AS n FROM nodes"
```

## Data model

- `files(file_id, path, pkg_name, parse_error, bytes)`
- `nodes(file_id, ordinal, parent_ordinal, kind, node_text, pos, end, start_line, start_col, end_line, end_col, start_offset, end_offset)`
- `run_meta(key, value)`

## Operational notes

- Use one process per DB path to avoid DuckDB lock conflicts.
- `.goast/` and DB files should be gitignored.

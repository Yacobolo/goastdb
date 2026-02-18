package explore

import (
	"fmt"
	"strings"
)

type Query struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

func DefaultQueries() []Query {
	return []Query{
		{
			ID:          "AST_KIND_DISTRIBUTION",
			Description: "Top AST node kinds by frequency",
			SQL: `
SELECT
  kind,
  COUNT(*) AS n
FROM nodes
GROUP BY kind
ORDER BY n DESC
LIMIT 50
`,
		},
		{
			ID:          "PACKAGE_FILE_COUNTS",
			Description: "Packages with the most files",
			SQL: `
SELECT
  coalesce(nullif(pkg_name, ''), '<unknown>') AS package_name,
  COUNT(*) AS file_count
FROM files
GROUP BY package_name
ORDER BY file_count DESC, package_name
LIMIT 50
`,
		},
		{
			ID:          "FILES_BY_NODE_COUNT",
			Description: "Largest files by total AST node count",
			SQL: `
SELECT
  f.path,
  COUNT(*) AS node_count
FROM nodes n
JOIN files f ON f.file_id = n.file_id
GROUP BY f.path
ORDER BY node_count DESC
LIMIT 30
`,
		},
		{
			ID:          "FUNCTIONS_PER_FILE",
			Description: "Files with the highest function declaration count",
			SQL: `
SELECT
  f.path,
  COUNT(*) AS function_count
FROM nodes n
JOIN files f ON f.file_id = n.file_id
WHERE n.kind = '*ast.FuncDecl'
GROUP BY f.path
ORDER BY function_count DESC
LIMIT 30
`,
		},
		{
			ID:          "LARGE_FUNCTIONS_BY_LINES",
			Description: "Functions with largest line span (large-function heuristic)",
			SQL: `
WITH funcs AS (
  SELECT file_id, ordinal AS func_ordinal, start_line, end_line
  FROM nodes
  WHERE kind = '*ast.FuncDecl'
),
func_names AS (
  SELECT
    file_id,
    parent_ordinal AS func_ordinal,
    node_text AS function_name,
    ROW_NUMBER() OVER (PARTITION BY file_id, parent_ordinal ORDER BY ordinal) AS rn
  FROM nodes
  WHERE kind = '*ast.Ident' AND parent_ordinal IS NOT NULL
)
SELECT
  f.path,
  coalesce(fn.function_name, '<anonymous>') AS function_name,
  funcs.start_line,
  funcs.end_line,
  (funcs.end_line - funcs.start_line + 1) AS line_span
FROM funcs
JOIN files f ON f.file_id = funcs.file_id
LEFT JOIN func_names fn ON fn.file_id = funcs.file_id AND fn.func_ordinal = funcs.func_ordinal AND fn.rn = 1
ORDER BY line_span DESC, f.path
LIMIT 50
`,
		},
		{
			ID:          "COMPLEX_FUNCTIONS_BY_BRANCHING",
			Description: "Functions with high branching/control-flow counts",
			SQL: `
WITH funcs AS (
  SELECT file_id, ordinal AS func_ordinal, start_line, end_line
  FROM nodes
  WHERE kind = '*ast.FuncDecl'
),
func_names AS (
  SELECT
    file_id,
    parent_ordinal AS func_ordinal,
    node_text AS function_name,
    ROW_NUMBER() OVER (PARTITION BY file_id, parent_ordinal ORDER BY ordinal) AS rn
  FROM nodes
  WHERE kind = '*ast.Ident' AND parent_ordinal IS NOT NULL
),
signals AS (
  SELECT
    funcs.file_id,
    funcs.func_ordinal,
    COUNT(*) FILTER (WHERE n.kind = '*ast.IfStmt') AS if_count,
    COUNT(*) FILTER (WHERE n.kind = '*ast.ForStmt') AS for_count,
    COUNT(*) FILTER (WHERE n.kind = '*ast.RangeStmt') AS range_count,
    COUNT(*) FILTER (WHERE n.kind = '*ast.SwitchStmt') AS switch_count,
    COUNT(*) FILTER (WHERE n.kind = '*ast.TypeSwitchStmt') AS type_switch_count,
    COUNT(*) FILTER (WHERE n.kind = '*ast.CaseClause') AS case_count,
    COUNT(*) FILTER (WHERE n.kind = '*ast.SelectStmt') AS select_count
  FROM funcs
  JOIN nodes n
    ON n.file_id = funcs.file_id
   AND n.start_line >= funcs.start_line
   AND n.end_line <= funcs.end_line
  GROUP BY funcs.file_id, funcs.func_ordinal
)
SELECT
  f.path,
  coalesce(fn.function_name, '<anonymous>') AS function_name,
  signals.if_count,
  signals.for_count,
  signals.range_count,
  signals.switch_count,
  signals.type_switch_count,
  signals.case_count,
  signals.select_count,
  (
    signals.if_count + signals.for_count + signals.range_count +
    signals.switch_count + signals.type_switch_count + signals.case_count + signals.select_count
  ) AS branching_score
FROM signals
JOIN files f ON f.file_id = signals.file_id
LEFT JOIN func_names fn ON fn.file_id = signals.file_id AND fn.func_ordinal = signals.func_ordinal AND fn.rn = 1
ORDER BY branching_score DESC, f.path
LIMIT 50
`,
		},
		{
			ID:          "SIGNATURES_WITH_MANY_FIELDS",
			Description: "Functions with large signatures (params + returns field count)",
			SQL: `
WITH funcs AS (
  SELECT file_id, ordinal AS func_ordinal
  FROM nodes
  WHERE kind = '*ast.FuncDecl'
),
func_names AS (
  SELECT
    file_id,
    parent_ordinal AS func_ordinal,
    node_text AS function_name,
    ROW_NUMBER() OVER (PARTITION BY file_id, parent_ordinal ORDER BY ordinal) AS rn
  FROM nodes
  WHERE kind = '*ast.Ident' AND parent_ordinal IS NOT NULL
),
func_types AS (
  SELECT file_id, parent_ordinal AS func_ordinal, ordinal AS func_type_ordinal
  FROM nodes
  WHERE kind = '*ast.FuncType' AND parent_ordinal IS NOT NULL
),
signature_fields AS (
  SELECT
    ft.file_id,
    ft.func_ordinal,
    COUNT(fd.ordinal) AS signature_field_count
  FROM func_types ft
  JOIN nodes fl
    ON fl.file_id = ft.file_id
   AND fl.parent_ordinal = ft.func_type_ordinal
   AND fl.kind = '*ast.FieldList'
  LEFT JOIN nodes fd
    ON fd.file_id = fl.file_id
   AND fd.parent_ordinal = fl.ordinal
   AND fd.kind = '*ast.Field'
  GROUP BY ft.file_id, ft.func_ordinal
)
SELECT
  f.path,
  coalesce(fn.function_name, '<anonymous>') AS function_name,
  sf.signature_field_count
FROM signature_fields sf
JOIN funcs fu ON fu.file_id = sf.file_id AND fu.func_ordinal = sf.func_ordinal
JOIN files f ON f.file_id = sf.file_id
LEFT JOIN func_names fn ON fn.file_id = sf.file_id AND fn.func_ordinal = sf.func_ordinal AND fn.rn = 1
ORDER BY sf.signature_field_count DESC, f.path
LIMIT 50
`,
		},
		{
			ID:          "LARGE_STRUCT_TYPES",
			Description: "Struct types with many fields",
			SQL: `
WITH type_names AS (
  SELECT
    ts.file_id,
    ts.ordinal AS type_spec_ordinal,
    id.node_text AS type_name
  FROM nodes ts
  JOIN nodes id
    ON id.file_id = ts.file_id
   AND id.parent_ordinal = ts.ordinal
   AND id.kind = '*ast.Ident'
  WHERE ts.kind = '*ast.TypeSpec'
),
struct_types AS (
  SELECT
    st.file_id,
    st.parent_ordinal AS type_spec_ordinal,
    st.ordinal AS struct_ordinal
  FROM nodes st
  WHERE st.kind = '*ast.StructType' AND st.parent_ordinal IS NOT NULL
),
struct_fields AS (
  SELECT
    st.file_id,
    st.type_spec_ordinal,
    COUNT(fd.ordinal) AS field_count
  FROM struct_types st
  JOIN nodes fl
    ON fl.file_id = st.file_id
   AND fl.parent_ordinal = st.struct_ordinal
   AND fl.kind = '*ast.FieldList'
  LEFT JOIN nodes fd
    ON fd.file_id = fl.file_id
   AND fd.parent_ordinal = fl.ordinal
   AND fd.kind = '*ast.Field'
  GROUP BY st.file_id, st.type_spec_ordinal
)
SELECT
  f.path,
  coalesce(tn.type_name, '<anonymous_type>') AS type_name,
  sf.field_count
FROM struct_fields sf
JOIN files f ON f.file_id = sf.file_id
LEFT JOIN type_names tn ON tn.file_id = sf.file_id AND tn.type_spec_ordinal = sf.type_spec_ordinal
ORDER BY sf.field_count DESC, f.path
LIMIT 50
`,
		},
		{
			ID:          "LARGE_INTERFACES",
			Description: "Interface types with many methods",
			SQL: `
WITH type_names AS (
  SELECT
    ts.file_id,
    ts.ordinal AS type_spec_ordinal,
    id.node_text AS type_name
  FROM nodes ts
  JOIN nodes id
    ON id.file_id = ts.file_id
   AND id.parent_ordinal = ts.ordinal
   AND id.kind = '*ast.Ident'
  WHERE ts.kind = '*ast.TypeSpec'
),
iface_types AS (
  SELECT
    it.file_id,
    it.parent_ordinal AS type_spec_ordinal,
    it.ordinal AS iface_ordinal
  FROM nodes it
  WHERE it.kind = '*ast.InterfaceType' AND it.parent_ordinal IS NOT NULL
),
iface_methods AS (
  SELECT
    it.file_id,
    it.type_spec_ordinal,
    COUNT(fd.ordinal) AS method_count
  FROM iface_types it
  JOIN nodes fl
    ON fl.file_id = it.file_id
   AND fl.parent_ordinal = it.iface_ordinal
   AND fl.kind = '*ast.FieldList'
  LEFT JOIN nodes fd
    ON fd.file_id = fl.file_id
   AND fd.parent_ordinal = fl.ordinal
   AND fd.kind = '*ast.Field'
  GROUP BY it.file_id, it.type_spec_ordinal
)
SELECT
  f.path,
  coalesce(tn.type_name, '<anonymous_type>') AS type_name,
  im.method_count
FROM iface_methods im
JOIN files f ON f.file_id = im.file_id
LEFT JOIN type_names tn ON tn.file_id = im.file_id AND tn.type_spec_ordinal = im.type_spec_ordinal
ORDER BY im.method_count DESC, f.path
LIMIT 50
`,
		},
		{
			ID:          "IMPORT_FREQUENCIES",
			Description: "Most frequently imported packages",
			SQL: `
SELECT
  replace(coalesce(n.node_text, ''), '"', '') AS import_path,
  COUNT(*) AS uses
FROM nodes n
WHERE n.kind = '*ast.ImportSpec'
GROUP BY import_path
ORDER BY uses DESC, import_path
LIMIT 50
`,
		},
		{
			ID:          "THIRD_PARTY_IMPORTS",
			Description: "Most common non-stdlib imports (heuristic)",
			SQL: `
SELECT
  import_path,
  COUNT(*) AS uses
FROM (
  SELECT replace(coalesce(n.node_text, ''), '"', '') AS import_path
  FROM nodes n
  WHERE n.kind = '*ast.ImportSpec'
) x
WHERE import_path LIKE '%.%' OR import_path LIKE 'github.com/%' OR import_path LIKE 'golang.org/%'
GROUP BY import_path
ORDER BY uses DESC, import_path
LIMIT 50
`,
		},
		{
			ID:          "TOP_IDENTIFIERS",
			Description: "Most common identifiers in the AST",
			SQL: `
SELECT
  n.node_text AS identifier,
  COUNT(*) AS uses
FROM nodes n
WHERE n.kind = '*ast.Ident'
  AND coalesce(n.node_text, '') <> ''
GROUP BY n.node_text
ORDER BY uses DESC, identifier
LIMIT 50
`,
		},
		{
			ID:          "BLANK_IDENTIFIER_USAGE",
			Description: "Files with the most blank identifier usage",
			SQL: `
SELECT
  f.path,
  COUNT(*) AS blank_identifier_uses
FROM nodes n
JOIN files f ON f.file_id = n.file_id
WHERE n.kind = '*ast.Ident' AND n.node_text = '_'
GROUP BY f.path
ORDER BY blank_identifier_uses DESC, f.path
LIMIT 50
`,
		},
		{
			ID:          "PANIC_USAGE",
			Description: "Call sites that likely invoke panic",
			SQL: `
SELECT
  f.path,
  n.start_line AS line,
  'panic' AS symbol,
  'possible panic call' AS detail
FROM nodes n
JOIN files f ON f.file_id = n.file_id
WHERE n.kind = '*ast.Ident' AND n.node_text = 'panic'
ORDER BY f.path, line
LIMIT 200
`,
		},
		{
			ID:          "GO_ROUTINE_SPAWNS",
			Description: "Files with the most goroutine spawn statements",
			SQL: `
SELECT
  f.path,
  COUNT(*) AS go_stmt_count
FROM nodes n
JOIN files f ON f.file_id = n.file_id
WHERE n.kind = '*ast.GoStmt'
GROUP BY f.path
ORDER BY go_stmt_count DESC, f.path
LIMIT 50
`,
		},
		{
			ID:          "DEFER_HEAVY_FUNCTIONS",
			Description: "Functions with many defer statements",
			SQL: `
WITH funcs AS (
  SELECT file_id, ordinal AS func_ordinal, start_line, end_line
  FROM nodes
  WHERE kind = '*ast.FuncDecl'
),
func_names AS (
  SELECT
    file_id,
    parent_ordinal AS func_ordinal,
    node_text AS function_name,
    ROW_NUMBER() OVER (PARTITION BY file_id, parent_ordinal ORDER BY ordinal) AS rn
  FROM nodes
  WHERE kind = '*ast.Ident' AND parent_ordinal IS NOT NULL
),
defer_counts AS (
  SELECT
    funcs.file_id,
    funcs.func_ordinal,
    COUNT(*) AS defer_count
  FROM funcs
  JOIN nodes n
    ON n.file_id = funcs.file_id
   AND n.start_line >= funcs.start_line
   AND n.end_line <= funcs.end_line
   AND n.kind = '*ast.DeferStmt'
  GROUP BY funcs.file_id, funcs.func_ordinal
)
SELECT
  f.path,
  coalesce(fn.function_name, '<anonymous>') AS function_name,
  dc.defer_count
FROM defer_counts dc
JOIN files f ON f.file_id = dc.file_id
LEFT JOIN func_names fn ON fn.file_id = dc.file_id AND fn.func_ordinal = dc.func_ordinal AND fn.rn = 1
ORDER BY dc.defer_count DESC, f.path
LIMIT 50
`,
		},
		{
			ID:          "INIT_FUNCTIONS",
			Description: "Locations of init functions",
			SQL: `
WITH init_funcs AS (
  SELECT
    fd.file_id,
    fd.start_line,
    id.node_text AS function_name
  FROM nodes fd
  JOIN nodes id
    ON id.file_id = fd.file_id
   AND id.parent_ordinal = fd.ordinal
   AND id.kind = '*ast.Ident'
  WHERE fd.kind = '*ast.FuncDecl' AND id.node_text = 'init'
)
SELECT
  f.path,
  init_funcs.start_line AS line,
  init_funcs.function_name
FROM init_funcs
JOIN files f ON f.file_id = init_funcs.file_id
ORDER BY f.path, line
LIMIT 200
`,
		},
		{
			ID:          "TEST_FILE_NODE_DENSITY",
			Description: "Largest test files by AST node count",
			SQL: `
SELECT
  f.path,
  COUNT(*) AS node_count
FROM nodes n
JOIN files f ON f.file_id = n.file_id
WHERE f.path LIKE '%_test.go'
GROUP BY f.path
ORDER BY node_count DESC, f.path
LIMIT 50
`,
		},
		{
			ID:          "LITERAL_HEAVY_FILES",
			Description: "Files with the most basic literals",
			SQL: `
SELECT
  f.path,
  COUNT(*) AS literal_count
FROM nodes n
JOIN files f ON f.file_id = n.file_id
WHERE n.kind = '*ast.BasicLit'
GROUP BY f.path
ORDER BY literal_count DESC, f.path
LIMIT 50
`,
		},
		{
			ID:          "PARSE_ERRORS",
			Description: "Files with parser errors",
			SQL: `
SELECT
  path,
  parse_error
FROM files
WHERE parse_error IS NOT NULL AND parse_error <> ''
ORDER BY path
LIMIT 100
`,
		},
	}
}

func SelectQueries(ids []string) ([]Query, error) {
	queries := DefaultQueries()
	if len(ids) == 0 {
		return queries, nil
	}
	byID := make(map[string]Query, len(queries))
	for _, q := range queries {
		byID[q.ID] = q
	}
	out := make([]Query, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		q, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("unknown exploration query id %q", id)
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		return queries, nil
	}
	return out, nil
}

package governance

func defaultRules() []Rule {
	// Keep defaults intentionally minimal and project-agnostic.
	// Repositories can insert stricter governance rules via the same table.
	return []Rule{
		{
			ID:          "EXAMPLE_IMPORTS_INTERNAL_ONLY",
			Category:    "example",
			Severity:    "warning",
			Description: "Example rule: list internal package imports",
			Enabled:     false,
			QuerySQL: `
SELECT
  f.path AS file_path,
  f.path AS symbol,
  ('imports ' || replace(coalesce(n.node_text, ''), '"', '')) AS detail,
  n.start_line AS line
FROM nodes n
JOIN files f ON f.file_id = n.file_id
WHERE n.kind = '*ast.ImportSpec'
  AND replace(coalesce(n.node_text, ''), '"', '') LIKE '%/internal/%'
ORDER BY f.path, n.start_line
`,
		},
	}
}

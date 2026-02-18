package explore

import "testing"

func TestSelectQueries_Default(t *testing.T) {
	t.Parallel()

	queries, err := SelectQueries(nil)
	if err != nil {
		t.Fatalf("select queries: %v", err)
	}
	if len(queries) == 0 {
		t.Fatal("expected default queries")
	}
}

func TestSelectQueries_UnknownID(t *testing.T) {
	t.Parallel()

	_, err := SelectQueries([]string{"DOES_NOT_EXIST"})
	if err == nil {
		t.Fatal("expected error for unknown query id")
	}
}

func TestDefaultQueries_UniqueAndNonEmpty(t *testing.T) {
	t.Parallel()

	queries := DefaultQueries()
	if len(queries) == 0 {
		t.Fatal("expected non-empty query library")
	}
	seen := make(map[string]struct{}, len(queries))
	for _, q := range queries {
		if q.ID == "" {
			t.Fatal("query ID must not be empty")
		}
		if q.Description == "" {
			t.Fatalf("query %s has empty description", q.ID)
		}
		if q.SQL == "" {
			t.Fatalf("query %s has empty SQL", q.ID)
		}
		if _, exists := seen[q.ID]; exists {
			t.Fatalf("duplicate query ID: %s", q.ID)
		}
		seen[q.ID] = struct{}{}
	}
}

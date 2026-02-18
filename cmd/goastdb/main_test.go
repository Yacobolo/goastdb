package main

import (
	"reflect"
	"testing"
)

func TestParseRuleIDs(t *testing.T) {
	t.Parallel()

	got := parseRuleIDs(" R1, R2, ,R1 ")
	want := []string{"R1", "R2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected parsed ids: got=%v want=%v", got, want)
	}
}

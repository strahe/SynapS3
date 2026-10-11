package repository_test

import (
	"encoding/json"
	"testing"

	"github.com/uptrace/bun"
)

type postgresExecutionPlan struct {
	NodeType            string                  `json:"Node Type"`
	RelationName        string                  `json:"Relation Name"`
	Alias               string                  `json:"Alias"`
	IndexName           string                  `json:"Index Name"`
	IndexCond           string                  `json:"Index Cond"`
	SortKeys            []string                `json:"Sort Key"`
	ActualRows          float64                 `json:"Actual Rows"`
	ActualLoops         float64                 `json:"Actual Loops"`
	RowsRemovedByFilter float64                 `json:"Rows Removed by Filter"`
	Children            []postgresExecutionPlan `json:"Plans"`
}

func explainPostgresPlan(t *testing.T, db *bun.DB, query string) postgresExecutionPlan {
	t.Helper()
	var raw string
	if err := db.NewRaw("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query).Scan(t.Context(), &raw); err != nil {
		t.Fatalf("explain repository query: %v", err)
	}
	var plans []struct {
		Plan postgresExecutionPlan `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(raw), &plans); err != nil || len(plans) != 1 {
		t.Fatalf("decode repository plan: %v, plan=%s", err, raw)
	}
	t.Logf("repository query plan:\n%s", raw)
	return plans[0].Plan
}

func planIndexConditions(plan postgresExecutionPlan, indexName string) []string {
	var conditions []string
	if plan.IndexName == indexName {
		conditions = append(conditions, plan.IndexCond)
	}
	for _, child := range plan.Children {
		conditions = append(conditions, planIndexConditions(child, indexName)...)
	}
	return conditions
}

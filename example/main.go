package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/snowmerak/sqlike-api/engine"
	"github.com/snowmerak/sqlike-api/transport"
)

func main() {
	e := engine.New()

	// --- Register SELECT resolvers ---

	e.Register("users", []string{"id", "name", "email", "active"},
		func(ctx context.Context, req engine.SourceRequest) ([]engine.Row, error) {
			all := []engine.Row{
				{"id": int64(1), "name": "alice", "email": "alice@example.com", "active": true},
				{"id": int64(2), "name": "bob", "email": "bob@example.com", "active": true},
				{"id": int64(3), "name": "charlie", "email": "charlie@example.com", "active": false},
			}
			return filterRows(all, req.Conditions), nil
		},
	)

	e.Register("posts", []string{"id", "user_id", "title", "body"},
		func(ctx context.Context, req engine.SourceRequest) ([]engine.Row, error) {
			all := []engine.Row{
				{"id": int64(1), "user_id": int64(1), "title": "Hello World", "body": "My first post"},
				{"id": int64(2), "user_id": int64(1), "title": "Go Tips", "body": "Some tips about Go"},
				{"id": int64(3), "user_id": int64(2), "title": "SQL Rocks", "body": "Why SQL is great"},
			}
			return filterRows(all, req.Conditions), nil
		},
	)

	// --- Register mutation resolver ---

	e.RegisterMutation("users", func(ctx context.Context, req engine.MutationRequest) (engine.MutationResult, error) {
		switch req.Type {
		case engine.MutationInsert:
			fmt.Printf("[INSERT] %d rows into %s: %v\n", len(req.Values), req.Table, req.Values)
		case engine.MutationUpdate:
			fmt.Printf("[UPDATE] %s SET %v WHERE %v\n", req.Table, req.Assignments, req.Conditions)
		case engine.MutationDelete:
			fmt.Printf("[DELETE] FROM %s WHERE %v\n", req.Table, req.Conditions)
		}
		return engine.MutationResult{Affected: 1}, nil
	})

	fmt.Println("sqlike-api server listening on :8080")
	fmt.Println()
	fmt.Println("Try:")
	fmt.Println(`  curl -s -X POST http://localhost:8080/query -H "Content-Type: application/json" -d "{\"query\": \"SELECT u.name AS author, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = true\"}"`)
	fmt.Println()

	if err := http.ListenAndServe(":8080", transport.Handler(e)); err != nil {
		fmt.Printf("server error: %v\n", err)
	}
}

// filterRows applies conditions to rows (simple in-memory filter for demo purposes).
func filterRows(rows []engine.Row, conds []engine.Condition) []engine.Row {
	if len(conds) == 0 {
		return rows
	}

	var result []engine.Row
	for _, row := range rows {
		if matchAll(row, conds) {
			result = append(result, row)
		}
	}
	return result
}

func matchAll(row engine.Row, conds []engine.Condition) bool {
	for _, c := range conds {
		val, ok := row[c.Column]
		if !ok {
			return false
		}
		switch c.Op {
		case engine.OpEq:
			if fmt.Sprintf("%v", val) != fmt.Sprintf("%v", c.Value) {
				return false
			}
		case engine.OpNeq:
			if fmt.Sprintf("%v", val) == fmt.Sprintf("%v", c.Value) {
				return false
			}
		case engine.OpGt:
			if fmt.Sprintf("%v", val) <= fmt.Sprintf("%v", c.Value) {
				return false
			}
		case engine.OpLike:
			pattern, ok := c.Value.(string)
			if !ok {
				return false
			}
			s, ok := val.(string)
			if !ok {
				return false
			}
			if !matchLike(s, pattern) {
				return false
			}
		}
	}
	return true
}

func matchLike(s, pattern string) bool {
	// Very basic LIKE: only supports trailing %
	if strings.HasSuffix(pattern, "%") {
		prefix := strings.TrimSuffix(pattern, "%")
		return strings.HasPrefix(strings.ToLower(s), strings.ToLower(prefix))
	}
	return strings.EqualFold(s, pattern)
}

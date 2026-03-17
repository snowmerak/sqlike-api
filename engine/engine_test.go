package engine_test

import (
	"context"
	"testing"

	"github.com/snowmerak/sqlike-api/engine"
)

func setupEngine() *engine.Engine {
	e := engine.New()

	e.Register("users", []string{"id", "name", "email", "active"}, func(ctx context.Context, req engine.SourceRequest) ([]engine.Row, error) {
		all := []engine.Row{
			{"id": int64(1), "name": "alice", "email": "alice@example.com", "active": true},
			{"id": int64(2), "name": "bob", "email": "bob@example.com", "active": true},
			{"id": int64(3), "name": "charlie", "email": "charlie@example.com", "active": false},
		}

		var result []engine.Row
		for _, row := range all {
			if matchConditions(row, req.Conditions) {
				result = append(result, row)
			}
		}
		return result, nil
	})

	e.Register("posts", []string{"id", "user_id", "title", "body"}, func(ctx context.Context, req engine.SourceRequest) ([]engine.Row, error) {
		all := []engine.Row{
			{"id": int64(1), "user_id": int64(1), "title": "Hello World", "body": "First post"},
			{"id": int64(2), "user_id": int64(1), "title": "Go Tips", "body": "Second post"},
			{"id": int64(3), "user_id": int64(2), "title": "SQL Rocks", "body": "Third post"},
		}

		var result []engine.Row
		for _, row := range all {
			if matchConditions(row, req.Conditions) {
				result = append(result, row)
			}
		}
		return result, nil
	})

	return e
}

func matchConditions(row engine.Row, conds []engine.Condition) bool {
	for _, c := range conds {
		val, ok := row[c.Column]
		if !ok {
			return false
		}
		switch c.Op {
		case engine.OpEq:
			if val != c.Value {
				return false
			}
		case engine.OpNeq:
			if val == c.Value {
				return false
			}
		}
	}
	return true
}

func TestSingleTable(t *testing.T) {
	e := setupEngine()
	ctx := context.Background()

	results, err := e.Query(ctx, "SELECT u.name, u.email FROM users u WHERE u.active = true")
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(results), results)
	}

	// Check that alice and bob are returned
	names := map[string]bool{}
	for _, row := range results {
		name, ok := row["name"].(string)
		if !ok {
			t.Fatalf("expected string name, got %T: %v", row["name"], row)
		}
		names[name] = true
	}
	if !names["alice"] || !names["bob"] {
		t.Fatalf("expected alice and bob, got %v", names)
	}
}

func TestJoin(t *testing.T) {
	e := setupEngine()
	ctx := context.Background()

	results, err := e.Query(ctx, `
		SELECT u.name AS author, p.title
		FROM users u
		JOIN posts p ON u.id = p.user_id
		WHERE u.active = true
	`)
	if err != nil {
		t.Fatal(err)
	}

	// alice has 2 posts, bob has 1 post, charlie is inactive
	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(results), results)
	}

	// Check alice's posts
	alicePosts := 0
	bobPosts := 0
	for _, row := range results {
		author, ok := row["author"].(string)
		if !ok {
			t.Fatalf("expected string author, got %T: %v", row["author"], row)
		}
		if _, ok := row["title"].(string); !ok {
			t.Fatalf("expected string title, got %T: %v", row["title"], row)
		}
		switch author {
		case "alice":
			alicePosts++
		case "bob":
			bobPosts++
		default:
			t.Fatalf("unexpected author: %s", author)
		}
	}

	if alicePosts != 2 {
		t.Fatalf("expected 2 alice posts, got %d", alicePosts)
	}
	if bobPosts != 1 {
		t.Fatalf("expected 1 bob post, got %d", bobPosts)
	}
}

func TestLimit(t *testing.T) {
	e := setupEngine()
	ctx := context.Background()

	results, err := e.Query(ctx, "SELECT u.name FROM users u LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}
}

func TestOrderBy(t *testing.T) {
	e := setupEngine()
	ctx := context.Background()

	results, err := e.Query(ctx, "SELECT u.name FROM users u ORDER BY u.name ASC")
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(results))
	}

	expected := []string{"alice", "bob", "charlie"}
	for i, row := range results {
		name := row["name"].(string)
		if name != expected[i] {
			t.Fatalf("row %d: expected %s, got %s", i, expected[i], name)
		}
	}
}

func TestSelectStar(t *testing.T) {
	e := setupEngine()
	ctx := context.Background()

	results, err := e.Query(ctx, "SELECT * FROM users u WHERE u.active = true")
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(results), results)
	}

	// Each row should have all 4 columns
	for _, row := range results {
		if len(row) != 4 {
			t.Fatalf("expected 4 columns, got %d: %v", len(row), row)
		}
	}
}

func TestAlias(t *testing.T) {
	e := setupEngine()
	ctx := context.Background()

	results, err := e.Query(ctx, "SELECT u.name AS username FROM users u LIMIT 1")
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}

	if _, ok := results[0]["username"]; !ok {
		t.Fatalf("expected 'username' key, got: %v", results[0])
	}
}

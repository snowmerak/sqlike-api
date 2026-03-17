package transport_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/snowmerak/sqlike-api/engine"
	"github.com/snowmerak/sqlike-api/transport"
)

func setupEngine() *engine.Engine {
	e := engine.New()

	e.Register("users", []string{"id", "name", "active"}, func(ctx context.Context, req engine.SourceRequest) ([]engine.Row, error) {
		all := []engine.Row{
			{"id": int64(1), "name": "alice", "active": true},
			{"id": int64(2), "name": "bob", "active": true},
			{"id": int64(3), "name": "charlie", "active": false},
		}
		var result []engine.Row
		for _, row := range all {
			match := true
			for _, c := range req.Conditions {
				if c.Op == engine.OpEq && row[c.Column] != c.Value {
					match = false
				}
			}
			if match {
				result = append(result, row)
			}
		}
		return result, nil
	})

	return e
}

func TestQueryEndpoint(t *testing.T) {
	e := setupEngine()
	handler := transport.Handler(e)

	body := `{"query": "SELECT u.name FROM users u WHERE u.active = true"}`
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp transport.QueryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}

	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(resp.Data))
	}
}

func TestQueryEndpointEmptyQuery(t *testing.T) {
	e := setupEngine()
	handler := transport.Handler(e)

	body := `{"query": ""}`
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestQueryEndpointBadSQL(t *testing.T) {
	e := setupEngine()
	handler := transport.Handler(e)

	body := `{"query": "NOT VALID SQL"}`
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var resp transport.QueryResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Error == "" {
		t.Fatal("expected error message")
	}
}

package transport

import (
	"encoding/json"
	"net/http"

	"github.com/snowmerak/sqlike-api/engine"
)

// QueryRequest is the JSON request body for POST /query.
type QueryRequest struct {
	Query string `json:"query"`
}

// QueryResponse is the JSON response body.
type QueryResponse struct {
	Data     []engine.Row          `json:"data,omitempty"`
	Mutation *engine.MutationResult `json:"mutation,omitempty"`
	Error    string                `json:"error,omitempty"`
}

// Handler creates an http.Handler for the sqlike engine.
func Handler(e *engine.Engine) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(QueryResponse{Error: "invalid request body: " + err.Error()})
			return
		}

		if req.Query == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(QueryResponse{Error: "query is required"})
			return
		}

		qr, err := e.Query(r.Context(), req.Query)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(QueryResponse{Error: err.Error()})
			return
		}

		resp := QueryResponse{}
		if qr.Rows != nil {
			resp.Data = qr.Rows
		} else if len(qr.Rows) == 0 && qr.Mutation == nil {
			resp.Data = []engine.Row{}
		}
		if qr.Mutation != nil {
			resp.Mutation = qr.Mutation
		}

		json.NewEncoder(w).Encode(resp)
	})

	return mux
}

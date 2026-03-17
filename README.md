# sqlike-api

A SQL-powered API query engine. Use SQL as the query language for your APIs — a practical alternative to GraphQL where clients send SQL queries and the server resolves them through user-defined resolvers.

## Concept

```
Client                               Server
  │                                    │
  │  POST /query                       │
  │  {"query": "SELECT ..."}  ──────▶  │  Parse SQL → Distribute conditions → Call resolvers → Join → Project
  │                                    │
  │  {"data": [...]}          ◀──────  │  Flat JSON response
```

- **The engine** parses SQL, distributes WHERE conditions per source, calls resolvers in parallel, and joins results in-memory.
- **Resolvers** are user-implemented. Fetch data from anywhere — databases, APIs, caches, or combine them all.
- SQL is a language every developer already knows. **Zero learning curve.**

## Install

```bash
go get github.com/snowmerak/sqlike-api
```

## Quick Start

```go
e := engine.New()

e.Register("users", []string{"id", "name", "email", "active"},
    func(ctx context.Context, req engine.SourceRequest) ([]engine.Row, error) {
        // req.Columns:    columns needed from this source
        // req.Conditions: WHERE conditions for this source only
        return fetchUsersFromDB(ctx, req)
    },
)

e.Register("posts", []string{"id", "user_id", "title"},
    func(ctx context.Context, req engine.SourceRequest) ([]engine.Row, error) {
        return fetchPostsFromAPI(ctx, req)
    },
)

result, _ := e.Query(ctx, `
    SELECT u.name AS author, p.title
    FROM users u
    JOIN posts p ON u.id = p.user_id
    WHERE u.active = true
`)
// result.Rows = [{"author":"alice","title":"Hello World"}, ...]
```

### Mutations

```go
e.RegisterMutation("users", func(ctx context.Context, req engine.MutationRequest) (engine.MutationResult, error) {
    switch req.Type {
    case engine.MutationInsert:
        // req.Columns = ["id", "name"]
        // req.Values  = [{"id": 1, "name": "alice"}, ...]
    case engine.MutationUpdate:
        // req.Assignments = {"name": "alice2", "active": false}
        // req.Conditions  = [{Column: "id", Op: "=", Value: 1}]
    case engine.MutationDelete:
        // req.Conditions = [{Column: "id", Op: "=", Value: 1}]
    }
    return engine.MutationResult{Affected: n}, nil
})
```

### HTTP Server

```go
http.ListenAndServe(":8080", transport.Handler(e))
```

See [`example/main.go`](example/main.go) for a complete working example.

## HTTP API

```bash
# SELECT with JOIN
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT u.name AS author, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = true"}'
# → {"data": [{"author":"alice","title":"Hello World"}, ...]}

# INSERT
curl -X POST http://localhost:8080/query \
  -d '{"query": "INSERT INTO users (id, name) VALUES (4, '\''dave'\'')"}'
# → {"mutation": {"affected": 1}}

# UPDATE
curl -X POST http://localhost:8080/query \
  -d '{"query": "UPDATE users SET name = '\''alice2'\'' WHERE id = 1"}'
# → {"mutation": {"affected": 1}}

# DELETE
curl -X POST http://localhost:8080/query \
  -d '{"query": "DELETE FROM users WHERE id = 1"}'
# → {"mutation": {"affected": 1}}
```

## Supported SQL

### SELECT

```sql
SELECT u.name, u.email AS mail, p.title, u.*, *
FROM users u
JOIN posts p ON u.id = p.user_id
WHERE u.active = true
  AND u.age > 20
  AND u.name LIKE 'a%'
  AND u.id IN (1, 2, 3)
  AND u.email IS NOT NULL
  AND u.score BETWEEN 50 AND 100
ORDER BY u.name ASC, u.id DESC
LIMIT 10
OFFSET 20
```

### INSERT

```sql
INSERT INTO users (id, name, email)
VALUES (1, 'alice', 'alice@example.com'),
       (2, 'bob', 'bob@example.com')
```

### UPDATE

```sql
UPDATE users SET name = 'alice2', active = false WHERE id = 1
```

### DELETE

```sql
DELETE FROM users WHERE id = 1 AND active = false
```

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                       Engine                         │
│                                                      │
│  SQL ──▶ Lexer ──▶ Parser ──▶ AST                   │
│                                 │                    │
│                    ┌────────────┴───────────┐        │
│                    │ SELECT        Mutation │        │
│                    ▼                  ▼     │        │
│              Distribute         MutationReq │        │
│              conditions               │     │        │
│             ┌──┴──┐                   ▼     │        │
│             ▼     ▼           MutationResolver       │
│        resolver resolver              │     │        │
│        (parallel)                     │     │        │
│             │     │                   │     │        │
│             ▼     ▼                   │     │        │
│         Hash Join (in-memory)         │     │        │
│             │                         │     │        │
│             ▼                         ▼     │        │
│      Projection + Sort       MutationResult │        │
│             │                         │     │        │
│             ▼                         ▼     │        │
│       QueryResult{Rows}  QueryResult{Mutation}       │
└──────────────────────────────────────────────────────┘
```

**SELECT pipeline:**
1. Distribute WHERE conditions to each source's resolver
2. Call all resolvers **in parallel**
3. Hash join results using JOIN ON conditions
4. Apply SELECT projection with aliases
5. Apply ORDER BY → LIMIT/OFFSET

## Type Reference

### Condition

| Field | Type | Description |
|-------|------|-------------|
| `Column` | `string` | Column name |
| `Op` | `Op` | `OpEq`, `OpNeq`, `OpGt`, `OpGte`, `OpLt`, `OpLte`, `OpLike`, `OpIn` |
| `Value` | `any` | Comparison value |

### MutationRequest

| Field | INSERT | UPDATE | DELETE |
|-------|--------|--------|--------|
| `Type` | `MutationInsert` | `MutationUpdate` | `MutationDelete` |
| `Table` | ✓ | ✓ | ✓ |
| `Columns` | ✓ | — | — |
| `Values` | ✓ `[]Row` | — | — |
| `Assignments` | — | ✓ `Row` | — |
| `Conditions` | — | ✓ | ✓ |

## License

MIT

package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/snowmerak/sqlike-api/sql/ast"
	"github.com/snowmerak/sqlike-api/sql/parser"
)

// Row represents a single result row.
type Row map[string]any

// Op represents a comparison operator in a filter condition.
type Op string

const (
	OpEq   Op = "="
	OpNeq  Op = "!="
	OpGt   Op = ">"
	OpGte  Op = ">="
	OpLt   Op = "<"
	OpLte  Op = "<="
	OpLike Op = "LIKE"
	OpIn   Op = "IN"
)

// Condition represents a single filter condition for a source.
type Condition struct {
	Column string
	Op     Op
	Value  any
}

// SourceRequest is passed to a resolver for a single source.
type SourceRequest struct {
	Columns    []string    // columns needed from this source
	Conditions []Condition // WHERE conditions for this source
}

// Resolver is a function that fetches data for a single source (SELECT).
type Resolver func(ctx context.Context, req SourceRequest) ([]Row, error)

// MutationType represents the type of mutation.
type MutationType string

const (
	MutationInsert MutationType = "INSERT"
	MutationUpdate MutationType = "UPDATE"
	MutationDelete MutationType = "DELETE"
)

// MutationRequest is passed to a mutation resolver.
type MutationRequest struct {
	Type        MutationType
	Table       string
	Columns     []string       // INSERT: column names
	Values      []Row          // INSERT: rows to insert (each Row maps column→value)
	Assignments Row            // UPDATE: column→new_value
	Conditions  []Condition    // UPDATE/DELETE: WHERE conditions
}

// MutationResult is returned from a mutation resolver.
type MutationResult struct {
	Affected int64 `json:"affected"`
}

// MutationResolver is a function that handles INSERT/UPDATE/DELETE.
type MutationResolver func(ctx context.Context, req MutationRequest) (MutationResult, error)

type sourceEntry struct {
	columns  []string
	resolver Resolver
}

// Engine is the sqlike query engine.
type Engine struct {
	sources   map[string]sourceEntry
	mutations map[string]MutationResolver
}

// New creates a new Engine.
func New() *Engine {
	return &Engine{
		sources:   make(map[string]sourceEntry),
		mutations: make(map[string]MutationResolver),
	}
}

// Register registers a virtual table with its columns and resolver.
func (e *Engine) Register(name string, columns []string, resolver Resolver) {
	e.sources[name] = sourceEntry{
		columns:  columns,
		resolver: resolver,
	}
}

// RegisterMutation registers a mutation resolver for a table.
func (e *Engine) RegisterMutation(name string, resolver MutationResolver) {
	e.mutations[name] = resolver
}

// QueryResult holds the result of any query type.
type QueryResult struct {
	Rows     []Row           `json:"rows,omitempty"`
	Mutation *MutationResult `json:"mutation,omitempty"`
}

// Query parses and executes a SQL query.
func (e *Engine) Query(ctx context.Context, sql string) (*QueryResult, error) {
	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	switch s := stmt.(type) {
	case *ast.SelectStatement:
		rows, err := e.executeSelect(ctx, s)
		if err != nil {
			return nil, err
		}
		return &QueryResult{Rows: rows}, nil
	case *ast.InsertStatement:
		result, err := e.executeInsert(ctx, s)
		if err != nil {
			return nil, err
		}
		return &QueryResult{Mutation: &result}, nil
	case *ast.UpdateStatement:
		result, err := e.executeUpdate(ctx, s)
		if err != nil {
			return nil, err
		}
		return &QueryResult{Mutation: &result}, nil
	case *ast.DeleteStatement:
		result, err := e.executeDelete(ctx, s)
		if err != nil {
			return nil, err
		}
		return &QueryResult{Mutation: &result}, nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// executeSelect runs a parsed SELECT statement.
func (e *Engine) executeSelect(ctx context.Context, stmt *ast.SelectStatement) ([]Row, error) {
	// 1. Collect all sources (FROM + JOINs)
	sources := e.collectSources(stmt)

	// 2. Resolve aliases → source names
	aliasMap := make(map[string]string) // alias → source name
	for alias, name := range sources {
		if _, ok := e.sources[name]; !ok {
			return nil, fmt.Errorf("unknown source: %s", name)
		}
		aliasMap[alias] = name
	}

	// 3. Compute needed columns per source (SELECT + JOIN ON + WHERE)
	neededCols := e.computeNeededColumns(stmt, aliasMap)

	// 4. Distribute WHERE conditions per source
	condMap := e.distributeConditions(stmt.Where, aliasMap)

	// 5. Build SourceRequest per source and call resolvers in parallel
	type sourceResult struct {
		alias string
		rows  []Row
		err   error
	}

	var wg sync.WaitGroup
	resultCh := make(chan sourceResult, len(sources))

	for alias, name := range sources {
		wg.Add(1)
		go func(alias, name string) {
			defer wg.Done()
			entry := e.sources[name]
			req := SourceRequest{
				Columns:    neededCols[alias],
				Conditions: condMap[alias],
			}
			rows, err := entry.resolver(ctx, req)
			resultCh <- sourceResult{alias: alias, rows: rows, err: err}
		}(alias, name)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	resultMap := make(map[string][]Row)
	for res := range resultCh {
		if res.err != nil {
			return nil, fmt.Errorf("resolver for %s failed: %w", res.alias, res.err)
		}
		resultMap[res.alias] = res.rows
	}

	// 6. Join results
	joined, err := e.joinResults(stmt, resultMap, aliasMap)
	if err != nil {
		return nil, err
	}

	// 7. Apply remaining WHERE filters (cross-table conditions)
	joined, err = e.applyPostFilters(stmt.Where, joined, aliasMap)
	if err != nil {
		return nil, err
	}

	// 8. Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		e.applyOrderBy(joined, stmt.OrderBy)
	}

	// 9. Apply LIMIT and OFFSET
	joined = e.applyLimitOffset(joined, stmt.Limit, stmt.Offset)

	// 10. Project — select only requested columns with aliases
	return e.project(joined, stmt, aliasMap)
}

// collectSources returns alias → source name for FROM + JOINs.
func (e *Engine) collectSources(stmt *ast.SelectStatement) map[string]string {
	sources := make(map[string]string)

	alias := stmt.From.Alias
	if alias == "" {
		alias = stmt.From.Name
	}
	sources[alias] = stmt.From.Name

	for _, j := range stmt.Joins {
		a := j.Table.Alias
		if a == "" {
			a = j.Table.Name
		}
		sources[a] = j.Table.Name
	}

	return sources
}

// computeNeededColumns figures out what columns each source needs to provide.
func (e *Engine) computeNeededColumns(stmt *ast.SelectStatement, aliasMap map[string]string) map[string][]string {
	needed := make(map[string]map[string]bool)
	for alias := range aliasMap {
		needed[alias] = make(map[string]bool)
	}

	// From SELECT
	for _, col := range stmt.Columns {
		if col.Column == "*" {
			if col.Table != "" {
				// t.* → all columns from that source
				if name, ok := aliasMap[col.Table]; ok {
					for _, c := range e.sources[name].columns {
						needed[col.Table][c] = true
					}
				}
			} else {
				// * → all columns from all sources
				for alias, name := range aliasMap {
					for _, c := range e.sources[name].columns {
						needed[alias][c] = true
					}
				}
			}
		} else if col.Table != "" {
			needed[col.Table][col.Column] = true
		}
	}

	// From JOIN ON conditions
	for _, j := range stmt.Joins {
		e.collectExprColumns(j.On, needed)
	}

	// From WHERE
	if stmt.Where != nil {
		e.collectExprColumns(stmt.Where, needed)
	}

	// From ORDER BY
	for _, ob := range stmt.OrderBy {
		if ob.Table != "" {
			needed[ob.Table][ob.Column] = true
		}
	}

	// Convert to slices
	result := make(map[string][]string)
	for alias, cols := range needed {
		for col := range cols {
			result[alias] = append(result[alias], col)
		}
	}
	return result
}

func (e *Engine) collectExprColumns(expr ast.Expression, needed map[string]map[string]bool) {
	if expr == nil {
		return
	}
	switch ex := expr.(type) {
	case ast.BinaryExpr:
		e.collectExprColumns(ex.Left, needed)
		e.collectExprColumns(ex.Right, needed)
	case ast.ColumnRef:
		if ex.Table != "" {
			if _, ok := needed[ex.Table]; ok {
				needed[ex.Table][ex.Column] = true
			}
		}
	case ast.InExpr:
		if ex.Column.Table != "" {
			if _, ok := needed[ex.Column.Table]; ok {
				needed[ex.Column.Table][ex.Column.Column] = true
			}
		}
	case ast.BetweenExpr:
		if ex.Column.Table != "" {
			if _, ok := needed[ex.Column.Table]; ok {
				needed[ex.Column.Table][ex.Column.Column] = true
			}
		}
	case ast.IsNullExpr:
		if ex.Column.Table != "" {
			if _, ok := needed[ex.Column.Table]; ok {
				needed[ex.Column.Table][ex.Column.Column] = true
			}
		}
	case ast.NotExpr:
		e.collectExprColumns(ex.Expr, needed)
	}
}

// distributeConditions distributes WHERE conditions to their respective sources.
// Only simple conditions (alias.col OP literal) that are AND-connected are distributed.
func (e *Engine) distributeConditions(where ast.Expression, aliasMap map[string]string) map[string][]Condition {
	condMap := make(map[string][]Condition)
	if where == nil {
		return condMap
	}

	// Flatten AND-connected conditions
	var flatConditions []ast.Expression
	e.flattenAnd(where, &flatConditions)

	for _, expr := range flatConditions {
		cond, alias, ok := e.tryExtractCondition(expr)
		if ok {
			if _, exists := aliasMap[alias]; exists {
				condMap[alias] = append(condMap[alias], cond)
			}
		}
	}

	return condMap
}

func (e *Engine) flattenAnd(expr ast.Expression, out *[]ast.Expression) {
	if be, ok := expr.(ast.BinaryExpr); ok && be.Op == ast.OpAnd {
		e.flattenAnd(be.Left, out)
		e.flattenAnd(be.Right, out)
	} else {
		*out = append(*out, expr)
	}
}

// tryExtractCondition tries to convert an AST expression into a simple Condition.
// Returns the condition, the source alias, and whether it succeeded.
func (e *Engine) tryExtractCondition(expr ast.Expression) (Condition, string, bool) {
	switch ex := expr.(type) {
	case ast.BinaryExpr:
		col, ok := ex.Left.(ast.ColumnRef)
		if !ok || col.Table == "" {
			return Condition{}, "", false
		}
		lit, ok := ex.Right.(ast.Literal)
		if !ok {
			return Condition{}, "", false
		}
		op, ok := mapASTOp(ex.Op)
		if !ok {
			return Condition{}, "", false
		}
		return Condition{Column: col.Column, Op: op, Value: lit.Value}, col.Table, true

	case ast.InExpr:
		if ex.Not || ex.Column.Table == "" {
			return Condition{}, "", false
		}
		values := make([]any, 0, len(ex.Values))
		for _, v := range ex.Values {
			lit, ok := v.(ast.Literal)
			if !ok {
				return Condition{}, "", false
			}
			values = append(values, lit.Value)
		}
		return Condition{Column: ex.Column.Column, Op: OpIn, Value: values}, ex.Column.Table, true

	case ast.IsNullExpr:
		if ex.Column.Table == "" {
			return Condition{}, "", false
		}
		op := OpEq
		if ex.Not {
			op = OpNeq
		}
		return Condition{Column: ex.Column.Column, Op: op, Value: nil}, ex.Column.Table, true
	}

	return Condition{}, "", false
}

func mapASTOp(op ast.Operator) (Op, bool) {
	switch op {
	case ast.OpEq:
		return OpEq, true
	case ast.OpNeq:
		return OpNeq, true
	case ast.OpGt:
		return OpGt, true
	case ast.OpGte:
		return OpGte, true
	case ast.OpLt:
		return OpLt, true
	case ast.OpLte:
		return OpLte, true
	case ast.OpLike:
		return OpLike, true
	default:
		return "", false
	}
}

// joinResults performs in-memory hash join across all sources.
func (e *Engine) joinResults(stmt *ast.SelectStatement, resultMap map[string][]Row, aliasMap map[string]string) ([]Row, error) {
	// Start with FROM rows — prefix each key with alias
	fromAlias := stmt.From.Alias
	if fromAlias == "" {
		fromAlias = stmt.From.Name
	}

	joined := prefixRows(resultMap[fromAlias], fromAlias)

	// Apply each JOIN
	for _, j := range stmt.Joins {
		rightAlias := j.Table.Alias
		if rightAlias == "" {
			rightAlias = j.Table.Name
		}
		rightRows := prefixRows(resultMap[rightAlias], rightAlias)

		leftCol, rightCol, err := extractJoinColumns(j.On)
		if err != nil {
			return nil, fmt.Errorf("invalid JOIN ON condition: %w", err)
		}

		leftKey := leftCol.Table + "." + leftCol.Column
		rightKey := rightCol.Table + "." + rightCol.Column

		joined = hashJoin(joined, rightRows, leftKey, rightKey)
	}

	return joined, nil
}

// prefixRows adds "alias." prefix to all keys in rows.
func prefixRows(rows []Row, alias string) []Row {
	result := make([]Row, len(rows))
	for i, row := range rows {
		prefixed := make(Row, len(row))
		for k, v := range row {
			prefixed[alias+"."+k] = v
		}
		result[i] = prefixed
	}
	return result
}

// extractJoinColumns extracts left and right column refs from a simple equi-join ON condition.
func extractJoinColumns(expr ast.Expression) (ast.ColumnRef, ast.ColumnRef, error) {
	be, ok := expr.(ast.BinaryExpr)
	if !ok || be.Op != ast.OpEq {
		return ast.ColumnRef{}, ast.ColumnRef{}, fmt.Errorf("JOIN ON must be a simple equality (a.col = b.col)")
	}
	left, ok := be.Left.(ast.ColumnRef)
	if !ok {
		return ast.ColumnRef{}, ast.ColumnRef{}, fmt.Errorf("JOIN ON left side must be a column reference")
	}
	right, ok := be.Right.(ast.ColumnRef)
	if !ok {
		return ast.ColumnRef{}, ast.ColumnRef{}, fmt.Errorf("JOIN ON right side must be a column reference")
	}
	return left, right, nil
}

// hashJoin performs an in-memory INNER hash join.
func hashJoin(left, right []Row, leftKey, rightKey string) []Row {
	// Build hash map from right side
	rightMap := make(map[any][]Row)
	for _, row := range right {
		key := row[rightKey]
		rightMap[key] = append(rightMap[key], row)
	}

	// Probe with left side
	var result []Row
	for _, lRow := range left {
		lVal := lRow[leftKey]
		if matchingRows, ok := rightMap[lVal]; ok {
			for _, rRow := range matchingRows {
				merged := make(Row, len(lRow)+len(rRow))
				for k, v := range lRow {
					merged[k] = v
				}
				for k, v := range rRow {
					merged[k] = v
				}
				result = append(result, merged)
			}
		}
	}

	return result
}

// applyPostFilters applies cross-table conditions that couldn't be pushed down.
func (e *Engine) applyPostFilters(where ast.Expression, rows []Row, aliasMap map[string]string) ([]Row, error) {
	if where == nil || len(rows) == 0 {
		return rows, nil
	}

	// Check if there are any cross-table conditions
	var crossConditions []ast.Expression
	var flatConditions []ast.Expression
	e.flattenAnd(where, &flatConditions)

	for _, expr := range flatConditions {
		if _, _, ok := e.tryExtractCondition(expr); !ok {
			// This condition wasn't pushed down — it's cross-table or complex
			crossConditions = append(crossConditions, expr)
		}
	}

	if len(crossConditions) == 0 {
		return rows, nil
	}

	var result []Row
	for _, row := range rows {
		match := true
		for _, cond := range crossConditions {
			if !e.evalExpr(cond, row) {
				match = false
				break
			}
		}
		if match {
			result = append(result, row)
		}
	}
	return result, nil
}

// evalExpr evaluates an expression against a joined row.
func (e *Engine) evalExpr(expr ast.Expression, row Row) bool {
	switch ex := expr.(type) {
	case ast.BinaryExpr:
		switch ex.Op {
		case ast.OpAnd:
			return e.evalExpr(ex.Left, row) && e.evalExpr(ex.Right, row)
		case ast.OpOr:
			return e.evalExpr(ex.Left, row) || e.evalExpr(ex.Right, row)
		default:
			lVal := e.evalValue(ex.Left, row)
			rVal := e.evalValue(ex.Right, row)
			return compareValues(lVal, rVal, ex.Op)
		}
	case ast.NotExpr:
		return !e.evalExpr(ex.Expr, row)
	default:
		return true
	}
}

func (e *Engine) evalValue(expr ast.Expression, row Row) any {
	switch ex := expr.(type) {
	case ast.ColumnRef:
		key := ex.Table + "." + ex.Column
		return row[key]
	case ast.Literal:
		return ex.Value
	default:
		return nil
	}
}

func compareValues(left, right any, op ast.Operator) bool {
	switch op {
	case ast.OpEq:
		return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right)
	case ast.OpNeq:
		return fmt.Sprintf("%v", left) != fmt.Sprintf("%v", right)
	default:
		// For simplicity, compare as strings for now
		l := fmt.Sprintf("%v", left)
		r := fmt.Sprintf("%v", right)
		switch op {
		case ast.OpLt:
			return l < r
		case ast.OpLte:
			return l <= r
		case ast.OpGt:
			return l > r
		case ast.OpGte:
			return l >= r
		}
	}
	return false
}

// applyOrderBy sorts rows by ORDER BY clauses.
func (e *Engine) applyOrderBy(rows []Row, orderBy []ast.OrderByClause) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, ob := range orderBy {
			key := ob.Column
			if ob.Table != "" {
				key = ob.Table + "." + ob.Column
			}
			vi := fmt.Sprintf("%v", rows[i][key])
			vj := fmt.Sprintf("%v", rows[j][key])
			if vi == vj {
				continue
			}
			if ob.Desc {
				return vi > vj
			}
			return vi < vj
		}
		return false
	})
}

// applyLimitOffset applies LIMIT and OFFSET to rows.
func (e *Engine) applyLimitOffset(rows []Row, limit, offset *int) []Row {
	start := 0
	if offset != nil {
		start = *offset
		if start > len(rows) {
			return nil
		}
		rows = rows[start:]
	}
	if limit != nil {
		if *limit < len(rows) {
			rows = rows[:*limit]
		}
	}
	return rows
}

// project applies SELECT column projection and aliasing.
func (e *Engine) project(rows []Row, stmt *ast.SelectStatement, aliasMap map[string]string) ([]Row, error) {
	// Expand * and t.*
	outputCols := e.expandSelectColumns(stmt, aliasMap)

	result := make([]Row, len(rows))
	for i, row := range rows {
		projected := make(Row, len(outputCols))
		for _, col := range outputCols {
			key := col.Table + "." + col.Column
			outputKey := col.Column
			if col.Alias != "" {
				outputKey = col.Alias
			}
			projected[outputKey] = row[key]
		}
		result[i] = projected
	}
	return result, nil
}

// expandSelectColumns expands * and t.* into concrete columns.
func (e *Engine) expandSelectColumns(stmt *ast.SelectStatement, aliasMap map[string]string) []ast.SelectColumn {
	var expanded []ast.SelectColumn

	for _, col := range stmt.Columns {
		if col.Column == "*" && col.Table == "" {
			// * → all columns from all sources
			for alias, name := range aliasMap {
				for _, c := range e.sources[name].columns {
					expanded = append(expanded, ast.SelectColumn{Table: alias, Column: c})
				}
			}
		} else if col.Column == "*" && col.Table != "" {
			// t.* → all columns from that source
			if name, ok := aliasMap[col.Table]; ok {
				for _, c := range e.sources[name].columns {
					expanded = append(expanded, ast.SelectColumn{Table: col.Table, Column: c})
				}
			}
		} else {
			expanded = append(expanded, col)
		}
	}

	return expanded
}

// executeInsert builds a MutationRequest from an INSERT statement and calls the mutation resolver.
func (e *Engine) executeInsert(ctx context.Context, stmt *ast.InsertStatement) (MutationResult, error) {
	resolver, ok := e.mutations[stmt.Table]
	if !ok {
		return MutationResult{}, fmt.Errorf("no mutation resolver for table: %s", stmt.Table)
	}

	rows := make([]Row, len(stmt.Values))
	for i, vals := range stmt.Values {
		row := make(Row, len(stmt.Columns))
		for j, col := range stmt.Columns {
			lit, ok := vals[j].(ast.Literal)
			if !ok {
				return MutationResult{}, fmt.Errorf("INSERT value must be a literal")
			}
			row[col] = lit.Value
		}
		rows[i] = row
	}

	return resolver(ctx, MutationRequest{
		Type:    MutationInsert,
		Table:   stmt.Table,
		Columns: stmt.Columns,
		Values:  rows,
	})
}

// executeUpdate builds a MutationRequest from an UPDATE statement and calls the mutation resolver.
func (e *Engine) executeUpdate(ctx context.Context, stmt *ast.UpdateStatement) (MutationResult, error) {
	resolver, ok := e.mutations[stmt.Table]
	if !ok {
		return MutationResult{}, fmt.Errorf("no mutation resolver for table: %s", stmt.Table)
	}

	assignments := make(Row, len(stmt.Assignments))
	for _, a := range stmt.Assignments {
		lit, ok := a.Value.(ast.Literal)
		if !ok {
			return MutationResult{}, fmt.Errorf("UPDATE SET value must be a literal")
		}
		assignments[a.Column] = lit.Value
	}

	conditions := e.extractMutationConditions(stmt.Where)

	return resolver(ctx, MutationRequest{
		Type:        MutationUpdate,
		Table:       stmt.Table,
		Assignments: assignments,
		Conditions:  conditions,
	})
}

// executeDelete builds a MutationRequest from a DELETE statement and calls the mutation resolver.
func (e *Engine) executeDelete(ctx context.Context, stmt *ast.DeleteStatement) (MutationResult, error) {
	resolver, ok := e.mutations[stmt.Table]
	if !ok {
		return MutationResult{}, fmt.Errorf("no mutation resolver for table: %s", stmt.Table)
	}

	conditions := e.extractMutationConditions(stmt.Where)

	return resolver(ctx, MutationRequest{
		Type:       MutationDelete,
		Table:      stmt.Table,
		Conditions: conditions,
	})
}

// extractMutationConditions extracts flat conditions from a WHERE clause for single-table mutations.
// Handles both qualified (table.col) and unqualified (col) column references.
func (e *Engine) extractMutationConditions(where ast.Expression) []Condition {
	if where == nil {
		return nil
	}

	var flat []ast.Expression
	e.flattenAnd(where, &flat)

	var conditions []Condition
	for _, expr := range flat {
		if cond, ok := e.tryExtractMutationCondition(expr); ok {
			conditions = append(conditions, cond)
		}
	}
	return conditions
}

func (e *Engine) tryExtractMutationCondition(expr ast.Expression) (Condition, bool) {
	switch ex := expr.(type) {
	case ast.BinaryExpr:
		col, ok := ex.Left.(ast.ColumnRef)
		if !ok {
			return Condition{}, false
		}
		lit, ok := ex.Right.(ast.Literal)
		if !ok {
			return Condition{}, false
		}
		op, ok := mapASTOp(ex.Op)
		if !ok {
			return Condition{}, false
		}
		return Condition{Column: col.Column, Op: op, Value: lit.Value}, true

	case ast.InExpr:
		if ex.Not {
			return Condition{}, false
		}
		values := make([]any, 0, len(ex.Values))
		for _, v := range ex.Values {
			lit, ok := v.(ast.Literal)
			if !ok {
				return Condition{}, false
			}
			values = append(values, lit.Value)
		}
		return Condition{Column: ex.Column.Column, Op: OpIn, Value: values}, true

	case ast.IsNullExpr:
		op := OpEq
		if ex.Not {
			op = OpNeq
		}
		return Condition{Column: ex.Column.Column, Op: op, Value: nil}, true
	}

	return Condition{}, false
}

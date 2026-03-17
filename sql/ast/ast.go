package ast

// SelectStatement represents a parsed SELECT query.
type SelectStatement struct {
	Columns []SelectColumn
	From    TableRef
	Joins   []JoinClause
	Where   Expression
	OrderBy []OrderByClause
	Limit   *int
	Offset  *int
}

// SelectColumn represents a single column in the SELECT clause.
type SelectColumn struct {
	Table  string // alias or table name (e.g., "u"), empty if unqualified
	Column string // column name or "*"
	Alias  string // AS alias, empty if none
}

// TableRef represents a table reference with an optional alias.
type TableRef struct {
	Name  string // table name: "users"
	Alias string // alias: "u", empty if none
}

// JoinType represents the type of JOIN.
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
)

// JoinClause represents a JOIN in the FROM clause.
type JoinClause struct {
	Type  JoinType
	Table TableRef
	On    Expression
}

// OrderByClause represents a single ORDER BY item.
type OrderByClause struct {
	Table  string // alias or table name, empty if unqualified
	Column string
	Desc   bool
}

// Expression is the interface for all expression nodes in WHERE/ON clauses.
type Expression interface {
	exprNode()
}

// BinaryExpr represents a binary expression: left op right.
type BinaryExpr struct {
	Left  Expression
	Op    Operator
	Right Expression
}

func (BinaryExpr) exprNode() {}

// ColumnRef represents a column reference (e.g., u.name).
type ColumnRef struct {
	Table  string // alias or table name, empty if unqualified
	Column string
}

func (ColumnRef) exprNode() {}

// Literal represents a literal value.
type Literal struct {
	Value any
}

func (Literal) exprNode() {}

// InExpr represents a column IN (values...) expression.
type InExpr struct {
	Column ColumnRef
	Values []Expression
	Not    bool
}

func (InExpr) exprNode() {}

// BetweenExpr represents a column BETWEEN low AND high expression.
type BetweenExpr struct {
	Column ColumnRef
	Low    Expression
	High   Expression
	Not    bool
}

func (BetweenExpr) exprNode() {}

// IsNullExpr represents a column IS [NOT] NULL expression.
type IsNullExpr struct {
	Column ColumnRef
	Not    bool
}

func (IsNullExpr) exprNode() {}

// NotExpr represents a NOT expression.
type NotExpr struct {
	Expr Expression
}

func (NotExpr) exprNode() {}

// Operator represents a binary operator.
type Operator int

const (
	OpEq   Operator = iota // =
	OpNeq                  // != or <>
	OpLt                   // <
	OpLte                  // <=
	OpGt                   // >
	OpGte                  // >=
	OpLike                 // LIKE
	OpAnd                  // AND
	OpOr                   // OR
)

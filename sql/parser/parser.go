package parser

import (
	"fmt"
	"strconv"

	"github.com/snowmerak/sqlike-api/sql/ast"
	"github.com/snowmerak/sqlike-api/sql/lexer"
	"github.com/snowmerak/sqlike-api/sql/token"
)

// Parser parses a SQL token stream into an AST.
type Parser struct {
	l      *lexer.Lexer
	tokens []token.Token
	pos    int
	errors []string
}

// New creates a new Parser from the given SQL string.
func New(sql string) *Parser {
	l := lexer.New(sql)
	return &Parser{
		l:      l,
		tokens: l.Tokenize(),
		pos:    0,
	}
}

// Parse parses a SELECT statement and returns the AST.
func (p *Parser) Parse() (*ast.SelectStatement, error) {
	stmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	if p.cur().Type != token.EOF {
		return nil, fmt.Errorf("unexpected token %q at position %d, expected EOF", p.cur().Literal, p.cur().Pos)
	}
	return stmt, nil
}

func (p *Parser) cur() token.Token {
	if p.pos >= len(p.tokens) {
		return token.Token{Type: token.EOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) peek() token.Token {
	if p.pos+1 >= len(p.tokens) {
		return token.Token{Type: token.EOF}
	}
	return p.tokens[p.pos+1]
}

func (p *Parser) advance() token.Token {
	tok := p.cur()
	p.pos++
	return tok
}

func (p *Parser) expect(t token.Type) (token.Token, error) {
	tok := p.cur()
	if tok.Type != t {
		return tok, fmt.Errorf("expected %d, got %q (%d) at position %d", t, tok.Literal, tok.Type, tok.Pos)
	}
	p.advance()
	return tok, nil
}

// parseSelect parses: SELECT columns FROM table [JOIN ...] [WHERE ...] [ORDER BY ...] [LIMIT ...] [OFFSET ...]
func (p *Parser) parseSelect() (*ast.SelectStatement, error) {
	if _, err := p.expect(token.Select); err != nil {
		return nil, fmt.Errorf("expected SELECT: %w", err)
	}

	columns, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(token.From); err != nil {
		return nil, fmt.Errorf("expected FROM: %w", err)
	}

	from, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}

	joins, err := p.parseJoins()
	if err != nil {
		return nil, err
	}

	var where ast.Expression
	if p.cur().Type == token.Where {
		p.advance()
		where, err = p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("parsing WHERE: %w", err)
		}
	}

	orderBy, err := p.parseOrderBy()
	if err != nil {
		return nil, err
	}

	limit, err := p.parseLimit()
	if err != nil {
		return nil, err
	}

	offset, err := p.parseOffset()
	if err != nil {
		return nil, err
	}

	return &ast.SelectStatement{
		Columns: columns,
		From:    from,
		Joins:   joins,
		Where:   where,
		OrderBy: orderBy,
		Limit:   limit,
		Offset:  offset,
	}, nil
}

// parseSelectColumns parses: col1, col2, t.col3 AS alias, *
func (p *Parser) parseSelectColumns() ([]ast.SelectColumn, error) {
	var cols []ast.SelectColumn

	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)

		if p.cur().Type != token.Comma {
			break
		}
		p.advance() // consume comma
	}

	return cols, nil
}

func (p *Parser) parseSelectColumn() (ast.SelectColumn, error) {
	var col ast.SelectColumn

	if p.cur().Type == token.Star {
		col.Column = "*"
		p.advance()
		return col, nil
	}

	// ident or ident.ident
	name, err := p.expect(token.Ident)
	if err != nil {
		return col, fmt.Errorf("expected column name: %w", err)
	}

	if p.cur().Type == token.Dot {
		p.advance() // consume dot
		col.Table = name.Literal
		if p.cur().Type == token.Star {
			col.Column = "*"
			p.advance()
		} else {
			colName, err := p.expect(token.Ident)
			if err != nil {
				return col, fmt.Errorf("expected column name after dot: %w", err)
			}
			col.Column = colName.Literal
		}
	} else {
		col.Column = name.Literal
	}

	// optional AS alias
	if p.cur().Type == token.As {
		p.advance()
		alias, err := p.expect(token.Ident)
		if err != nil {
			return col, fmt.Errorf("expected alias after AS: %w", err)
		}
		col.Alias = alias.Literal
	} else if p.cur().Type == token.Ident && p.cur().Type != token.From {
		// implicit alias (without AS keyword)
		// only if next token is an identifier and not a keyword
		col.Alias = p.cur().Literal
		p.advance()
	}

	return col, nil
}

// parseTableRef parses: table_name [alias] or table_name AS alias
func (p *Parser) parseTableRef() (ast.TableRef, error) {
	var ref ast.TableRef

	name, err := p.expect(token.Ident)
	if err != nil {
		return ref, fmt.Errorf("expected table name: %w", err)
	}
	ref.Name = name.Literal

	if p.cur().Type == token.As {
		p.advance()
		alias, err := p.expect(token.Ident)
		if err != nil {
			return ref, fmt.Errorf("expected alias after AS: %w", err)
		}
		ref.Alias = alias.Literal
	} else if p.cur().Type == token.Ident {
		ref.Alias = p.cur().Literal
		p.advance()
	}

	return ref, nil
}

// parseJoins parses: [INNER] JOIN table ON expr [, ...]
func (p *Parser) parseJoins() ([]ast.JoinClause, error) {
	var joins []ast.JoinClause

	for {
		joinType := ast.InnerJoin

		if p.cur().Type == token.Inner {
			p.advance()
			if _, err := p.expect(token.Join); err != nil {
				return nil, fmt.Errorf("expected JOIN after INNER: %w", err)
			}
		} else if p.cur().Type == token.Left {
			joinType = ast.LeftJoin
			p.advance()
			if _, err := p.expect(token.Join); err != nil {
				return nil, fmt.Errorf("expected JOIN after LEFT: %w", err)
			}
		} else if p.cur().Type == token.Join {
			p.advance()
		} else {
			break
		}

		table, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}

		if _, err := p.expect(token.On); err != nil {
			return nil, fmt.Errorf("expected ON: %w", err)
		}

		on, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("parsing ON condition: %w", err)
		}

		joins = append(joins, ast.JoinClause{
			Type:  joinType,
			Table: table,
			On:    on,
		})
	}

	return joins, nil
}

// parseExpression parses expressions with operator precedence:
// OR < AND < comparison (=, !=, <, >, <=, >=, LIKE, IN, IS, BETWEEN)
func (p *Parser) parseExpression() (ast.Expression, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (ast.Expression, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.cur().Type == token.Or {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = ast.BinaryExpr{Left: left, Op: ast.OpOr, Right: right}
	}

	return left, nil
}

func (p *Parser) parseAnd() (ast.Expression, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	for p.cur().Type == token.And {
		p.advance()
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = ast.BinaryExpr{Left: left, Op: ast.OpAnd, Right: right}
	}

	return left, nil
}

func (p *Parser) parseComparison() (ast.Expression, error) {
	// Handle NOT prefix
	if p.cur().Type == token.Not {
		p.advance()
		expr, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		return ast.NotExpr{Expr: expr}, nil
	}

	// Handle parenthesized expression
	if p.cur().Type == token.LParen {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(token.RParen); err != nil {
			return nil, fmt.Errorf("expected closing parenthesis: %w", err)
		}
		return expr, nil
	}

	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	// Check for IS [NOT] NULL
	if p.cur().Type == token.Is {
		p.advance()
		not := false
		if p.cur().Type == token.Not {
			not = true
			p.advance()
		}
		if _, err := p.expect(token.Null); err != nil {
			return nil, fmt.Errorf("expected NULL after IS: %w", err)
		}
		colRef, ok := left.(ast.ColumnRef)
		if !ok {
			return nil, fmt.Errorf("IS NULL requires a column reference")
		}
		return ast.IsNullExpr{Column: colRef, Not: not}, nil
	}

	// Check for [NOT] IN (...)
	if p.cur().Type == token.In || (p.cur().Type == token.Not && p.peek().Type == token.In) {
		not := false
		if p.cur().Type == token.Not {
			not = true
			p.advance()
		}
		p.advance() // consume IN
		colRef, ok := left.(ast.ColumnRef)
		if !ok {
			return nil, fmt.Errorf("IN requires a column reference")
		}
		values, err := p.parseInValues()
		if err != nil {
			return nil, err
		}
		return ast.InExpr{Column: colRef, Values: values, Not: not}, nil
	}

	// Check for [NOT] BETWEEN ... AND ...
	if p.cur().Type == token.Between || (p.cur().Type == token.Not && p.peek().Type == token.Between) {
		not := false
		if p.cur().Type == token.Not {
			not = true
			p.advance()
		}
		p.advance() // consume BETWEEN
		colRef, ok := left.(ast.ColumnRef)
		if !ok {
			return nil, fmt.Errorf("BETWEEN requires a column reference")
		}
		low, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(token.And); err != nil {
			return nil, fmt.Errorf("expected AND in BETWEEN: %w", err)
		}
		high, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return ast.BetweenExpr{Column: colRef, Low: low, High: high, Not: not}, nil
	}

	// Check for comparison operator
	op, ok := p.matchComparisonOp()
	if !ok {
		return left, nil
	}
	p.advance()

	right, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	return ast.BinaryExpr{Left: left, Op: op, Right: right}, nil
}

func (p *Parser) matchComparisonOp() (ast.Operator, bool) {
	switch p.cur().Type {
	case token.Eq:
		return ast.OpEq, true
	case token.Neq:
		return ast.OpNeq, true
	case token.Lt:
		return ast.OpLt, true
	case token.Lte:
		return ast.OpLte, true
	case token.Gt:
		return ast.OpGt, true
	case token.Gte:
		return ast.OpGte, true
	case token.Like:
		return ast.OpLike, true
	default:
		return 0, false
	}
}

func (p *Parser) parseInValues() ([]ast.Expression, error) {
	if _, err := p.expect(token.LParen); err != nil {
		return nil, fmt.Errorf("expected '(' after IN: %w", err)
	}

	var values []ast.Expression
	for {
		val, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		values = append(values, val)

		if p.cur().Type != token.Comma {
			break
		}
		p.advance()
	}

	if _, err := p.expect(token.RParen); err != nil {
		return nil, fmt.Errorf("expected ')' after IN values: %w", err)
	}

	return values, nil
}

// parsePrimary parses: column_ref, literal, or parenthesized expression
func (p *Parser) parsePrimary() (ast.Expression, error) {
	switch p.cur().Type {
	case token.Ident:
		name := p.advance()
		if p.cur().Type == token.Dot {
			p.advance()
			col, err := p.expect(token.Ident)
			if err != nil {
				return nil, fmt.Errorf("expected column name after dot: %w", err)
			}
			return ast.ColumnRef{Table: name.Literal, Column: col.Literal}, nil
		}
		return ast.ColumnRef{Column: name.Literal}, nil

	case token.Int:
		tok := p.advance()
		val, err := strconv.ParseInt(tok.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q: %w", tok.Literal, err)
		}
		return ast.Literal{Value: val}, nil

	case token.Float:
		tok := p.advance()
		val, err := strconv.ParseFloat(tok.Literal, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float %q: %w", tok.Literal, err)
		}
		return ast.Literal{Value: val}, nil

	case token.String:
		tok := p.advance()
		return ast.Literal{Value: tok.Literal}, nil

	case token.True:
		p.advance()
		return ast.Literal{Value: true}, nil

	case token.False:
		p.advance()
		return ast.Literal{Value: false}, nil

	case token.Null:
		p.advance()
		return ast.Literal{Value: nil}, nil

	case token.LParen:
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(token.RParen); err != nil {
			return nil, fmt.Errorf("expected ')': %w", err)
		}
		return expr, nil

	default:
		return nil, fmt.Errorf("unexpected token %q (%d) at position %d", p.cur().Literal, p.cur().Type, p.cur().Pos)
	}
}

// parseOrderBy parses: ORDER BY col [ASC|DESC] [, ...]
func (p *Parser) parseOrderBy() ([]ast.OrderByClause, error) {
	if p.cur().Type != token.Order {
		return nil, nil
	}
	p.advance()
	if _, err := p.expect(token.By); err != nil {
		return nil, fmt.Errorf("expected BY after ORDER: %w", err)
	}

	var clauses []ast.OrderByClause
	for {
		var clause ast.OrderByClause
		name, err := p.expect(token.Ident)
		if err != nil {
			return nil, fmt.Errorf("expected column in ORDER BY: %w", err)
		}

		if p.cur().Type == token.Dot {
			p.advance()
			col, err := p.expect(token.Ident)
			if err != nil {
				return nil, fmt.Errorf("expected column name after dot: %w", err)
			}
			clause.Table = name.Literal
			clause.Column = col.Literal
		} else {
			clause.Column = name.Literal
		}

		if p.cur().Type == token.Desc {
			clause.Desc = true
			p.advance()
		} else if p.cur().Type == token.Asc {
			p.advance()
		}

		clauses = append(clauses, clause)

		if p.cur().Type != token.Comma {
			break
		}
		p.advance()
	}

	return clauses, nil
}

// parseLimit parses: LIMIT n
func (p *Parser) parseLimit() (*int, error) {
	if p.cur().Type != token.Limit {
		return nil, nil
	}
	p.advance()

	tok, err := p.expect(token.Int)
	if err != nil {
		return nil, fmt.Errorf("expected integer after LIMIT: %w", err)
	}

	val, err := strconv.Atoi(tok.Literal)
	if err != nil {
		return nil, fmt.Errorf("invalid LIMIT value: %w", err)
	}

	return &val, nil
}

// parseOffset parses: OFFSET n
func (p *Parser) parseOffset() (*int, error) {
	if p.cur().Type != token.Offset {
		return nil, nil
	}
	p.advance()

	tok, err := p.expect(token.Int)
	if err != nil {
		return nil, fmt.Errorf("expected integer after OFFSET: %w", err)
	}

	val, err := strconv.Atoi(tok.Literal)
	if err != nil {
		return nil, fmt.Errorf("invalid OFFSET value: %w", err)
	}

	return &val, nil
}

package token

// Type represents the type of a SQL token.
type Type int

const (
	// Special
	Illegal Type = iota
	EOF

	// Literals
	Ident  // column names, table names, aliases
	Int    // 42
	Float  // 3.14
	String // 'hello'

	// Operators
	Eq    // =
	Neq   // != or <>
	Lt    // <
	Lte   // <=
	Gt    // >
	Gte   // >=
	Comma // ,
	Dot   // .
	Star  // *
	LParen // (
	RParen // )

	// Keywords
	Select
	From
	Where
	And
	Or
	Not
	As
	On
	Join
	Inner
	Left
	Order
	By
	Asc
	Desc
	Limit
	Offset
	Like
	In
	Is
	Null
	True
	False
	Between
)

// Token represents a single token with its type, literal value, and position.
type Token struct {
	Type    Type
	Literal string
	Pos     int // byte offset in input
}

var keywords = map[string]Type{
	"SELECT":  Select,
	"FROM":    From,
	"WHERE":   Where,
	"AND":     And,
	"OR":      Or,
	"NOT":     Not,
	"AS":      As,
	"ON":      On,
	"JOIN":    Join,
	"INNER":   Inner,
	"LEFT":    Left,
	"ORDER":   Order,
	"BY":      By,
	"ASC":     Asc,
	"DESC":    Desc,
	"LIMIT":   Limit,
	"OFFSET":  Offset,
	"LIKE":    Like,
	"IN":      In,
	"IS":      Is,
	"NULL":    Null,
	"TRUE":    True,
	"FALSE":   False,
	"BETWEEN": Between,
}

// LookupIdent returns the keyword Type for the given identifier,
// or Ident if it is not a keyword. Case-insensitive.
func LookupIdent(ident string) Type {
	upper := toUpper(ident)
	if tok, ok := keywords[upper]; ok {
		return tok
	}
	return Ident
}

func toUpper(s string) string {
	b := make([]byte, len(s))
	for i := range s {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

package lexer

import (
	"fmt"

	"github.com/snowmerak/sqlike-api/sql/token"
)

// Lexer tokenizes a SQL input string.
type Lexer struct {
	input   string
	pos     int  // current position in input
	readPos int  // next position to read
	ch      byte // current character
}

// New creates a new Lexer for the given input.
func New(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
}

func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() token.Token {
	l.skipWhitespace()

	var tok token.Token
	tok.Pos = l.pos

	switch l.ch {
	case '=':
		tok = l.newToken(token.Eq, "=")
	case ',':
		tok = l.newToken(token.Comma, ",")
	case '.':
		tok = l.newToken(token.Dot, ".")
	case '*':
		tok = l.newToken(token.Star, "*")
	case '(':
		tok = l.newToken(token.LParen, "(")
	case ')':
		tok = l.newToken(token.RParen, ")")
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok = l.newToken(token.Lte, "<=")
		} else if l.peekChar() == '>' {
			l.readChar()
			tok = l.newToken(token.Neq, "<>")
		} else {
			tok = l.newToken(token.Lt, "<")
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = l.newToken(token.Gte, ">=")
		} else {
			tok = l.newToken(token.Gt, ">")
		}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = l.newToken(token.Neq, "!=")
		} else {
			tok = l.newToken(token.Illegal, string(l.ch))
		}
	case '\'':
		str, err := l.readString()
		if err != nil {
			tok = token.Token{Type: token.Illegal, Literal: err.Error(), Pos: l.pos}
			return tok
		}
		tok = token.Token{Type: token.String, Literal: str, Pos: l.pos}
		return tok
	case 0:
		tok = token.Token{Type: token.EOF, Literal: "", Pos: l.pos}
		return tok
	default:
		if isLetter(l.ch) {
			pos := l.pos
			ident := l.readIdentifier()
			typ := token.LookupIdent(ident)
			return token.Token{Type: typ, Literal: ident, Pos: pos}
		} else if isDigit(l.ch) {
			return l.readNumber()
		}
		tok = l.newToken(token.Illegal, string(l.ch))
	}

	l.readChar()
	return tok
}

// Tokenize returns all tokens from the input.
func (l *Lexer) Tokenize() []token.Token {
	var tokens []token.Token
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == token.EOF {
			break
		}
	}
	return tokens
}

func (l *Lexer) newToken(typ token.Type, literal string) token.Token {
	return token.Token{Type: typ, Literal: literal, Pos: l.pos}
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) readIdentifier() string {
	start := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[start:l.pos]
}

func (l *Lexer) readNumber() token.Token {
	pos := l.pos
	start := l.pos
	isFloat := false

	for isDigit(l.ch) {
		l.readChar()
	}
	if l.ch == '.' && isDigit(l.peekChar()) {
		isFloat = true
		l.readChar() // consume '.'
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	literal := l.input[start:l.pos]
	if isFloat {
		return token.Token{Type: token.Float, Literal: literal, Pos: pos}
	}
	return token.Token{Type: token.Int, Literal: literal, Pos: pos}
}

func (l *Lexer) readString() (string, error) {
	l.readChar() // skip opening quote
	start := l.pos

	for {
		if l.ch == 0 {
			return "", fmt.Errorf("unterminated string literal")
		}
		if l.ch == '\'' {
			if l.peekChar() == '\'' {
				// escaped quote ''
				l.readChar()
				l.readChar()
				continue
			}
			break
		}
		l.readChar()
	}

	str := l.input[start:l.pos]
	l.readChar() // skip closing quote
	return str, nil
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

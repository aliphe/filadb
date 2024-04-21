package lexer

import (
	"slices"
	"strings"
)

type TokenType string

const (
	TokenTypeIllegal    TokenType = "ILLEGAL"
	TokenTypeWhitespace TokenType = "WHITESPACE"

	// SQL keywords
	TokenTypeSelect TokenType = "SELECT"
	TokenTypeInsert TokenType = "INSERT"
	TokenTypeFrom   TokenType = "FROM"
	TokenTypeWhere  TokenType = "WHERE"

	// users, id, etc.
	TokenTypeLiteral TokenType = "LITERAL"

	TokenTypeNewLine     TokenType = "\n"
	TokenTypeComma       TokenType = ","
	TokenTypeSemiColumn  TokenType = ";"
	TokenTypeQuote       TokenType = "'"
	TokenTypeDoubleQuote TokenType = "\""

	TokenTypeEqual TokenType = "="
	TokenTypeAbove TokenType = ">"
	TokenTypeBelow TokenType = "<"
)

type Token struct {
	Type  TokenType
	Value string
}

func NewToken(t TokenType, val string) *Token {
	return &Token{
		Type:  t,
		Value: val,
	}
}

// Matcher returns true and the matched substring if a match is found in the provided string
type Matcher func(string) (bool, *Token)

var matchers = []Matcher{
	// Whitespace
	func(s string) (bool, *Token) {
		var whitespaces = []rune{' ', '\t'}

		if slices.Contains(whitespaces, rune(s[0])) {
			return true, NewToken(TokenTypeWhitespace, s[0:1])
		}
		return false, nil
	},
	// String matchers
	func(s string) (bool, *Token) {
		for _, tok := range []TokenType{
			TokenTypeSelect, TokenTypeInsert, TokenTypeFrom,
			TokenTypeWhere, TokenTypeNewLine, TokenTypeComma,
			TokenTypeSemiColumn, TokenTypeQuote, TokenTypeDoubleQuote,
			TokenTypeEqual, TokenTypeAbove, TokenTypeBelow,
		} {
			_, ok := strings.CutPrefix(strings.ToLower(s), strings.ToLower(string(tok)))
			if ok {
				return true, NewToken(tok, strings.Clone(s[:len(tok)]))
			}
		}
		return false, nil
	},
	// Literal
	func(s string) (bool, *Token) {
		var match string
		for _, c := range s {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '*' {
				match += string(c)
			} else {
				break
			}
		}
		if match != "" {
			return true, NewToken(TokenTypeLiteral, match)
		}
		return false, nil
	},

	// Illegal
	// Note: this one needs to be last, at it will always match and matchers are computed in order
	func(s string) (bool, *Token) {
		return true, NewToken(TokenTypeIllegal, s[0:1])
	},
}

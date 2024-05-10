package lexer

import (
	"slices"
	"strconv"
	"strings"
)

type Kind string

const (
	KindAny        Kind = ""
	KindIllegal    Kind = "ILLEGAL"
	KindWhitespace      = "WHITESPACE"

	// SQL keywords
	KindSelect Kind = "SELECT"
	KindInsert Kind = "INSERT"
	KindInto   Kind = "INTO"
	KindValues Kind = "VALUES"
	KindFrom   Kind = "FROM"
	KindWhere  Kind = "WHERE"
	KindAnd    Kind = "AND"
	KindCreate Kind = "CREATE"
	KindTable  Kind = "TABLE"

	// SQL types
	KindNumber Kind = "NUMBER"
	KindText   Kind = "TEXT"

	// users, id, etc.
	KindIdentifier    Kind = "IDENTIFIER"
	KindStringLiteral Kind = "STRING_LITERAL"
	KindNumberLiteral Kind = "NUMBER_LITERAL"

	KindNewLine    Kind = "\n"
	KindComma      Kind = ","
	KindSemiColumn Kind = ";"
	KindEOF        Kind = ""
	KindOpenParen  Kind = "("
	KindCloseParen Kind = ")"

	KindEqual Kind = "="
	KindAbove Kind = ">"
	KindBelow Kind = "<"
)

type Token struct {
	Kind     Kind
	Value    interface{}
	Len      int
	Position int
}

func NewToken(t Kind, val interface{}, len int) *Token {
	return &Token{
		Kind:  t,
		Value: val,
		Len:   len,
	}
}

// Matcher returns true and the matched substring if a match is found in the provided string
type Matcher func(string) (bool, *Token)

var matchers = []Matcher{
	// Whitespace
	func(s string) (bool, *Token) {
		var whitespaces = []rune{' ', '\t', '\n'}

		if slices.Contains(whitespaces, rune(s[0])) {
			return true, NewToken(KindWhitespace, s[0:1], 1)
		}
		return false, nil
	},
	// String matchers
	func(s string) (bool, *Token) {
		for _, tok := range []Kind{
			KindSelect, KindInsert, KindFrom, KindWhere, KindAnd, KindComma, KindSemiColumn,
			KindEqual, KindAbove, KindBelow, KindInto, KindOpenParen, KindCloseParen,
			KindValues, KindTable, KindCreate, KindText, KindNumber,
		} {
			_, ok := strings.CutPrefix(strings.ToLower(s), strings.ToLower(string(tok)))
			if ok {
				return true, NewToken(tok, strings.Clone(s[:len(tok)]), len(tok))
			}
		}
		return false, nil
	},
	// Identifier
	func(s string) (bool, *Token) {
		var match string
		for _, c := range s {
			if (c >= 'a' && c <= 'z') ||
				(c >= 'A' && c <= 'Z') ||
				c == '_' || c == '*' {
				match += string(c)
			} else {
				break
			}
		}
		if match != "" {
			return true, NewToken(KindIdentifier, match, len(match))
		}
		return false, nil
	},
	// String literal
	func(s string) (bool, *Token) {
		if s[0] != '\'' {
			return false, nil
		}
		var i = 1
		for ; i < len(s) && s[i] != '\''; i++ {
		}
		if i == len(s) {
			return false, nil
		}

		return true, NewToken(KindStringLiteral, s[1:i], i+1)
	},
	// Number literal
	func(s string) (bool, *Token) {
		var i = 0
		for ; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
		}
		n, _ := strconv.Atoi(s[:i])
		return i > 0, NewToken(KindNumberLiteral, n, i)
	},

	// Illegal
	// Note: this one needs to be last, at it will always match and matchers are computed in order
	func(s string) (bool, *Token) {
		return true, NewToken(KindIllegal, s[0:1], 1)
	},
}

package lexer

import (
	"slices"
	"strings"
)

type Kind string

const (
	KindIllegal    Kind = "ILLEGAL"
	KindWhitespace Kind = "WHITESPACE"

	// SQL keywords
	KindSelect Kind = "SELECT"
	KindInsert Kind = "INSERT"
	KindInto   Kind = "INTO"
	KindValues Kind = "VALUES"
	KindFrom   Kind = "FROM"
	KindWhere  Kind = "WHERE"
	KindAnd    Kind = "AND"

	// users, id, etc.
	KindLiteral Kind = "LITERAL"

	KindNewLine     Kind = "\n"
	KindComma       Kind = ","
	KindSemiColumn  Kind = ";"
	KindQuote       Kind = "'"
	KindDoubleQuote Kind = "\""
	KindEOF         Kind = ""
	KindOpenParen   Kind = "("
	KindCloseParen  Kind = ")"

	KindEqual Kind = "="
	KindAbove Kind = ">"
	KindBelow Kind = "<"
)

type Token struct {
	Kind     Kind
	Value    string
	Position int
}

func NewToken(t Kind, val string) *Token {
	return &Token{
		Kind:  t,
		Value: val,
	}
}

// Matcher returns true and the matched substring if a match is found in the provided string
type Matcher func(string) (bool, *Token)

var matchers = []Matcher{
	// Whitespace
	func(s string) (bool, *Token) {
		var whitespaces = []rune{' ', '\t', '\n'}

		if slices.Contains(whitespaces, rune(s[0])) {
			return true, NewToken(KindWhitespace, s[0:1])
		}
		return false, nil
	},
	// String matchers
	func(s string) (bool, *Token) {
		for _, tok := range []Kind{
			KindSelect, KindInsert, KindFrom,
			KindWhere, KindAnd, KindComma,
			KindSemiColumn, KindQuote, KindDoubleQuote,
			KindEqual, KindAbove, KindBelow,
			KindInto, KindOpenParen, KindCloseParen,
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
			if (c >= 'a' && c <= 'z') ||
				(c >= 'A' && c <= 'Z') ||
				(c >= '0' && c <= '9') ||
				c == '_' || c == '*' {
				match += string(c)
			} else {
				break
			}
		}
		if match != "" {
			return true, NewToken(KindLiteral, match)
		}
		return false, nil
	},

	// Illegal
	// Note: this one needs to be last, at it will always match and matchers are computed in order
	func(s string) (bool, *Token) {
		return true, NewToken(KindIllegal, s[0:1])
	},
}

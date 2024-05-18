package lexer

import (
	"errors"
	"strings"
)

type Lexer struct {
	input  string
	cursor int
}

func Tokenize(expr string) ([]*Token, error) {
	var tokens []*Token
	s := strings.Clone(expr)
	var pos int
	matches := make([]*Token, 0, len(matchers))

	for pos < len(s) {
		for _, match := range matchers {
			ok, tok := match(s[pos:])
			if ok {
				matches = append(matches, tok)
			}
		}
		if len(matches) > 0 {
			match := biggest(matches)
			match.Position = pos
			pos += match.Len
			if match.Kind != KindWhitespace {
				tokens = append(tokens, match)
			}
		} else {
			return nil, errors.New("invalid expression")
		}
		matches = matches[:0]
	}
	return tokens, nil
}

func biggest(tokens []*Token) *Token {
	max := tokens[0]
	for _, t := range tokens {
		if t.Len > max.Len {
			max = t
		}
	}
	return max
}

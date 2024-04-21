package lexer

import "strings"

type Lexer struct {
	input  string
	cursor int
}

func Tokenize(str string) []*Token {
	var tokens []*Token
	s := strings.Clone(str)

	for len(s) > 0 {
		for _, match := range matchers {
			ok, tok := match(s)
			if ok {
				tokens = append(tokens, tok)
				s = s[len(tok.Value):]
				break
			}
		}
	}
	return tokens
}

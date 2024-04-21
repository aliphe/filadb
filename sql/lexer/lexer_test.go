package lexer

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Test_NextToken(t *testing.T) {
	tests := []struct {
		given string
		want  []*Token
	}{
		{
			given: "SELECT *\tfrom users;",
			want: []*Token{
				{
					Type:  TokenTypeSelect,
					Value: "SELECT",
				},
				{
					Type:  TokenTypeWhitespace,
					Value: " ",
				},
				{
					Type:  TokenTypeLiteral,
					Value: "*",
				},
				{
					Type:  TokenTypeWhitespace,
					Value: "\t",
				},
				{
					Type:  TokenTypeFrom,
					Value: "from",
				},
				{
					Type:  TokenTypeWhitespace,
					Value: " ",
				},
				{
					Type:  TokenTypeLiteral,
					Value: "users",
				},
				{
					Type:  TokenTypeSemiColumn,
					Value: ";",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.given, func(t *testing.T) {
			got := Tokenize(tc.given)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("Tokenize() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

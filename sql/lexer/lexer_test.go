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
					Kind:     KindSelect,
					Value:    "SELECT",
					Position: 0,
				},
				{
					Kind:     KindWhitespace,
					Value:    " ",
					Position: 6,
				},
				{
					Kind:     KindLiteral,
					Value:    "*",
					Position: 7,
				},
				{
					Kind:     KindWhitespace,
					Value:    "\t",
					Position: 8,
				},
				{
					Kind:     KindFrom,
					Value:    "from",
					Position: 9,
				},
				{
					Kind:     KindWhitespace,
					Value:    " ",
					Position: 13,
				},
				{
					Kind:     KindLiteral,
					Value:    "users",
					Position: 14,
				},
				{
					Kind:     KindSemiColumn,
					Value:    ";",
					Position: 19,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.given, func(t *testing.T) {
			got, _ := Tokenize(tc.given)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("Tokenize() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

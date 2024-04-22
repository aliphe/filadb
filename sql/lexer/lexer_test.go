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
					Kind:  KindSelect,
					Value: "SELECT",
				},
				{
					Kind:  KindWhitespace,
					Value: " ",
				},
				{
					Kind:  KindLiteral,
					Value: "*",
				},
				{
					Kind:  KindWhitespace,
					Value: "\t",
				},
				{
					Kind:  KindFrom,
					Value: "from",
				},
				{
					Kind:  KindWhitespace,
					Value: " ",
				},
				{
					Kind:  KindLiteral,
					Value: "users",
				},
				{
					Kind:  KindSemiColumn,
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

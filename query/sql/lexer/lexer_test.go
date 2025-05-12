package lexer

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func Test_NextToken(t *testing.T) {
	tests := []struct {
		given string
		want  []*Token
	}{
		{
			given: `SELECT * from users where name = 'alif' and id IN ('1', '2');`,
			want: []*Token{
				{Kind: KindSelect, Value: "SELECT"},
				{Kind: KindIdentifier, Value: "*"},
				{Kind: KindFrom, Value: "from"},
				{Kind: KindIdentifier, Value: "users"},
				{Kind: KindWhere, Value: "where"},
				{Kind: KindIdentifier, Value: "name"},
				{Kind: KindEqual, Value: "="},
				{Kind: KindStringLiteral, Value: "alif"},
				{Kind: KindAnd, Value: "and"},
				{Kind: KindIdentifier, Value: "id"},
				{Kind: KindIn, Value: "IN"},
				{Kind: KindOpenParen, Value: "("},
				{Kind: KindStringLiteral, Value: "1"},
				{Kind: KindComma, Value: ","},
				{Kind: KindStringLiteral, Value: "2"},
				{Kind: KindCloseParen, Value: ")"},
				{Kind: KindSemiColumn, Value: ";"},
			},
		},
		{
			given: `SELECT id, name, * from users limit 10;`,
			want: []*Token{
				{Kind: KindSelect, Value: "SELECT"},
				{Kind: KindIdentifier, Value: "id"},
				{Kind: KindComma, Value: ","},
				{Kind: KindIdentifier, Value: "name"},
				{Kind: KindComma, Value: ","},
				{Kind: KindIdentifier, Value: "*"},
				{Kind: KindFrom, Value: "from"},
				{Kind: KindIdentifier, Value: "users"},
				{Kind: KindLimit, Value: "limit"},
				{Kind: KindNumberLiteral, Value: int32(10)},
				{Kind: KindSemiColumn, Value: ";"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.given, func(t *testing.T) {
			got, _ := Tokenize(tc.given)
			g := make([]*Token, 0, len(got))
			for _, got := range got {
				g = append(g, got)
			}
			if diff := cmp.Diff(tc.want, g, cmpopts.IgnoreFields(Token{}, "Position", "Len")); diff != "" {
				t.Fatalf("Tokenize() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

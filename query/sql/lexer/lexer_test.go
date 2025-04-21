package lexer

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Test_NextToken(t *testing.T) {
	tests := []struct {
		given string
		want  []Kind
	}{
		{
			given: `SELECT * from users where name = 'alif' and id IN ('1', '2');`,
			want: []Kind{
				KindSelect,
				KindIdentifier,
				KindFrom,
				KindIdentifier,
				KindWhere,
				KindIdentifier,
				KindEqual,
				KindStringLiteral,
				KindAnd,
				KindIdentifier,
				KindIn,
				KindOpenParen,
				KindStringLiteral,
				KindComma,
				KindStringLiteral,
				KindCloseParen,
				KindSemiColumn,
			},
		},
		{
			given: `SELECT id, name, * from users;`,
			want: []Kind{
				KindSelect,
				KindIdentifier,
				KindComma,
				KindIdentifier,
				KindComma,
				KindIdentifier,
				KindFrom,
				KindIdentifier,
				KindSemiColumn,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.given, func(t *testing.T) {
			got, _ := Tokenize(tc.given)
			g := make([]Kind, 0, len(got))
			for _, got := range got {
				g = append(g, got.Kind)
			}
			if diff := cmp.Diff(tc.want, g); diff != "" {
				t.Fatalf("Tokenize() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

package validation

import (
	"errors"
	"testing"

	"github.com/aliphe/filadb/db/schema"
	"github.com/aliphe/filadb/db/system"
	"github.com/aliphe/filadb/query/sql/lexer"
	"github.com/aliphe/filadb/query/sql/parser"
)

func Test_Check(t *testing.T) {
	tests := map[string]struct {
		shape *system.DatabaseShape
		given string
		want  error
	}{
		"valid select query": {
			shape: system.NewDatabaseShape([]*schema.Schema{
				{
					Table: "users",
					Columns: []schema.Column{
						{
							Name: "id",
							Type: schema.ColumnTypeText,
						},
						{
							Name: "name",
							Type: schema.ColumnTypeText,
						},
					},
				},
				{
					Table: "posts",
					Columns: []schema.Column{
						{
							Name: "id",
							Type: schema.ColumnTypeText,
						},
						{
							Name: "user_id",
							Type: schema.ColumnTypeText,
						},
						{
							Name: "title",
							Type: schema.ColumnTypeText,
						},
					},
				}}),
			given: "select users.id, posts.title from users join posts on posts.user_id = users.id;",
			want:  nil,
		},
		"ambigious select query": {
			shape: system.NewDatabaseShape([]*schema.Schema{
				{
					Table: "user",
					Columns: []schema.Column{
						{
							Name: "id",
							Type: schema.ColumnTypeText,
						},
						{
							Name: "name",
							Type: schema.ColumnTypeText,
						},
					},
				},
				{
					Table: "post",
					Columns: []schema.Column{
						{
							Name: "id",
							Type: schema.ColumnTypeText,
						},
						{
							Name: "user_id",
							Type: schema.ColumnTypeText,
						},
						{
							Name: "title",
							Type: schema.ColumnTypeText,
						},
					},
				},
			}),
			given: "select id, post.title from users join posts on posts.user_id = users.id;",
			want:  ErrAmbiguousReference,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sc := NewSanityChecker(tc.shape)

			tokens, err := lexer.Tokenize(tc.given)
			if err != nil {
				t.Fatal(err)
			}

			q, err := parser.Parse(tokens)
			if err != nil {
				t.Fatal(err)
			}

			err = sc.Check(q)
			if !errors.Is(err, tc.want) {
				t.Errorf("Check() mismatch, want %s, got %s", tc.want, err)
			}
		})
	}
}

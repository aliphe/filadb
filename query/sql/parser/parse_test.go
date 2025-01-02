package parser

import (
	"testing"

	"github.com/aliphe/filadb/query/sql/lexer"
	"github.com/google/go-cmp/cmp"
)

func Test_Parse(t *testing.T) {
	tests := []struct {
		given   string
		want    SQLQuery
		wantErr error
	}{
		{
			given: "SELECT * FROM USERS;",
			want: SQLQuery{
				Type: QueryTypeSelect,
				Select: Select{
					Fields: []Field{{Column: "*"}},
					From: From{
						Table: "USERS",
					},
				},
			},
		},
		{
			given: "SELECT * FROM USERS WHERE id = '1' and name = 'john'",
			want: SQLQuery{
				Type: QueryTypeSelect,
				Select: Select{
					Fields: []Field{{Column: "*"}},
					From: From{
						Table: "USERS",
						Where: []Filter{
							{
								Field: Field{
									Column: "id",
								},
								Op: OpEqual,
								Value: FilterValue{
									Type:  FilterTypeLitteral,
									Value: "1",
								},
							},
							{
								Field: Field{
									Column: "name",
								},
								Op: OpEqual,
								Value: FilterValue{
									Type:  FilterTypeLitteral,
									Value: "john",
								},
							},
						},
					},
				},
			},
		},
		{
			given: "SELECT posts.name FROM users JOIN posts ON posts.user_id = users.id WHERE posts.label = 'public' and users.name = 'bob';",
			want: SQLQuery{
				Type: QueryTypeSelect,
				Select: Select{
					Fields: []Field{
						{
							Table:  "posts",
							Column: "name",
						},
					},
					From: From{
						Table: "users",
						Where: []Filter{
							{
								Field: Field{
									Table:  "posts",
									Column: "label",
								},
								Op: OpEqual,
								Value: FilterValue{
									Type:  FilterTypeLitteral,
									Value: "public",
								},
							},
							{
								Field: Field{
									Table:  "users",
									Column: "name",
								},
								Op: OpEqual,
								Value: FilterValue{
									Type:  FilterTypeLitteral,
									Value: "bob",
								},
							},
						},
						Joins: []Join{
							{
								Table: "posts",
								On: JoinOn{
									Op: OpEqual,
									Left: Field{
										Table:  "posts",
										Column: "user_id",
									},
									Right: Field{
										Table:  "users",
										Column: "id",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.given, func(t *testing.T) {
			tokens, err := lexer.Tokenize(tc.given)
			if err != nil {
				t.Fatalf("Tokenize error: %v", err)
			}
			ast, err := Parse(tokens)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			if diff := cmp.Diff(tc.want, ast); diff != "" {
				t.Fatalf("Parse() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

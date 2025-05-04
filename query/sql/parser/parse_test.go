package parser

import (
	"testing"

	"github.com/aliphe/filadb/db"
	"github.com/aliphe/filadb/db/object"
	"github.com/aliphe/filadb/query/sql/lexer"
	"github.com/google/go-cmp/cmp"
)

func Test_Parse(t *testing.T) {
	tests := []struct {
		given   string
		want    *SQLQuery
		wantErr error
	}{
		{
			given: "SELECT * FROM USERS;",
			want: &SQLQuery{
				Type: QueryTypeSelect,
				Select: Select{
					Fields: []Field{{Column: "*"}},
					From:   "USERS",
				},
			},
		},
		{
			given: "SELECT email FROM USERS WHERE id = '1' and name = 'john'",
			want: &SQLQuery{
				Type: QueryTypeSelect,
				Select: Select{
					Fields: []Field{{Column: "email"}},
					From:   "USERS",
					Filters: []Filter{
						{
							Left: Value{
								Type: ValueTypeReference,
								Reference: Field{
									Column: "id",
								},
							},
							Op: db.OpEqual,
							Right: Value{
								Type:  ValueTypeLitteral,
								Value: "1",
							},
						},
						{
							Left: Value{
								Type: ValueTypeReference,
								Reference: Field{
									Column: "name",
								},
							},
							Op: db.OpEqual,
							Right: Value{
								Type:  ValueTypeLitteral,
								Value: "john",
							},
						},
					},
				},
			},
		},
		{
			given: `UPDATE users SET email = 'new@email.com' where id = 1;`,
			want: &SQLQuery{
				Type: QueryTypeUpdate,
				Update: Update{
					From: "users",
					Set: Set{
						Update: object.Row{
							"email": "new@email.com",
						},
					},
					Filters: []Filter{
						{
							Left: Value{
								Type: ValueTypeReference,
								Reference: Field{
									Column: "id",
								},
							},
							Op: db.OpEqual,
							Right: Value{
								Type:  ValueTypeLitteral,
								Value: int32(1),
							},
						},
					},
				},
			},
		},
		{
			given: `
				SELECT posts.name FROM users
				JOIN posts ON posts.user_id = users.id
				WHERE posts.label = 'public' and users.name IN ('alice', 'bob');`,
			want: &SQLQuery{
				Type: QueryTypeSelect,
				Select: Select{
					Fields: []Field{
						{
							Table:  "posts",
							Column: "name",
						},
					},
					From: "users",
					Filters: []Filter{
						{
							Left: Value{
								Type: ValueTypeReference,
								Reference: Field{
									Table:  "posts",
									Column: "label",
								},
							},
							Op: db.OpEqual,
							Right: Value{
								Type:  ValueTypeLitteral,
								Value: "public",
							},
						},
						{
							Left: Value{
								Type: ValueTypeReference,
								Reference: Field{
									Table:  "users",
									Column: "name",
								},
							},
							Op: db.OpInclude,
							Right: Value{
								Type:  ValueTypeList,
								Value: []any{"alice", "bob"},
							},
						},
					},
					Joins: []Join{
						{
							Table: "posts",
							On: On{
								Local: Field{
									Table:  "users",
									Column: "id",
								},
								Foreign: Field{
									Table:  "posts",
									Column: "user_id",
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
			q, err := Parse(tokens)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			if diff := cmp.Diff(tc.want, q); diff != "" {
				t.Fatalf("Parse() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

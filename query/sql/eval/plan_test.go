package eval

import (
	"testing"

	"github.com/aliphe/filadb/query/sql/parser"
)

func Test_eval(t *testing.T) {
	// select * from users
	// join posts on posts.user_id = users.id
	// where users.age > 21
	tests := []struct {
		given string
		want  step
	}{
		{
			given: `
			select * from users
			join posts on posts.user_id = users.id
			where users.age > 21
			`,
			want: step{
				queries: []query{
					{
						table: "users",
						filters: []parser.Filter{
							{
								Left: parser.Value{
									Type: parser.ValueTypeReference,
									Reference: parser.Field{
										Column: "age",
									},
								},
								Op: parser.OpMoreThan,
								Right: parser.Value{
									Type:  parser.ValueTypeLitteral,
									Value: 21,
								},
							},
						},
					},
				},
				children: []step{
					{
						queries: []query{
							{
								table: "posts",
								filters: []parser.Filter{
									{
										Left: parser.Value{
											Type: parser.ValueTypeReference,
											Reference: parser.Field{
												Table:  "posts",
												Column: "user_id",
											},
										},
										Op: parser.OpEqual,
										Right: parser.Value{
											Type: parser.ValueTypeReference,
											Reference: parser.Field{
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
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.given, func(_ *testing.T) {
			return
		})
	}
}

package eval

import (
	"testing"

	"github.com/aliphe/filadb/query/sql/parser"
)

func Test_eval(t *testing.T) {
	// select * from users
	// join posts on posts.user_id = users.id
	// where users.age > 21
	var _ = step{
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
	}

	// tests := []struct {
	// 	given parser.Select
	// 	want  step
	// }{
	// 	{
	// 		given: parser.Select{
	// 			From: parser.From{
	// 				Table: "users",
	// 				Where: []parser.Filter{
	// 					{
	// 						Field: parser.Field{
	// 							Column: "age",
	// 						},
	// 						Op: parser.OpMoreThan,
	// 						Value: parser.FilterValue{
	// 							Type:  parser.FilterTypeLitteral,
	// 							Value: 21,
	// 						},
	// 					},
	// 				},
	// 				Joins: []parser.Join{
	// 					{
	// 						Table:
	// 					}
	// 				},
	// 			},
	// 		},
	// 	},
	// }

}

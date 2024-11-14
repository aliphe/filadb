package app

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/aliphe/filadb/btree/file"
)

func Test_Run(t *testing.T) {
	type step struct {
		given string
		want  string
	}
	tests := map[string]struct {
		scenario []step
	}{
		"Read system tables": {
			scenario: []step{
				{
					given: "CREATE TABLE users (id NUMBER, email TEXT);",
					want:  "CREATE TABLE\n>",
				},
				{
					given: "INSERT INTO users (id, email) VALUES (1, 'test@tust.com'), (2, 'tast@test.com');",
					want:  "INSERT 2\n>",
				},
				{
					given: "SELECT * FROM users;",
					want: `id,email
1,test@tust.com
2,tast@test.com

>`,
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := os.RemoveAll(".testdb")
			if err != nil {
				t.Fatal(err)
			}

			go Run(WithFileOptions(file.WithPath(".testdb")))

			conn, err := net.Dial("tcp", ":5432")
			if err != nil {
				t.Fatal(err)
			}

			for _, step := range tc.scenario {
				_, err := conn.Write([]byte(step.given))
				if err != nil {
					t.Fatal(err)
				}
				res, err := bufio.NewReader(conn).ReadBytes('>')
				if err != nil {
					t.Fatal(err)
				}
				if string(res) != step.want {
					t.Fatal(fmt.Errorf("res mismatch, want='%s', got='%s'", string(step.want), string(res)))
				}
			}
		})
	}
}

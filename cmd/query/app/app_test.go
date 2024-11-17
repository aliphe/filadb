package app

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

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
					want:  strings.Join([]string{"CREATE TABLE", ">"}, "\n"),
				},
				{
					given: "INSERT INTO users (id, email) VALUES (1, 'test@tust.com'), (2, 'tast@test.com');",
					want:  strings.Join([]string{"INSERT 2", ">"}, "\n"),
				},
				{
					given: "SELECT * FROM users;",
					want:  strings.Join([]string{"id,email", "1,test@tust.com", "2,tast@test.com", ">"}, "\n"),
				},
				{
					given: "SELECT * FROM users where id = 1;",
					want:  strings.Join([]string{"id,email", "1,test@tust.com", ">"}, "\n"),
				},
				{
					given: "SELECT email, id, * FROM users where id = 2;",
					want:  strings.Join([]string{"email,id,id,email", "tast@test.com,2,2,tast@test.com", ">"}, "\n"),
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

			time.Sleep(50 * time.Millisecond)

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
					t.Fatal(fmt.Errorf("%s mismatch, want='%s', got='%s'", step.given, string(step.want), string(res)))
				}
			}
		})
	}
}

package app

import (
	"bufio"
	"context"
	"fmt"
	"net"
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
					given: "SELECT id, email FROM users;",
					want:  strings.Join([]string{"id,email", "1,test@tust.com", "2,tast@test.com", ">"}, "\n"),
				},
				{
					given: "SELECT id, email FROM users where id = 1;",
					want:  strings.Join([]string{"id,email", "1,test@tust.com", ">"}, "\n"),
				},
				{
					given: "UPDATE users SET email = 'new@email.com' WHERE id = 2;",
					want:  strings.Join([]string{"UPDATE 1", ">"}, "\n"),
				},
				{
					given: "SELECT id, email FROM users where id IN (1,2);",
					want:  strings.Join([]string{"id,email", "1,test@tust.com", "2,new@email.com", ">"}, "\n"),
				},
			},
		},
		"With index": {
			scenario: []step{
				{
					given: "CREATE TABLE users (id NUMBER, email TEXT);",
					want:  strings.Join([]string{"CREATE TABLE", ">"}, "\n"),
				},
				{
					given: "CREATE INDEX user_email ON users(email);",
					want:  strings.Join([]string{"CREATE INDEX", ">"}, "\n"),
				},
				{
					given: "INSERT INTO users (id, email) VALUES (1, 'test@indexed.com');",
					want:  strings.Join([]string{"INSERT 1", ">"}, "\n"),
				},
				{
					given: "SELECT id FROM users WHERE email = 'test@indexed.com';",
					want:  strings.Join([]string{"id", "1", ">"}, "\n"),
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// t.Parallel()
			dir := t.TempDir()
			t.Log("DIR", dir)
			// err := os.RemoveAll(dir)

			ctx, cancel := context.WithCancel(t.Context())
			go Run(ctx, WithFileOptions(file.WithPath(dir)))

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

			err = conn.Close()
			if err != nil {
				t.Fatal(err)
			}
			cancel()
		})
	}
}

package app

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aliphe/filadb/btree/file"
	"github.com/aliphe/filadb/cmd/db/app/handler"
	fnet "github.com/aliphe/filadb/net"
)

func Test_Run(t *testing.T) {
	type step struct {
		given string
		want  string
	}
	tests := map[string]struct {
		scenario []step
	}{
		// "Basic operations": {
		// 	scenario: []step{
		// 		{
		// 			given: "CREATE TABLE users (id NUMBER, email TEXT);",
		// 			want:  "CREATE TABLE",
		// 		},
		// 		{
		// 			given: "INSERT INTO users (id, email) VALUES (1, 'test@tust.com'), (2, 'tast@test.com');",
		// 			want:  "INSERT 2",
		// 		},
		// 		{
		// 			given: "SELECT id, email FROM users;",
		// 			want:  strings.Join([]string{"id,email", "1,test@tust.com", "2,tast@test.com"}, "\n"),
		// 		},
		// 		{
		// 			given: "SELECT id, email FROM users where id = 1;",
		// 			want:  strings.Join([]string{"id,email", "1,test@tust.com"}, "\n"),
		// 		},
		// 		{
		// 			given: "UPDATE users SET email = 'new@email.com' WHERE id = 2;",
		// 			want:  strings.Join([]string{"UPDATE 1"}, "\n"),
		// 		},
		// 		{
		// 			given: "SELECT * FROM users where id IN (1,2);",
		// 			want:  strings.Join([]string{"email,id", "test@tust.com,1", "new@email.com,2"}, "\n"),
		// 		},
		// 	},
		// },
		// "With index": {
		// 	scenario: []step{
		// 		{
		// 			given: "CREATE TABLE users (id NUMBER, email TEXT);",
		// 			want:  strings.Join([]string{"CREATE TABLE"}, "\n"),
		// 		},
		// 		{
		// 			given: "CREATE INDEX user_email ON users(email);",
		// 			want:  strings.Join([]string{"CREATE INDEX"}, "\n"),
		// 		},
		// 		{
		// 			given: "INSERT INTO users (id, email) VALUES (1, 'test@indexed.com');",
		// 			want:  strings.Join([]string{"INSERT 1"}, "\n"),
		// 		},
		// 		{
		// 			given: "SELECT id FROM users WHERE email = 'test@indexed.com';",
		// 			want:  strings.Join([]string{"id", "1"}, "\n"),
		// 		},
		// 	},
		// },
		"With join": {
			scenario: []step{
				{
					given: "CREATE TABLE users (id NUMBER, email TEXT);",
					want:  strings.Join([]string{"CREATE TABLE"}, "\n"),
				},
				{
					given: "CREATE TABLE posts (id NUMBER, user_id NUMBER, content TEXT);",
					want:  strings.Join([]string{"CREATE TABLE"}, "\n"),
				},
				{
					given: "CREATE TABLE comments(id NUMBER, post_id NUMBER, content TEXT);",
					want:  strings.Join([]string{"CREATE TABLE"}, "\n"),
				},
				{
					given: "INSERT INTO users (id, email) VALUES (1, 'test@indexed.com');",
					want:  strings.Join([]string{"INSERT 1"}, "\n"),
				},
				{
					given: "INSERT INTO posts (id, user_id, content) VALUES (1, 1, 'First post');",
					want:  strings.Join([]string{"INSERT 1"}, "\n"),
				},
				{
					given: "INSERT INTO comments (id, post_id, content) VALUES (1, 1, 'First comment'), (2, 1, 'Second comment'), (3, 1, 'Third comment');",
					want:  strings.Join([]string{"INSERT 3"}, "\n"),
				},
				{
					given: `
					SELECT
						users.email, posts.content, comments.content
					FROM users
					JOIN posts ON users.id = posts.user_id
					JOIN comments ON posts.id = comments.post_id;`,
					want: strings.Join([]string{"email,content,content", "test@indexed.com,First post,First comment", "test@indexed.com,First post,Second comment", "test@indexed.com,First post,Third comment"}, "\n"),
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()

			// initialise a listener on a random port to retrieve a valid one.
			listener, err := net.Listen("tcp", ":0")
			if err != nil {
				t.Fatal(err)
			}
			addr := listener.Addr().String()
			listener.Close()

			ctx, cancel := context.WithCancel(t.Context())
			go Run(ctx, WithFileOptions(file.WithPath(dir)), WithHandlerOptions(handler.WithAddr(addr)))

			time.Sleep(50 * time.Millisecond)

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}

			for _, step := range tc.scenario {
				err := fnet.Send(conn, []byte(step.given))
				if err != nil {
					t.Fatal(err)
				}
				res, err := fnet.Read(conn)
				if err != nil {
					t.Fatal(err)
				}

				if string(res) != step.want {
					t.Fatal(fmt.Errorf("%s mismatch, want='%s', got='%s'", strings.TrimSpace(step.given), string(step.want), string(res)))
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

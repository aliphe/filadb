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
		given []byte
		want  []byte
	}
	tests := map[string]struct {
		scenario []step
	}{
		"Read system tables": {
			scenario: []step{
				{
					given: []byte("SELECT * FROM tables;"),
					want:  []byte(""),
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
				_, err := conn.Write(step.given)
				if err != nil {
					t.Fatal(err)
				}
				res, err := bufio.NewReader(conn).ReadBytes('\n')
				if err != nil {
					t.Fatal(err)
				}

				if string(res) != string(step.want) {
					t.Fatal(fmt.Errorf("res mismatch, want='%s', got='%s'", string(step.want), string(res)))
				}
			}

		})
	}
}

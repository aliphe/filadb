package tcp

import (
	"io"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Test_readQueries(t *testing.T) {
	tests := map[string]struct {
		given io.Reader
		want  []string
	}{
		"single op": {
			given: strings.NewReader("select * from users;"),
			want:  []string{"select * from users"},
		},
		"multi op": {
			given: strings.NewReader("select * from posts; select * from users;"),
			want:  []string{"select * from posts", "select * from users"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := readQueries(tc.given)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("readQueries mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

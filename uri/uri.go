package uri

import (
	"errors"
	"fmt"
	"net"
	"net/url"
)

const Scheme = "filadb"

type User struct {
	Username string
	Password string
}

// URI represents a filadb instance URI
type URI struct {
	User *User
	Host string
	Port string
}

func Parse(uri string) (*URI, error) {
	url, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	if url.Scheme != Scheme {
		return nil, errors.New("invalid scheme, expected filadb")
	}

	var out URI
	out.User = &User{}

	if url.User != nil {
		out.User.Username = url.User.Username()
		pwd, ok := url.User.Password()
		if ok {
			out.User.Password = pwd
		}
	}
	out.Host = url.Hostname()
	out.Port = url.Port()
	return &out, nil
}

func (u *URI) Address() string {
	addr := net.JoinHostPort(u.Host, u.Port)
	if u.User != nil && u.User.Username != "" {
		if u.User.Password != "" {
			return fmt.Sprintf("%s:%s@%s", u.User.Username, u.User.Password, addr)
		}
		return fmt.Sprintf("%s@%s", u.User.Username, addr)
	}
	return addr
}

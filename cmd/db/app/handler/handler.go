package handler

import "time"

type Type string

const (
	TypeRestAPI Type = "restapi"
	TypeTCP     Type = "tcp"
)

type Handler interface {
	Listen() error
}

type Options struct {
	Addr    string
	Timeout time.Duration
}

type Option func(*Options)

func WithAddr(addr string) Option {
	return func(o *Options) {
		o.Addr = addr
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.Timeout = timeout
	}
}

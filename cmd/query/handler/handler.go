package handler

type Type string

const (
	TypeRestAPI Type = "restapi"
)

type Handler interface {
	Listen() error
	Close()
}

type Options struct {
	Version string
	Addr    string
}

type Option func(*Options)

func WithVersion(version string) Option {
	return func(o *Options) {
		o.Version = version
	}
}

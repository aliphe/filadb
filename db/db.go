package db

import (
	"os"
)

type DB struct {
	f *os.File
}

func New(f *os.File) *DB {
	return &DB{
		f: f,
	}
}

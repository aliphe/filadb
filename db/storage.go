package db

import (
	"fmt"
	"os"
)

func fileName(table string) string {
	return table + ".log"
}

func fileWriter(table string) (*os.File, error) {
	name := fileName(table)

	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file %s: %w", name, err)
	}

	return f, nil
}

func fileReader(table string) (*os.File, error) {
	name := fileName(table)

	f, err := os.OpenFile(name, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file %s: %w", name, err)
	}

	return f, nil
}

package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

func Write(conn io.Writer, res []byte) error {
	headerLen := uint32(len(res))

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, headerLen)

	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	if _, err := conn.Write(res); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	return nil
}

func Read(conn io.Reader) ([]byte, error) {
	lenBuf := make([]byte, 4)
	_, err := io.ReadFull(conn, lenBuf)
	if err != nil {
		return nil, err
	}

	resLen := binary.BigEndian.Uint32(lenBuf)

	res := make([]byte, resLen)
	_, err = io.ReadFull(conn, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

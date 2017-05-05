package goavro

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

func booleanDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 1 {
		return nil, nil, io.ErrShortBuffer
	}
	var b byte
	b, buf = buf[0], buf[1:]
	switch b {
	case byte(0):
		return false, buf, nil
	case byte(1):
		return true, buf, nil
	default:
		return nil, buf, fmt.Errorf("boolean: expected: Go byte(0) or byte(1); received: byte(%d)", b)
	}
}

func booleanEncoder(buf []byte, datum interface{}) ([]byte, error) {
	value, ok := datum.(bool)
	if !ok {
		return buf, fmt.Errorf("boolean: expected: Go bool; received: %T", datum)
	}
	var b byte
	if value {
		b = 1
	}
	return append(buf, b), nil
}

func booleanTextDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 4 {
		return nil, nil, io.ErrShortBuffer
	}
	if bytes.Equal(buf[:4], []byte("true")) {
		return true, buf[4:], nil
	}
	if len(buf) < 5 {
		return nil, nil, io.ErrShortBuffer
	}
	if bytes.Equal(buf[:5], []byte("false")) {
		return false, buf[5:], nil
	}
	return nil, buf, errors.New("expected false or true")
}

func booleanTextEncoder(buf []byte, datum interface{}) ([]byte, error) {
	value, ok := datum.(bool)
	if !ok {
		return buf, fmt.Errorf("boolean: expected: Go bool; received: %T", datum)
	}
	if value {
		return append(buf, "true"...), nil
	}
	return append(buf, "false"...), nil
}

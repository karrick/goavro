package goavro

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var nullBytes = []byte("null")

func nullNativeFromBinary(buf []byte) (interface{}, []byte, error) { return nil, buf, nil }

func nullBinaryFromNative(buf []byte, datum interface{}) ([]byte, error) {
	if datum != nil {
		return nil, fmt.Errorf("cannot encode binary null: expected: Go nil; received: %T", datum)
	}
	return buf, nil
}

func nullNativeFromTextual(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 4 {
		return nil, nil, fmt.Errorf("cannot decode textual null: %s", io.ErrShortBuffer)
	}
	if bytes.Equal(buf[:4], nullBytes) {
		return nil, buf[4:], nil
	}
	return nil, nil, errors.New("cannot decode textual null: expected: null")
}

func nullTextualFromNative(buf []byte, datum interface{}) ([]byte, error) {
	if datum != nil {
		return nil, fmt.Errorf("cannot encode textual null: expected: Go nil; received: %T", datum)
	}
	return append(buf, nullBytes...), nil
}

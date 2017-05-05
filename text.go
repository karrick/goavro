package goavro

import (
	"fmt"
	"io"
	"unicode"
)

func gobble(buf []byte, expected byte) ([]byte, error) {
	var err error
	if buf, err = advanceToNonWhitespace(buf); err != nil {
		return nil, err
	}
	if actual := buf[0]; actual != expected {
		return buf, fmt.Errorf("cannot read Record: expected: %q; actual: %q", expected, actual)
	}
	return buf[1:], nil
}

// advanceToNonWhitespace consumes bytes from buf until non-whitespace character
// is found. It returns error when no more bytes remain, because its purpose is
// to scan ahead to the next non-whitespace character.
func advanceToNonWhitespace(buf []byte) ([]byte, error) {
	for i, b := range buf {
		if !unicode.IsSpace(rune(b)) {
			return buf[i:], nil
		}
	}
	return nil, io.ErrShortBuffer
}

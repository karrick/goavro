package goavro

import (
	"fmt"
)

// Fixed does not have child objects, therefore whatever namespace it defines is
// just to store its name in the symbol table.
func makeFixedCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	c, err := registerNewCodec(st, schemaMap, enclosingNamespace)
	if err != nil {
		return nil, fmt.Errorf("Fixed ought to have valid name: %s", err)
	}
	// Fixed type must have size
	s1, ok := schemaMap["size"]
	if !ok {
		return nil, fmt.Errorf("Fixed %q ought to have size key", c.typeName)
	}
	s2, ok := s1.(float64)
	if !ok || s2 <= 0 {
		return nil, fmt.Errorf("Fixed %q size ought to be number greater than zero: %v", c.typeName, s1)
	}
	size := uint(s2)

	c.binaryDecoder = func(buf []byte) (interface{}, []byte, error) {
		if buflen := uint(len(buf)); size > buflen {
			return nil, buf, fmt.Errorf("Fixed %q short buffer: schema size exceeds remaining buffer size: %d > %d", c.typeName, size, buflen)
		}
		return buf[:size], buf[size:], nil
	}
	c.binaryEncoder = func(buf []byte, datum interface{}) ([]byte, error) {
		someBytes, ok := datum.([]byte)
		if !ok {
			return buf, fmt.Errorf("cannot encode Fixed %q: expected []byte; received: %T", c.typeName, datum)
		}
		if count := uint(len(someBytes)); count != size {
			return buf, fmt.Errorf("cannot encode Fixed %q: datum size ought to equal schema size: %d != %d", c.typeName, count, size)
		}
		return append(buf, someBytes...), nil
	}

	c.textDecoder = func(buf []byte) (interface{}, []byte, error) {
		if buflen := uint(len(buf)); size > buflen {
			return nil, buf, fmt.Errorf("Fixed %q short buffer: schema size exceeds remaining buffer size: %d > %d", c.typeName, size, buflen)
		}
		var datum interface{}
		var err error
		datum, buf, err = bytesTextDecoder(buf)
		if err != nil {
			return nil, buf, err
		}
		datumBytes := datum.([]byte)
		if count := uint(len(datumBytes)); count != size {
			return nil, buf, fmt.Errorf("cannot decode Fixed %q: datum size ought to equal schema size: %d != %d", c.typeName, count, size)
		}
		return datum, buf, err
	}

	c.textEncoder = func(buf []byte, datum interface{}) ([]byte, error) {
		someBytes, ok := datum.([]byte)
		if !ok {
			return buf, fmt.Errorf("cannot encode Fixed %q: expected []byte; received: %T", c.typeName, datum)
		}
		if count := uint(len(someBytes)); count != size {
			return buf, fmt.Errorf("cannot encode Fixed %q: datum size ought to equal schema size: %d != %d", c.typeName, count, size)
		}
		return bytesTextEncoder(buf, someBytes)
	}

	return c, nil
}

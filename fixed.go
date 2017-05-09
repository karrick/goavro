package goavro

import (
	"fmt"
)

// Fixed does not have child objects, therefore whatever namespace it defines is just to store its
// name in the symbol table.
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
	if !ok || int(s2) <= 0 {
		return nil, fmt.Errorf("Fixed %q size ought to be number greater than zero: %v", c.typeName, s1)
	}
	size := int(s2)

	c.binaryDecoder = func(buf []byte) (interface{}, []byte, error) {
		if len(buf) < size {
			return nil, buf, fmt.Errorf("Fixed %q short buffer: size exceeds remaining buffer length: %d > %d", c.typeName, size, len(buf))
		}
		return buf[:size], buf[size:], nil
	}
	c.binaryEncoder = func(buf []byte, datum interface{}) ([]byte, error) {
		var value []byte
		switch v := datum.(type) {
		case string:
			value = []byte(v)
		case []byte:
			value = v
		default:
			return buf, fmt.Errorf("cannot encode Fixed %q: expected string or bytes; received: %T", c.typeName, v)
		}
		if count := len(value); count != size {
			return buf, fmt.Errorf("cannot encode Fixed %q: datum length ought to equal size: %d != %d", c.typeName, count, size)
		}
		return append(buf, value...), nil
	}

	return c, nil
}

package goavro

import (
	"errors"
	"fmt"
)

// Fixed does not have child objects, therefore whatever namespace it defines is just to store its
// name in the symbol table.
func (st symtab) makeFixedCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot create Fixed codec: expected: map[string]interface{}; received: %T", schema)
	}
	// Fixed type must have size
	s1, ok := schemaMap["size"]
	if !ok {
		return nil, fmt.Errorf("cannot create Fixed codec: ought to have size key")
	}
	s2, ok := s1.(float64)
	if !ok || s2 == 0 {
		return nil, fmt.Errorf("cannot create Fixed codec: size ought to be non-zero number: %v", s1)
	}
	size := int32(s2)

	c := &codec{
		namedType: true,
		binaryDecoder: func(buf []byte) (interface{}, []byte, error) {
			if int32(len(buf)) < size {
				return nil, buf, fmt.Errorf("cannot decode Fixed: size exceeds remaining buffer length: %d > %d", size, len(buf))
			}
			return buf[:size], buf[size:], nil
		},
		binaryEncoder: func(buf []byte, datum interface{}) ([]byte, error) {
			var value []byte
			switch v := datum.(type) {
			case string:
				value = []byte(v)
			case []byte:
				value = v
			default:
				return buf, fmt.Errorf("cannot encode Fixed: expected: Go string, []byte; received: %T", v)
			}
			return append(buf, value...), nil
		},
	}

	if err := st.registerCodec(c, schemaMap, enclosingNamespace); err != nil {
		return nil, fmt.Errorf("cannot create Fixed codec: %s", err)
	}
	if c.name == nil {
		return nil, errors.New("cannot create Fixed codec: Fixed requires name")
	}
	return c, nil
}

package goavro

import (
	"fmt"
)

// enum does not have child objects, therefore whatever namespace it defines is just to store its
// name in the symbol table.
func (st symtab) makeEnumCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot create enum codec: expected: map[string]interface{}; received: %T", schema)
	}
	// enum type must have symbols
	s1, ok := schemaMap["symbols"]
	if !ok {
		return nil, fmt.Errorf("cannot create enum codec: ought to have symbols key")
	}
	s2, ok := s1.([]interface{})
	if !ok || len(s2) == 0 {
		return nil, fmt.Errorf("cannot create enum codec: symbols ought to be non-empty array of strings")
	}
	symbols := make([]string, len(s2))
	for i, s := range s2 {
		symbol, ok := s.(string)
		if !ok {
			return nil, fmt.Errorf("cannot create enum codec: symbol ought to be string; received: %T", symbol)
		}
		if err := checkNameComponent(symbol); err != nil {
			return nil, fmt.Errorf("cannot create enum codec: invalid symbol name: %s", err)
		}
		symbols[i] = symbol
	}

	codec := &codec{
		// name: "enum",
		decoder: func(buf []byte) (interface{}, []byte, error) {
			var value interface{}
			var err error
			var index int64

			if value, buf, err = longDecoder(buf); err != nil {
				return nil, buf, fmt.Errorf("cannot decode enum: cannot decode enum index: %s", err)
			}
			index = value.(int64) // longDecoder always returns int64
			if index < 0 || index >= int64(len(symbols)) {
				return nil, buf, fmt.Errorf("cannot decode enum: index must be between 0 and %d; read index: %d", len(symbols)-1, index)
			}
			return symbols[index], buf, nil
		},
		encoder: func(buf []byte, datum interface{}) ([]byte, error) {
			someString, ok := datum.(string)
			if !ok {
				return buf, fmt.Errorf("cannot encode enum: expected string; received: %T", datum)
			}
			for i, symbol := range symbols {
				if symbol == someString {
					return longEncoder(buf, i)
				}
			}
			return buf, fmt.Errorf("cannot encode enum: string not member of enum symbols: %s", someString)
		},
	}

	// name support
	var name, namespace string
	if value, ok := schemaMap["name"]; ok {
		name, ok = value.(string)
		if !ok {
			return nil, fmt.Errorf("cannot create enum codec: name ought to be string; received: %T", value)
		}
	}
	if value, ok := schemaMap["namespace"]; ok {
		namespace, ok = value.(string)
		if !ok {
			return nil, fmt.Errorf("cannot create enum codec: namespace ought to be string; received: %T", value)
		}
	}
	if name != "" {
		// if name is defined, then register with symbol table
		n, err := NewName(name, namespace, enclosingNamespace)
		if err != nil {
			return nil, fmt.Errorf("cannot create enum codec: %s", err)
		}
		// fmt.Printf("DEBUG: n: %#v\n", n)
		codec.name = n.FullName
		st.cache[n.FullName] = codec
	}

	return codec, nil
}

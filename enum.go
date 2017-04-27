package goavro

import (
	"fmt"
)

// enum does not have child objects, therefore whatever namespace it defines is just to store its
// name in the symbol table.
func makeEnumCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	c, err := registerNewCodec(st, schemaMap, enclosingNamespace)
	if err != nil {
		return nil, fmt.Errorf("Enum ought to have valid name: %s", err)
	}

	// enum type must have symbols
	s1, ok := schemaMap["symbols"]
	if !ok {
		return nil, fmt.Errorf("Enum %q ought to have symbols key", c.typeName)
	}
	s2, ok := s1.([]interface{})
	if !ok || len(s2) == 0 {
		return nil, fmt.Errorf("Enum %q symbols ought to be non-empty array of strings: %v", c.typeName, s1)
	}
	symbols := make([]string, len(s2))
	for i, s := range s2 {
		symbol, ok := s.(string)
		if !ok {
			return nil, fmt.Errorf("Enum %q symbol %d ought to be non-empty string; received: %T", c.typeName, i+1, symbol)
		}
		if err := checkString(symbol); err != nil {
			return nil, fmt.Errorf("Enum %q symbol %d ought to %s", c.typeName, i+1, err)
		}
		symbols[i] = symbol
	}

	c.binaryDecoder = func(buf []byte) (interface{}, []byte, error) {
		var value interface{}
		var err error
		var index int64

		if value, buf, err = longDecoder(buf); err != nil {
			return nil, buf, fmt.Errorf("cannot decode Enum %q: index: %s", c.typeName, err)
		}
		index = value.(int64) // longDecoder always returns int64
		if index < 0 || index >= int64(len(symbols)) {
			return nil, buf, fmt.Errorf("cannot decode Enum %q: index ought to be between 0 and %d; read index: %d", c.typeName, len(symbols)-1, index)
		}
		return symbols[index], buf, nil
	}
	c.binaryEncoder = func(buf []byte, datum interface{}) ([]byte, error) {
		someString, ok := datum.(string)
		if !ok {
			return buf, fmt.Errorf("cannot encode Enum %q: expected string; received: %T", c.typeName, datum)
		}
		for i, symbol := range symbols {
			if symbol == someString {
				return longEncoder(buf, i)
			}
		}
		return buf, fmt.Errorf("cannot encode Enum %q: value ought to be member of symbols: %v; %q", c.typeName, symbols, someString)
	}

	return c, nil
}

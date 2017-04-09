package goavro

import (
	"encoding/json"
	"fmt"
)

type codec struct {
	namespace, name, fullname string
	decoder                   func([]byte) (interface{}, []byte, error)
	encoder                   func([]byte, interface{}) ([]byte, error)
}

// NewCodec ...
func NewCodec(someJSONSchema string) (*codec, error) {
	// pre-load the cache with primitive types
	st := &symtab{cache: map[string]*codec{
		"boolean": &codec{name: "boolean", decoder: booleanDecoder, encoder: booleanEncoder},
		"bytes":   &codec{name: "bytes", decoder: bytesDecoder, encoder: bytesEncoder},
		"double":  &codec{name: "double", decoder: doubleDecoder, encoder: doubleEncoder},
		"float":   &codec{name: "float", decoder: floatDecoder, encoder: floatEncoder},
		"int":     &codec{name: "int", decoder: intDecoder, encoder: intEncoder},
		"long":    &codec{name: "long", decoder: longDecoder, encoder: longEncoder},
		"null":    &codec{name: "null", decoder: nullDecoder, encoder: nullEncoder},
		"string":  &codec{name: "string", decoder: stringDecoder, encoder: stringEncoder},
	}}

	// NOTE: Legal schema might be an unadorned Avro primitive type name, which cannot be
	// unmarshaled via JSON library.
	if cd, ok := st.cache[someJSONSchema]; ok {
		return cd, nil
	}

	var schema interface{}
	if err := json.Unmarshal([]byte(someJSONSchema), &schema); err != nil {
		return nil, fmt.Errorf("cannot unmarshal JSON: %s", err)
	}

	return st.buildCodec("", schema)
}

func (cd codec) Decode(buf []byte) (interface{}, []byte, error) {
	return cd.decoder(buf)
}

func (cd codec) Encode(buf []byte, datum interface{}) ([]byte, error) {
	return cd.encoder(buf, datum)
}

type symtab struct {
	cache map[string]*codec
}

// convert a schema data structure to a codec, prefixing with specified namespace
func (st symtab) buildCodec(namespace string, schema interface{}) (*codec, error) {
	switch schemaType := schema.(type) {
	case string:
		return st.buildCodecForTypeDescribedByString(namespace, schemaType, schema)
	case []interface{}:
		return st.buildCodecForTypeDescribedBySlice(namespace, schemaType)
	case map[string]interface{}:
		return st.buildCodecForTypeDescribedByMap(namespace, schemaType)
	default:
		return nil, fmt.Errorf("cannot build codec: unknown schema type: %T", schema)
	}
}

func (st symtab) buildCodecForTypeDescribedByMap(namespace string, schema map[string]interface{}) (*codec, error) {
	// INPUT: {"type":"boolean", ...}

	// (map needs to have a type)
	t, ok := schema["type"]
	if !ok {
		return nil, fmt.Errorf("cannot build codec: missing type: %v", schema)
	}
	switch v := t.(type) {
	case string:
		// Already defined types may be abbreviated with its string name.
		// EXAMPLE: "type":"int"
		// EXAMPLE: "type":"enum"
		// EXAMPLE: "type":"somePreviouslyDefinedCustomTypeString"
		return st.buildCodecForTypeDescribedByString(namespace, v, schema)
	case map[string]interface{}, []interface{}:
		// EXAMPLE: "type":{"type":fixed","name":"fixed_16","size":16}
		// EXAMPLE: "type":["null","int"]
		return st.buildCodec(namespace, v)
	default:
		return nil, fmt.Errorf("cannot build codec: type ought to be either string, map[string]interface{}, or []interface{}; received: %T", t)
	}
}

func (st symtab) buildCodecForTypeDescribedByString(namespace string, schemaType string, schema interface{}) (*codec, error) {
	// when codec already exists, return it
	if cd, ok := st.cache[namespace+schemaType]; ok {
		return cd, nil
	}
	switch schemaType {
	case "array":
		return st.makeArrayCodec(namespace, schema)
	case "map":
		return st.makeMapCodec(namespace, schema)
	// case "enum":
	// 	return st.makeEnumCodec(enclosingNamespace, schema)
	// case "fixed":
	// 	return st.makeFixedCodec(enclosingNamespace, schema)
	// case "record":
	// 	return st.makeRecordCodec(namespace, schema)
	default:
		return nil, fmt.Errorf("cannot build codec for unknown schema type: %s", schemaType)
	}
}

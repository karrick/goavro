package goavro

import (
	"encoding/json"
	"fmt"
)

// Decoder interface describes types that expose the Decode method.
type Decoder interface {
	Decode([]byte) (interface{}, []byte, error)
}

// Encoder interface describes types that expose the Encode method.
type Encoder interface {
	Encode([]byte, interface{}) ([]byte, error)
}

// Codec interface describes types that expose both the Decode and the Encode methods.
type Codec interface {
	Decoder
	Encoder
}

type codec struct {
	name    string
	decoder func([]byte) (interface{}, []byte, error)
	encoder func([]byte, interface{}) ([]byte, error)
}

// NewCodec returns a Codec that can encode and decode the specified schema.
func NewCodec(schemaSpecification string) (Codec, error) {
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

	// NOTE: Some clients will give us unadorned primitive type name for the schema, e.g., "long".
	// This is a valid schema, while it is not valid JSON.  Provide special handling for primitive
	// type names.
	if cd, ok := st.cache[schemaSpecification]; ok {
		return cd, nil
	}

	var schema interface{}
	if err := json.Unmarshal([]byte(schemaSpecification), &schema); err != nil {
		return nil, fmt.Errorf("cannot unmarshal JSON: %s", err)
	}

	codec, err := st.buildCodec("", schema)
	// fmt.Printf("DEBUG: symtab: %#v\n", st)
	return codec, err
}

func (cd codec) Decode(buf []byte) (interface{}, []byte, error) {
	return cd.decoder(buf)
}

func (cd codec) Encode(buf []byte, datum interface{}) ([]byte, error) {
	return cd.encoder(buf, datum)
}

// symtab represents a set of Avro full names to the codec that processes that schema type.
type symtab struct {
	cache map[string]*codec
}

// convert a schema data structure to a codec, prefixing with specified namespace
func (st symtab) buildCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	switch schemaType := schema.(type) {
	case map[string]interface{}:
		return st.buildCodecForTypeDescribedByMap(enclosingNamespace, schemaType)
	case string:
		return st.buildCodecForTypeDescribedByString(enclosingNamespace, schemaType, schema)
	case []interface{}:
		return st.buildCodecForTypeDescribedBySlice(enclosingNamespace, schemaType)
	default:
		return nil, fmt.Errorf("cannot build codec: unknown schema type: %T", schema)
	}
}

// Reach into the map, grabbing its "type".  Use that to create the codec.
func (st symtab) buildCodecForTypeDescribedByMap(enclosingNamespace string, schemaMap map[string]interface{}) (*codec, error) {
	// INPUT: {"type":"boolean", ...}

	t, ok := schemaMap["type"]
	if !ok {
		return nil, fmt.Errorf("cannot build codec: missing type: %v", schemaMap)
	}
	switch v := t.(type) {
	case map[string]interface{}:
		return st.buildCodecForTypeDescribedByMap(enclosingNamespace, v)
	case string:
		// Already defined types may be abbreviated with its string name.
		// EXAMPLE: "type":"int"
		// EXAMPLE: "type":"enum"
		// EXAMPLE: "type":"somePreviouslyDefinedCustomTypeString"
		return st.buildCodecForTypeDescribedByString(enclosingNamespace, v, schemaMap)
	case []interface{}:
		return st.buildCodecForTypeDescribedBySlice(enclosingNamespace, v)
	default:
		return nil, fmt.Errorf("cannot build codec: type ought to be either string, map[string]interface{}, or []interface{}; received: %T", t)
	}
}

func (st symtab) buildCodecForTypeDescribedByString(enclosingNamespace string, schemaType string, schema interface{}) (*codec, error) {
	// when codec already exists, return it
	if cd, ok := st.cache[schemaType]; ok {
		return cd, nil
	}
	switch schemaType {
	case "array":
		return st.makeArrayCodec(enclosingNamespace, schema)
	case "map":
		return st.makeMapCodec(enclosingNamespace, schema)
	case "enum":
		return st.makeEnumCodec(enclosingNamespace, schema)
	// case "fixed":
	// 	return st.makeFixedCodec(enclosingNamespace, schema)
	// case "record":
	// 	return st.makeRecordCodec(namespace, schema)
	default:
		// fmt.Printf("ns: %q\n", name.FullName)
		return nil, fmt.Errorf("cannot build codec for unknown schema type: %s", schemaType)
	}
}

// notion of enclosing namespace changes when record, enum, or fixed create a new namespace, for child objects.
func (st symtab) registerCodec(codec *codec, schemaMap map[string]interface{}, enclosingNamespace string) error {
	var name, namespace string
	if value, ok := schemaMap["name"]; ok {
		name, ok = value.(string)
		if !ok {
			return fmt.Errorf("cannot register codec: name ought to be string; received: %T", value)
		}
		if name != "" {
			if value, ok := schemaMap["namespace"]; ok {
				namespace, ok = value.(string)
				if !ok {
					return fmt.Errorf("cannot register codec: namespace ought to be string; received: %T", value)
				}
			}
			n, err := NewName(name, namespace, enclosingNamespace)
			if err != nil {
				return fmt.Errorf("cannot register codec: %s", err)
			}

			// fmt.Printf("REGISTER: n: %#v\n", n)
			if _, ok := st.cache[n.FullName]; ok {
				return fmt.Errorf("cannot register codec: duplicate type name: %q", n.FullName)
			}
			st.cache[n.FullName] = codec
			codec.name = n.FullName
		}
	}
	return nil
}

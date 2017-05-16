package goavro

import (
	"encoding/json"
	"fmt"
)

// BinaryCoder interface describes types that expose both the BinaryDecode and
// the BinaryEncode methods.
type BinaryCoder interface {
	BinaryDecoder
	BinaryEncoder
}

// BinaryDecoder interface describes types that expose the BinaryDecode method.
type BinaryDecoder interface {
	BinaryDecode([]byte) (interface{}, []byte, error)
}

// BinaryEncoder interface describes types that expose the BinaryEncode method.
type BinaryEncoder interface {
	BinaryEncode([]byte, interface{}) ([]byte, error)
}

// TextCoder interface describes types that expose both the TextDecode and the
// TextEncode methods.
type TextCoder interface {
	TextDecoder
	TextEncoder
}

// TextDecoder interface describes types that expose the TextDecode method.
type TextDecoder interface {
	TextDecode([]byte) (interface{}, []byte, error)
}

// TextEncoder interface describes types that expose the TextEncode method.
type TextEncoder interface {
	TextEncode([]byte, interface{}) ([]byte, error)
}

// Codec supports decoding binary and text Avro data to Go native data types,
// and conversely encoding Go native data types to binary or text Avro data. A
// Codec is created as a stateless structure that can be safely used in multiple
// go routines simultaneously.
type Codec struct {
	typeName *name
	schema   string

	binaryDecoder func([]byte) (interface{}, []byte, error)
	textDecoder   func([]byte) (interface{}, []byte, error)

	binaryEncoder func([]byte, interface{}) ([]byte, error)
	textEncoder   func([]byte, interface{}) ([]byte, error)
}

func newSymbolTable() map[string]*Codec {
	return map[string]*Codec{
		"boolean": &Codec{
			typeName:      &name{"boolean", nullNamespace},
			binaryDecoder: booleanDecoder,
			binaryEncoder: booleanEncoder,
			textDecoder:   booleanTextDecoder,
			textEncoder:   booleanTextEncoder,
		},
		"bytes": &Codec{
			typeName:      &name{"bytes", nullNamespace},
			binaryDecoder: bytesDecoder,
			binaryEncoder: bytesEncoder,
			textDecoder:   bytesTextDecoder,
			textEncoder:   bytesTextEncoder,
		},
		"double": &Codec{
			typeName:      &name{"double", nullNamespace},
			binaryDecoder: doubleDecoder,
			binaryEncoder: doubleEncoder,
			textDecoder:   doubleTextDecoder,
			textEncoder:   doubleTextEncoder,
		},
		"float": &Codec{
			typeName:      &name{"float", nullNamespace},
			binaryDecoder: floatDecoder,
			binaryEncoder: floatEncoder,
			textDecoder:   floatTextDecoder,
			textEncoder:   floatTextEncoder,
		},
		"int": &Codec{
			typeName:      &name{"int", nullNamespace},
			binaryDecoder: intDecoder,
			binaryEncoder: intEncoder,
			textDecoder:   intTextDecoder,
			textEncoder:   intTextEncoder,
		},
		"long": &Codec{
			typeName:      &name{"long", nullNamespace},
			binaryDecoder: longDecoder,
			binaryEncoder: longEncoder,
			textDecoder:   longTextDecoder,
			textEncoder:   longTextEncoder,
		},
		"null": &Codec{
			typeName:      &name{"null", nullNamespace},
			binaryDecoder: nullDecoder,
			binaryEncoder: nullEncoder,
			textDecoder:   nullTextDecoder,
			textEncoder:   nullTextEncoder,
		},
		"string": &Codec{
			typeName:      &name{"string", nullNamespace},
			binaryDecoder: stringDecoder,
			binaryEncoder: stringEncoder,
			textDecoder:   stringTextDecoder,
			textEncoder:   stringTextEncoder,
		},
	}
}

// NewCodec returns a Codec used to decode binary and text Avro data to Go
// native data types, and conversely encode Go native types to binary and text
// Avro data. The returned Codec is a stateless structure that can be safely
// used in multiple go routines simultaneously. The returned Codec will only be
// able to decode and encode data that adheres to the supplied schema
// specification string.
func NewCodec(schemaSpecification string) (*Codec, error) {
	// bootstrap a symbol table with primitive type codecs for the new codec
	st := newSymbolTable()

	// NOTE: Some clients might give us unadorned primitive type name for the
	// schema, e.g., "long". While it is not valid JSON, it is a valid schema.
	// Provide special handling for primitive type names.
	if c, ok := st[schemaSpecification]; ok {
		c.schema = schemaSpecification
		return c, nil
	}

	// NOTE: At this point, schema should be valid JSON, otherwise it's an error
	// condition.
	var schema interface{}
	if err := json.Unmarshal([]byte(schemaSpecification), &schema); err != nil {
		return nil, fmt.Errorf("cannot unmarshal JSON: %s", err)
	}

	c, err := buildCodec(st, nullNamespace, schema)
	if err == nil {
		// compact schema and save it
		compact, err := json.Marshal(schema)
		if err != nil {
			return nil, fmt.Errorf("cannot remarshal schema: %s", err)
		}
		c.schema = string(compact)
	}
	return c, err
}

// BinaryDecode converts Avro data in binary format from the provided byte slice
// to Go native data types in accordance with the Avro schema supplied when
// creating the Codec. On success, it returns the decoded datum, along with a
// new byte slice with the decoded bytes consumed, and a nil error value. On
// error, it returns nil for the datum value, the original byte slice, and the
// error message.
func (c Codec) BinaryDecode(buf []byte) (interface{}, []byte, error) {
	value, newBuf, err := c.binaryDecoder(buf)
	if err != nil {
		return nil, buf, err // if error, return original byte slice
	}
	return value, newBuf, nil
}

// BinaryEncode converts Go native data types to Avro data in binary format in
// accordance with the Avro schema supplied when creating the Codec. It is
// supplied a byte slice to which to append the encoded data and the actual data
// to encode. On success, it returns a new byte slice with the encoded bytes
// appended, and a nil error value. On error, it returns the original byte
// slice, and the error message.
func (c Codec) BinaryEncode(buf []byte, datum interface{}) ([]byte, error) {
	newBuf, err := c.binaryEncoder(buf, datum)
	if err != nil {
		return buf, err // if error, return original byte slice
	}
	return newBuf, nil
}

// TextDecode converts Avro data in JSON text format from the provided byte
// slice to Go native data types in accordance with the Avro schema supplied
// when creating the Codec. On success, it returns the decoded datum, along with
// a new byte slice with the decoded bytes consumed, and a nil error value. On
// error, it returns nil for the datum value, the original byte slice, and the
// error message.
func (c Codec) TextDecode(buf []byte) (interface{}, []byte, error) {
	value, newBuf, err := c.textDecoder(buf)
	if err != nil {
		return nil, buf, err // if error, return original byte slice
	}
	return value, newBuf, nil
}

// TextEncode converts Go native data types to Avro data in JSON text format in
// accordance with the Avro schema supplied when creating the Codec. It is
// supplied a byte slice to which to append the encoded data and the actual data
// to encode. On success, it returns a new byte slice with the encoded bytes
// appended, and a nil error value. On error, it returns the original byte
// slice, and the error message.
func (c Codec) TextEncode(buf []byte, datum interface{}) ([]byte, error) {
	newBuf, err := c.textEncoder(buf, datum)
	if err != nil {
		return buf, err // if error, return original byte slice
	}
	return newBuf, nil
}

// Schema returns the compact schema used to create the Codec.
func (c Codec) Schema() string {
	return c.schema
}

// convert a schema data structure to a codec, prefixing with specified
// namespace
func buildCodec(st map[string]*Codec, enclosingNamespace string, schema interface{}) (*Codec, error) {
	switch schemaType := schema.(type) {
	case map[string]interface{}:
		return buildCodecForTypeDescribedByMap(st, enclosingNamespace, schemaType)
	case string:
		return buildCodecForTypeDescribedByString(st, enclosingNamespace, schemaType, nil)
	case []interface{}:
		return buildCodecForTypeDescribedBySlice(st, enclosingNamespace, schemaType)
	default:
		return nil, fmt.Errorf("unknown schema type: %T", schema)
	}
}

// Reach into the map, grabbing its "type". Use that to create the codec.
func buildCodecForTypeDescribedByMap(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	t, ok := schemaMap["type"]
	if !ok {
		return nil, fmt.Errorf("missing type: %v", schemaMap)
	}
	switch v := t.(type) {
	case string:
		// Already defined types may be abbreviated with its string name.
		// EXAMPLE: "type":"array"
		// EXAMPLE: "type":"enum"
		// EXAMPLE: "type":"fixed"
		// EXAMPLE: "type":"int"
		// EXAMPLE: "type":"record"
		// EXAMPLE: "type":"somePreviouslyDefinedCustomTypeString"
		return buildCodecForTypeDescribedByString(st, enclosingNamespace, v, schemaMap)
	case map[string]interface{}:
		return buildCodecForTypeDescribedByMap(st, enclosingNamespace, v)
	case []interface{}:
		return buildCodecForTypeDescribedBySlice(st, enclosingNamespace, v)
	default:
		return nil, fmt.Errorf("type ought to be either string, map[string]interface{}, or []interface{}; received: %T", t)
	}
}

func buildCodecForTypeDescribedByString(st map[string]*Codec, enclosingNamespace string, typeName string, schemaMap map[string]interface{}) (*Codec, error) {
	// NOTE: When codec already exists, return it. This includes both primitive
	// type codecs added in NewCodec, and user-defined types, added while
	// building the codec.
	if cd, ok := st[typeName]; ok {
		return cd, nil
	}
	// NOTE: Sometimes schema may abbreviate type name inside a namespace.
	if enclosingNamespace != "" {
		if cd, ok := st[enclosingNamespace+"."+typeName]; ok {
			return cd, nil
		}
	}
	// There are only a small handful of complex Avro data types.
	switch typeName {
	case "array":
		return makeArrayCodec(st, enclosingNamespace, schemaMap)
	case "enum":
		return makeEnumCodec(st, enclosingNamespace, schemaMap)
	case "fixed":
		return makeFixedCodec(st, enclosingNamespace, schemaMap)
	case "map":
		return makeMapCodec(st, enclosingNamespace, schemaMap)
	case "record":
		return makeRecordCodec(st, enclosingNamespace, schemaMap)
	default:
		return nil, fmt.Errorf("unknown type name: %q", typeName)
	}
}

// notion of enclosing namespace changes when record, enum, or fixed create a
// new namespace, for child objects.
func registerNewCodec(st map[string]*Codec, schemaMap map[string]interface{}, enclosingNamespace string) (*Codec, error) {
	n, err := newNameFromSchemaMap(enclosingNamespace, schemaMap)
	if err != nil {
		return nil, err
	}
	c := &Codec{typeName: n}
	st[n.fullName] = c
	return c, nil
}

func typeNames(st map[string]*Codec) []string {
	var keys []string
	for k := range st {
		keys = append(keys, k)
	}
	return keys
}

package goavro

import (
	"encoding/json"
	"fmt"
)

// BinaryDecoder interface describes types that expose the Decode method.
type BinaryDecoder interface {
	BinaryDecode([]byte) (interface{}, []byte, error)
}

// BinaryEncoder interface describes types that expose the Encode method.
type BinaryEncoder interface {
	BinaryEncode([]byte, interface{}) ([]byte, error)
}

// BinaryCoder interface describes types that expose both the Decode and the Encode methods.
type BinaryCoder interface {
	BinaryDecoder
	BinaryEncoder
}

// codec stores function pointers for encoding and decoding Avro blobs according to their defined
// specification.  Their state is created during initialization, but then never modified, so the
// same codec may be safely used in multiple go routines to encode and or decode different Avro
// streams concurrently.
type codec struct {
	name *Name

	binaryDecoder func([]byte) (interface{}, []byte, error)
	binaryEncoder func([]byte, interface{}) ([]byte, error)

	// textDecoder func([]byte) (interface{}, []byte, error)
	// textEncoder func([]byte, interface{}) ([]byte, error)

	// NOTE: When encoding unions, encoder attempts to encode using named type codecs prior to
	// unnamed types.
	namedType bool
}

// NewCodec returns a Codec that can encode and decode the specified Avro schema.
func NewCodec(schemaSpecification string) (BinaryCoder, error) {
	// pre-load the cache with primitive types
	st := &symtab{cache: map[string]*codec{
		"boolean": &codec{name: &Name{"boolean", ""}, binaryDecoder: booleanDecoder, binaryEncoder: booleanEncoder},
		"bytes":   &codec{name: &Name{"bytes", ""}, binaryDecoder: bytesDecoder, binaryEncoder: bytesEncoder},
		"double":  &codec{name: &Name{"double", ""}, binaryDecoder: doubleDecoder, binaryEncoder: doubleEncoder},
		"float":   &codec{name: &Name{"float", ""}, binaryDecoder: floatDecoder, binaryEncoder: floatEncoder},
		"int":     &codec{name: &Name{"int", ""}, binaryDecoder: intDecoder, binaryEncoder: intEncoder},
		"long":    &codec{name: &Name{"long", ""}, binaryDecoder: longDecoder, binaryEncoder: longEncoder},
		"null":    &codec{name: &Name{"null", ""}, binaryDecoder: nullDecoder, binaryEncoder: nullEncoder},
		"string":  &codec{name: &Name{"string", ""}, binaryDecoder: stringDecoder, binaryEncoder: stringEncoder},
	}}

	// NOTE: Some clients might give us unadorned primitive type name for the schema, e.g., "long".
	// While it is not valid JSON, it is a valid schema.  Provide special handling for primitive
	// type names.
	if cd, ok := st.cache[schemaSpecification]; ok {
		return cd, nil
	}

	var schema interface{}
	if err := json.Unmarshal([]byte(schemaSpecification), &schema); err != nil {
		return nil, fmt.Errorf("cannot unmarshal JSON: %s", err)
	}

	codec, err := st.buildCodec("", schema)
	// fmt.Printf("DEBUG: symtab: %v\n", st)
	return codec, err
}

// BinaryDecode decodes the provided byte slice in accordance with the Codec's Avro schema.  On success,
// it returns the decoded value, along with a new byte slice with the decoded bytes consumed.  In
// other words, when decoding an Avro int that happens to take 3 bytes, the returned byte slice will
// be like the original byte slice, but with the first three bytes removed.  On error, it returns
// the original byte slice without any bytes consumed and the error.
func (cd codec) BinaryDecode(buf []byte) (interface{}, []byte, error) {
	value, newBuf, err := cd.binaryDecoder(buf)
	if err != nil {
		return nil, buf, err
	}
	return value, newBuf, nil
}

// BinaryEncode encodes the provided datum value in accordance with the Codec's Avro schema.  It takes a
// byte slice to which to append the encoded bytes.  On success, it returns the new byte slice with
// the appended byte slice.  On error, it returns the original byte slice without any encoded bytes.
func (cd codec) BinaryEncode(buf []byte, datum interface{}) ([]byte, error) {
	newBuf, err := cd.binaryEncoder(buf, datum)
	if err != nil {
		return buf, err
	}
	return newBuf, nil
}

// symtab represents a set of Avro full names to the Codec that processes that schema type.
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
	case "record":
		return st.makeRecordCodec(enclosingNamespace, schema)
	default:
		return nil, fmt.Errorf("cannot build codec for unknown schema type: %s", schemaType)
	}
}

// notion of enclosing namespace changes when record, enum, or fixed create a new namespace, for child objects.
func (st symtab) registerCodec(c *codec, schemaMap map[string]interface{}, enclosingNamespace string) error {
	n, err := newNameFromSchemaMap(enclosingNamespace, schemaMap)
	if err != nil {
		return err
	}
	st.cache[n.FullName] = c
	c.name = n // reach back into codec and set its name
	return nil
}

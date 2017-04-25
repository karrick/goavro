package goavro

import (
	"encoding/json"
	"fmt"
	"sort"
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
}

// NewCodec returns a Codec that can encode and decode the specified Avro schema.
func NewCodec(schemaSpecification string) (BinaryCoder, error) {
	st := newSymbolTable()

	// NOTE: Some clients might give us unadorned primitive type name for the schema, e.g., "long".
	// While it is not valid JSON, it is a valid schema.  Provide special handling for primitive
	// type names.
	if c, ok := st.cache[schemaSpecification]; ok {
		return c, nil
	}

	var schema interface{}
	if err := json.Unmarshal([]byte(schemaSpecification), &schema); err != nil {
		return nil, fmt.Errorf("cannot unmarshal JSON: %s", err)
	}

	c, err := st.buildCodec(nullNamespace, schema)
	if false {
		tns := st.typeNames()
		sort.Strings(tns)
		for _, tn := range tns {
			fmt.Println(tn)
		}
	}
	return c, err
}

// BinaryDecode decodes the provided byte slice in accordance with the Codec's Avro schema.  On success,
// it returns the decoded value, along with a new byte slice with the decoded bytes consumed.  In
// other words, when decoding an Avro int that happens to take 3 bytes, the returned byte slice will
// be like the original byte slice, but with the first three bytes removed.  On error, it returns
// the original byte slice without any bytes consumed and the error.
func (c codec) BinaryDecode(buf []byte) (interface{}, []byte, error) {
	value, newBuf, err := c.binaryDecoder(buf)
	if err != nil {
		return nil, buf, err
	}
	return value, newBuf, nil
}

// BinaryEncode encodes the provided datum value in accordance with the Codec's Avro schema.  It takes a
// byte slice to which to append the encoded bytes.  On success, it returns the new byte slice with
// the appended byte slice.  On error, it returns the original byte slice without any encoded bytes.
func (c codec) BinaryEncode(buf []byte, datum interface{}) ([]byte, error) {
	newBuf, err := c.binaryEncoder(buf, datum)
	if err != nil {
		return buf, err
	}
	return newBuf, nil
}

package goavro

import (
	"errors"
	"fmt"
	"reflect"
)

type unionEncoder struct {
	encoder func([]byte, interface{}) ([]byte, error)
	index   int64
}

func (st symtab) buildCodecForTypeDescribedBySlice(enclosingNamespace string, schemaArray []interface{}) (*codec, error) {
	if len(schemaArray) == 0 {
		return nil, errors.New("cannot create union codec without any members")
	}

	encoderFromName := make(map[string]unionEncoder)
	decoderFromIndex := make([]func([]byte) (interface{}, []byte, error), len(schemaArray))
	allowedNames := make([]string, len(schemaArray))

	for i, unionMemberSchema := range schemaArray {
		unionMemberCodec, err := st.buildCodec(enclosingNamespace, unionMemberSchema)
		if err != nil {
			// TODO: error message needs more surrounding context of where we are in schema
			return nil, fmt.Errorf("cannot create union codec for item: %d; %s", i, err)
		}

		decoderFromIndex[i] = unionMemberCodec.decoder
		allowedNames[i] = unionMemberCodec.name
		encoderFromName[unionMemberCodec.name] = unionEncoder{
			encoder: unionMemberCodec.encoder,
			index:   int64(i),
		}
	}

	return &codec{
		name: "union (FIXME)",
		decoder: func(buf []byte) (interface{}, []byte, error) {
			var decoded interface{}
			var err error

			decoded, buf, err = longDecoder(buf)
			if err != nil {
				return nil, buf, err
			}
			index := decoded.(int64) // longDecoder always returns int64, so elide error checking
			if index < 0 || index >= int64(len(decoderFromIndex)) {
				return nil, buf, fmt.Errorf("cannot decode union: index must be between 0 and %d: read index: %d", len(decoderFromIndex)-1, index)
			}
			return decoderFromIndex[index](buf)
		},
		encoder: func(buf []byte, datum interface{}) ([]byte, error) {
			var err error
			var candidate string
			var candidates []string
			var ue unionEncoder
			var ok bool

			// NOTE: To allow greater client flexibility, when we receive a particular Go native
			// data type, there are a few possible candidate types that can encode the native type.
			// In these cases, we check whether the union supports any of the listed candidates, and
			// use that encoder if so.  For instance, if we receive a Go `int` native type, its
			// value could be encoded as an Avro int, long, float, or double.
			switch datum.(type) {
			case nil:
				candidates = []string{"null"}
			case int:
				candidates = []string{"int", "long", "float", "double"}
			case int64:
				candidates = []string{"long", "int", "double", "float"}
			case int32:
				candidates = []string{"int", "long", "float", "double"}
			case float64:
				candidates = []string{"double", "float", "long", "int"}
			case float32:
				candidates = []string{"float", "double", "long", "int"}
			case string:
				candidates = []string{"string", "bytes"} // add "fixed"
			case []byte:
				candidates = []string{"bytes", "string"} // add "fixed"
			case map[string]interface{}:
				candidates = []string{"map (FIXME)"}
			case []interface{}:
				candidates = []string{"array (FIXME)"}
			default:
				// NOTE: If given any sort of slice, zip values to items as convenience to client.
				if v := reflect.ValueOf(datum); v.Kind() != reflect.Slice {
					return buf, fmt.Errorf("cannot encode union: received: %T", datum)
				}
				candidates = []string{"array (FIXME)"}
			}

			// pick first candidate that matches possible candidate list
			for _, candidate = range candidates {
				if ue, ok = encoderFromName[candidate]; ok {
					break
				}
			}
			if !ok {
				return buf, fmt.Errorf("cannot encode union: acceptable: %v; received: %T", candidates, datum)
			}
			// encode union member index
			buf, _ = longEncoder(buf, ue.index)
			// encode datum value
			if buf, err = ue.encoder(buf, datum); err != nil {
				return buf, fmt.Errorf("cannot encode union value as Avro type: %q; %s", candidate, err)
			}
			return buf, nil
		},
	}, nil
}

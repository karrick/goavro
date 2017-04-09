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

func (st symtab) buildCodecForTypeDescribedBySlice(namespace string, schemaArray []interface{}) (*codec, error) {
	if len(schemaArray) == 0 {
		return nil, errors.New("cannot create union codec without any members")
	}

	encoderFromName := make(map[string]unionEncoder)
	decoderFromIndex := make([]func([]byte) (interface{}, []byte, error), len(schemaArray))
	allowedNames := make([]string, len(schemaArray))

	for i, unionMemberSchema := range schemaArray {
		unionMemberCodec, err := st.buildCodec(namespace, unionMemberSchema)
		if err != nil {
			// TODO: error message needs more surrounding context of where we are
			return nil, fmt.Errorf("cannot create union codec: item %d; %s", i, err)
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
			index := decoded.(int64)
			if index < 0 || index >= int64(len(decoderFromIndex)) {
				return nil, buf, fmt.Errorf("cannot decode union: index must be between 0 and %d: read index: %d", len(decoderFromIndex)-1, index)
			}
			return decoderFromIndex[index](buf)
		},
		encoder: func(buf []byte, datum interface{}) ([]byte, error) {
			var err error
			var candidates []string
			var ue unionEncoder
			var ok bool

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
				// NOTE: If given any sort of slice, zip values to items
				v := reflect.ValueOf(datum)
				if v.Kind() != reflect.Slice {
					return buf, fmt.Errorf("cannot encode union: received: %T", datum)
				}
				candidates = []string{"array (FIXME)"}
			}

			// pick first candidate that matches
			for _, candidate := range candidates {
				if ue, ok = encoderFromName[candidate]; ok {
					break
				}
			}
			if !ok {
				return buf, fmt.Errorf("cannot encode union: acceptable: %v; received: %T", candidates, datum)
			}

			buf, _ = longEncoder(buf, ue.index)
			if buf, err = ue.encoder(buf, datum); err != nil {
				return buf, fmt.Errorf("cannot encode union: %s", err)
			}
			return buf, nil
		},
	}, nil
}

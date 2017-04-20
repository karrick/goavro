package goavro

import (
	"errors"
	"fmt"
)

func (st symtab) buildCodecForTypeDescribedBySlice(enclosingNamespace string, schemaArray []interface{}) (*codec, error) {
	if len(schemaArray) == 0 {
		return nil, errors.New("cannot create union codec without any members")
	}

	setOfNames := make(map[string]struct{})          // used during codec creation, then vanishes
	allowedTypes := make([]string, len(schemaArray)) // used for error reporting when encoder receives invalid datum type
	codecs := make([]*codec, len(schemaArray))

	for i, unionMemberSchema := range schemaArray {
		unionMemberCodec, err := st.buildCodec(enclosingNamespace, unionMemberSchema)
		if err != nil {
			// TODO: error message needs more surrounding context of where we are in schema
			return nil, fmt.Errorf("cannot create union codec for item: %d; %s", i, err)
		}
		if _, ok := setOfNames[unionMemberCodec.name]; ok {
			return nil, fmt.Errorf("cannot create union: duplicate type: %s", unionMemberCodec.name)
		}
		setOfNames[unionMemberCodec.name] = struct{}{}
		allowedTypes[i] = unionMemberCodec.name
		codecs[i] = unionMemberCodec
	}

	return &codec{
		// name: "union",
		decoder: func(buf []byte) (interface{}, []byte, error) {
			var decoded interface{}
			var err error

			decoded, buf, err = longDecoder(buf)
			if err != nil {
				return nil, buf, err
			}
			index := decoded.(int64) // longDecoder always returns int64, so elide error checking
			if index < 0 || index >= int64(len(codecs)) {
				return nil, buf, fmt.Errorf("cannot decode union: index must be between 0 and %d: read index: %d", len(codecs)-1, index)
			}
			return codecs[index].decoder(buf)
		},
		encoder: func(buf []byte, datum interface{}) ([]byte, error) {
			var err error
			originalLength := len(buf)

			for i, codec := range codecs {
				buf, _ = longEncoder(buf, i)
				if buf, err = codec.encoder(buf, datum); err == nil {
					return buf, nil // codec able to encode datum
				}
				buf = buf[:originalLength] // reset buf and try with next encoder in list
			}
			return buf, fmt.Errorf("cannot encode union value: no union types in schema support datum: allowed types: %v; received: %T", allowedTypes, datum)
		},
	}, nil
}

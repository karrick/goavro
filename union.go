package goavro

import (
	"errors"
	"fmt"
)

func (st symtab) buildCodecForTypeDescribedBySlice(enclosingNamespace string, schemaArray []interface{}) (*codec, error) {
	if len(schemaArray) == 0 {
		return nil, errors.New("cannot create union codec without any members")
	}

	allowedTypes := make([]string, len(schemaArray)) // used for error reporting when encoder receives invalid datum type
	indexFromNamedType := make(map[string]int, len(schemaArray))
	indexFromUnnamedType := make(map[string]int, len(schemaArray))
	codecFromIndex := make([]*codec, len(schemaArray))

	for i, unionMemberSchema := range schemaArray {
		unionMemberCodec, err := st.buildCodec(enclosingNamespace, unionMemberSchema)
		if err != nil {
			// TODO: error message needs more surrounding context of where we are in schema
			return nil, fmt.Errorf("cannot create union codec for item: %d; %s", i, err)
		}
		fullName := unionMemberCodec.name.FullName
		if _, ok := indexFromNamedType[fullName]; ok {
			return nil, fmt.Errorf("cannot create union: duplicate type: %s", unionMemberCodec.name)
		}
		if _, ok := indexFromUnnamedType[fullName]; ok {
			return nil, fmt.Errorf("cannot create union: duplicate type: %s", unionMemberCodec.name)
		}
		if unionMemberCodec.namedType {
			indexFromNamedType[fullName] = i
		} else {
			indexFromUnnamedType[fullName] = i
		}
		allowedTypes[i] = fullName
		codecFromIndex[i] = unionMemberCodec
	}

	return &codec{
		name: &Name{"union", ""}, // ???
		binaryDecoder: func(buf []byte) (interface{}, []byte, error) {
			var decoded interface{}
			var err error

			decoded, buf, err = longDecoder(buf)
			if err != nil {
				return nil, buf, err
			}
			index := decoded.(int64) // longDecoder always returns int64, so elide error checking
			if index < 0 || index >= int64(len(codecFromIndex)) {
				return nil, buf, fmt.Errorf("cannot decode union: index must be between 0 and %d: read index: %d", len(codecFromIndex)-1, index)
			}
			c := codecFromIndex[index]
			decoded, buf, err = c.binaryDecoder(buf)
			if err != nil {
				return nil, buf, fmt.Errorf("cannot decode union: item %d; %s", index, err)
			}
			if decoded == nil {
				return nil, buf, nil
			}
			return decoded, buf, nil
		},
		binaryEncoder: func(buf []byte, datum interface{}) ([]byte, error) {
			var err error
			originalLength := len(buf)

			// ??? why have all this stuff ???  because when encoding a datum into a union, the
			// client must be able to specify whether a particular datum is one union member type or
			// another. normally, it wouldn't matter. if the client throws a Go int into a union
			// that holds any numeric type, the old method is to loop through union member encoders
			// and choose the first one that can actually encode the provided datum value.  However,
			// when a schema allows either a map or a record, and the client provides a record,
			// which actually looks like a Go map, ideally the encoder will chose record type. when
			// the client provides a map, it would choose the map type. maybe the simplest answer is
			// always chose the record type first, and if that doesn't work, then try the map.

			// Try all named types first (record, enum, fixed) (not sure if best solution; might not need for enum or fixed)
			for _, index := range indexFromNamedType {
				c := codecFromIndex[index]
				buf, _ = longEncoder(buf, index)
				if buf, err = c.binaryEncoder(buf, datum); err == nil {
					return buf, nil // codec able to encode datum
				}
				buf = buf[:originalLength] // reset buf and try with next encoder in list
			}

			// Try all unnamed types last
			for _, index := range indexFromUnnamedType {
				c := codecFromIndex[index]
				buf, _ = longEncoder(buf, index)
				if buf, err = c.binaryEncoder(buf, datum); err == nil {
					return buf, nil // codec able to encode datum
				}
				buf = buf[:originalLength] // reset buf and try with next encoder in list
			}
			return buf, fmt.Errorf("cannot encode union value: no union types in schema support datum: allowed types: %v; received: %T", allowedTypes, datum)
		},
	}, nil
}

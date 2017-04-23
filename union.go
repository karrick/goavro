package goavro

import (
	"errors"
	"fmt"
)

// Union wraps a datum value in a map for encoding as a Union, as required by Union encoder.
func Union(name string, datum interface{}) interface{} {
	if datum == nil && name == "null" {
		return nil
	}
	return map[string]interface{}{name: datum}
}

func (st symtab) buildCodecForTypeDescribedBySlice(enclosingNamespace string, schemaArray []interface{}) (*codec, error) {
	if len(schemaArray) == 0 {
		return nil, errors.New("cannot create Union codec without any members")
	}

	allowedTypes := make([]string, len(schemaArray)) // used for error reporting when encoder receives invalid datum type
	codecFromIndex := make([]*codec, len(schemaArray))
	indexFromName := make(map[string]int, len(schemaArray))

	for i, unionMemberSchema := range schemaArray {
		unionMemberCodec, err := st.buildCodec(enclosingNamespace, unionMemberSchema)
		if err != nil {
			// TODO: error message needs more surrounding context of where we are in schema
			return nil, fmt.Errorf("cannot create Union codec for item: %d; %s", i, err)
		}
		fullName := unionMemberCodec.name.FullName
		if _, ok := indexFromName[fullName]; ok {
			return nil, fmt.Errorf("cannot create Union: duplicate type: %s", unionMemberCodec.name)
		}
		indexFromName[fullName] = i
		allowedTypes[i] = fullName
		codecFromIndex[i] = unionMemberCodec
	}

	return &codec{
		name: &Name{"union", nullNamespace},
		binaryDecoder: func(buf []byte) (interface{}, []byte, error) {
			var decoded interface{}
			var err error

			decoded, buf, err = longDecoder(buf)
			if err != nil {
				return nil, buf, err
			}
			index := decoded.(int64) // longDecoder always returns int64, so elide error checking
			if index < 0 || index >= int64(len(codecFromIndex)) {
				return nil, buf, fmt.Errorf("cannot decode Union: index must be between 0 and %d: read index: %d", len(codecFromIndex)-1, index)
			}
			c := codecFromIndex[index]
			decoded, buf, err = c.binaryDecoder(buf)
			if err != nil {
				return nil, buf, fmt.Errorf("cannot decode Union: item %d; %s", index, err)
			}
			if decoded == nil {
				return nil, buf, nil
			}
			return map[string]interface{}{allowedTypes[index]: decoded}, buf, nil
		},
		binaryEncoder: func(buf []byte, datum interface{}) ([]byte, error) {
			switch v := datum.(type) {
			case nil:
				index, ok := indexFromName["null"]
				if !ok {
					return buf, fmt.Errorf("cannot encode Union value: no Union types in schema support datum: allowed types: %v; received: %T", allowedTypes, datum)
				}
				return longEncoder(buf, index)
			case map[string]interface{}:
				if len(v) != 1 {
					return buf, fmt.Errorf("cannot encode Union value: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; %T", allowedTypes, datum)
				}
				// will execute exactly once
				for key, value := range v {
					index, ok := indexFromName[key]
					if !ok {
						return buf, fmt.Errorf("cannot encode Union value: no Union types in schema support datum: allowed types: %v; received: %T", allowedTypes, datum)
					}
					c := codecFromIndex[index]
					buf, _ = longEncoder(buf, index)
					return c.binaryEncoder(buf, value)
				}
			}
			return buf, fmt.Errorf("cannot encode Union value: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: %v; %T", allowedTypes, datum)
		},
	}, nil
}

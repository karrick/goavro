package goavro

import (
	"errors"
	"fmt"
)

func (st symtab) makeMapCodec(namespace string, schema interface{}) (*codec, error) {
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot create map codec: expected: map[string]interface{}; received: %T", schema)
	}
	// map type must have values
	v, ok := schemaMap["values"]
	if !ok {
		return nil, errors.New("cannot create map codec: ought to have values key")
	}
	valuesCodec, err := st.buildCodec(namespace, v)
	if err != nil {
		return nil, fmt.Errorf("cannot create map codec: cannot create codec for specified values type: %s", err)
	}

	return &codec{
		name: "map (FIXME)",
		decoder: func(buf []byte) (interface{}, []byte, error) {
			dataMap := make(map[string]interface{})
			var err error
			var value interface{}
			if value, buf, err = longDecoder(buf); err != nil {
				return nil, buf, fmt.Errorf("cannot decode map: cannot decode block count: %s", err)
			}
			blockCount := value.(int64)

			for blockCount != 0 {
				// NOTE: Negative block count means following
				// long is the block size, for which we have no
				// use.
				if blockCount < 0 {
					blockCount = -blockCount // convert to its positive equivalent
					if _, buf, err = longDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode map: cannot decode block size: %s", err)
					}
				}
				for i := int64(0); i < blockCount; i++ {
					if value, buf, err = stringDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode map: cannot decode key string: %s", err)
					}
					key := value.(string) // string decoder always returns a string
					if value, buf, err = valuesCodec.decoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode map: cannot decode value for key: %q; %s", key, err)
					}
					dataMap[key] = value
				}
				// decode next blockCount
				if value, buf, err = longDecoder(buf); err != nil {
					return nil, buf, fmt.Errorf("cannot decode map: cannot decode block count: %s", err)
				}
				blockCount = value.(int64)
			}
			return dataMap, buf, nil
		},
		encoder: func(buf []byte, datum interface{}) ([]byte, error) {
			dataMap, ok := datum.(map[string]interface{})
			if !ok {
				return buf, fmt.Errorf("cannot encode map: received: %T", datum)
			}
			if len(dataMap) > 0 {
				buf, _ = longEncoder(buf, len(dataMap))
				for k, v := range dataMap {
					if buf, err = stringEncoder(buf, k); err != nil {
						return buf, fmt.Errorf("cannot encode map: cannot encode key: %q; %s", k, err)
					}
					if buf, err = valuesCodec.Encode(buf, v); err != nil {
						return buf, fmt.Errorf("cannot encode map: cannot encode value: %v; %s", v, err)
					}
				}
			}
			buf, _ = longEncoder(buf, 0)
			return buf, nil
		},
	}, nil
}

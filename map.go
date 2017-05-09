package goavro

import (
	"errors"
	"fmt"
)

func makeMapCodec(st map[string]*Codec, namespace string, schemaMap map[string]interface{}) (*Codec, error) {
	// map type must have values
	valueSchema, ok := schemaMap["values"]
	if !ok {
		return nil, errors.New("Map ought to have values key")
	}
	valueCodec, err := buildCodec(st, namespace, valueSchema)
	if err != nil {
		return nil, fmt.Errorf("Map values ought to be valid Avro type: %s", err)
	}

	return &Codec{
		typeName: &name{"map", nullNamespace},
		binaryDecoder: func(buf []byte) (interface{}, []byte, error) {
			var err error
			var value interface{}

			if value, buf, err = longDecoder(buf); err != nil {
				return nil, buf, fmt.Errorf("cannot decode Map block count: %s", err)
			}
			blockCount := value.(int64)

			// NOTE: While the attempt of a RAM optimization shown below is not
			// necessary, many encoders will encode all array items in a single
			// block.  We can optimize amount of RAM allocated by runtime for
			// the array by initializing the array for that number of items.
			initialSize := blockCount
			if initialSize < 0 {
				initialSize = -initialSize
			}
			mapValues := make(map[string]interface{}, initialSize)

			for blockCount != 0 {
				if blockCount < 0 {
					// NOTE: Negative block count means following long is the block size, for which
					// we have no use.
					blockCount = -blockCount // convert to its positive equivalent
					if _, buf, err = longDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode Map block size: %s", err)
					}
				}
				// Decode `blockCount` datum values from buffer
				for i := int64(0); i < blockCount; i++ {
					// first decode the key string
					if value, buf, err = stringDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode Map key: %s", err)
					}
					key := value.(string) // string decoder always returns a string
					// then decode the value
					if value, buf, err = valueCodec.binaryDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode Map value for key %q: %s", key, err)
					}
					mapValues[key] = value
				}
				// Decode next blockCount from buffer, because there may be more blocks
				if value, buf, err = longDecoder(buf); err != nil {
					return nil, buf, fmt.Errorf("cannot decode Map block count: %s", err)
				}
				blockCount = value.(int64)
			}
			return mapValues, buf, nil
		},
		binaryEncoder: func(buf []byte, datum interface{}) ([]byte, error) {
			mapValues, ok := datum.(map[string]interface{})
			if !ok {
				return buf, fmt.Errorf("cannot encode Map: expected: map[string]interface{}; received: %T", datum)
			}
			if len(mapValues) > 0 {
				// encode all map key-value pairs into a single block
				buf, _ = longEncoder(buf, len(mapValues))
				for k, v := range mapValues {
					// stringEncoder only fails when given non string, so elide error checking
					buf, _ = stringEncoder(buf, k)
					// encode the pair value
					if buf, err = valueCodec.binaryEncoder(buf, v); err != nil {
						return buf, fmt.Errorf("cannot encode Map value for key %q: %v: %s", k, v, err)
					}
				}
			}
			// always end with final blockCount of 0
			return longEncoder(buf, 0)
		},
	}, nil
}

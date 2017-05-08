package goavro

import (
	"fmt"
	"reflect"
)

func makeArrayCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	// array type must have items
	itemSchema, ok := schemaMap["items"]
	if !ok {
		return nil, fmt.Errorf("Array ought to have items key")
	}
	itemCodec, err := buildCodec(st, enclosingNamespace, itemSchema)
	if err != nil {
		return nil, fmt.Errorf("Array items ought to be valid Avro type: %s", err)
	}

	return &Codec{
		typeName: &name{"array", nullNamespace},
		binaryDecoder: func(buf []byte) (interface{}, []byte, error) {
			var value interface{}
			var err error

			if value, buf, err = longDecoder(buf); err != nil {
				return nil, buf, fmt.Errorf("cannot decode Array block count: %s", err)
			}
			blockCount := value.(int64)

			// NOTE: While below RAM optimization not necessary, many encoders will encode all
			// array items in a single block.  We can optimize amount of RAM allocated by
			// runtime for the array by initializing the array for that number of items.
			initialSize := blockCount
			if initialSize < 0 {
				initialSize = -initialSize
			}
			if initialSize > MaxAllocationSize {
				return nil, buf, fmt.Errorf("array: implementation error: length of array (%d) is greater than the max currently MaxAllocationSize (%d)", initialSize, MaxAllocationSize)
			}
			arrayValues := make([]interface{}, 0, initialSize)

			for blockCount != 0 {
				if blockCount < 0 {
					// NOTE: Negative block count means following long is the block size, for which
					// we have no use.  Read its value and discard.
					blockCount = -blockCount // convert to its positive equivalent
					if _, buf, err = longDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode Array block size: %s", err)
					}
				}
				// Decode `blockCount` datum values from buffer
				for i := int64(0); i < blockCount; i++ {
					if value, buf, err = itemCodec.binaryDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode Array item %d: %s", i+1, err)
					}
					arrayValues = append(arrayValues, value)
				}
				// Decode next blockCount from buffer, because there may be more blocks
				if value, buf, err = longDecoder(buf); err != nil {
					return nil, buf, fmt.Errorf("cannot decode Array block count: %s", err)
				}
				blockCount = value.(int64)
			}
			return arrayValues, buf, nil
		},
		binaryEncoder: func(buf []byte, datum interface{}) ([]byte, error) {
			var arrayValues []interface{}
			switch i := datum.(type) {
			case []interface{}:
				arrayValues = i
			default:
				// NOTE: If given any sort of slice, zip values to items as convenience to client.
				v := reflect.ValueOf(datum)
				if v.Kind() != reflect.Slice {
					return buf, fmt.Errorf("Array: expected []interface{}; received: %T", datum)
				}
				// NOTE: Two better alternatives to the current algorithm are:
				//   (1) mutate the reflection tuple underneath to convert the []int, for example,
				//       to []interface{}, with O(1) complexity
				//   (2) use copy builtin to zip the data items over, much like what gorrd does,
				//       with O(n) complexity, but more efficient than what's below.
				arrayValues = make([]interface{}, v.Len())
				for idx := 0; idx < v.Len(); idx++ {
					arrayValues[idx] = v.Index(idx).Interface()
				}
			}
			if len(arrayValues) > 0 {
				buf, _ = longEncoder(buf, len(arrayValues))
				for i, item := range arrayValues {
					if buf, err = itemCodec.binaryEncoder(buf, item); err != nil {
						return buf, fmt.Errorf("cannot encode Array item %d; %v: %s", i+1, item, err)
					}
				}
			}
			return longEncoder(buf, 0)
		},
	}, nil
}

package goavro

import (
	"fmt"
	"reflect"
)

func (st symtab) makeArrayCodec(namespace string, schema interface{}) (*codec, error) {
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot create array codec: expected: map[string]interface{}; received: %T", schema)
	}
	v, ok := schemaMap["items"]
	if !ok {
		return nil, fmt.Errorf("cannot create array codec: ought to have items key")
	}
	valuesCodec, err := st.buildCodec(namespace, v)
	if err != nil {
		return nil, fmt.Errorf("cannot create array codec: cannot create codec for specified items type: %s", err)
	}

	return &codec{
		name: "array (FIXME)",
		decoder: func(buf []byte) (interface{}, []byte, error) {
			var dataArray []interface{}
			var err error
			var value interface{}
			if value, buf, err = longDecoder(buf); err != nil {
				return nil, buf, fmt.Errorf("cannot decode array: cannot decode block count: %s", err)
			}
			blockCount := value.(int64)

			for blockCount != 0 {
				// NOTE: Negative block count means following
				// long is the block size, for which we have no
				// use.
				if blockCount < 0 {
					blockCount = -blockCount // convert to its positive equivalent
					if _, buf, err = longDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode array: cannot decode block size: %s", err)
					}
				}
				for i := int64(0); i < blockCount; i++ {
					if value, buf, err = valuesCodec.decoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode array: cannot decode item: %i; %s", i, err)
					}
					dataArray = append(dataArray, value)
				}
				// decode next blockCount
				if value, buf, err = longDecoder(buf); err != nil {
					return nil, buf, fmt.Errorf("cannot decode array: cannot decode block count: %s", err)
				}
				blockCount = value.(int64)
			}
			return dataArray, buf, nil

		},
		encoder: func(buf []byte, datum interface{}) ([]byte, error) {
			var items []interface{}
			switch i := datum.(type) {
			case []interface{}:
				items = i
			default:
				// NOTE: If given any sort of slice, zip values to items
				v := reflect.ValueOf(datum)
				if v.Kind() != reflect.Slice {
					return buf, fmt.Errorf("cannot encode array: received: %T", datum)
				}
				// NOTE: Two better alternatives to the current algorithm are:
				//   (1) mutate the reflection tuple underneath to convert the []int, for example,
				//       to []interface{}, with O(1) complexity
				//   (2) sue copy builtin to zip the data items over, much like what gorrd does,
				//       with O(n) complexity, but more efficient than what's below.
				items = make([]interface{}, v.Len())
				for idx := 0; idx < v.Len(); idx++ {
					items[idx] = v.Index(idx).Interface()
				}
			}
			if len(items) > 0 {
				buf, _ = longEncoder(buf, len(items))
				for i, item := range items {
					if buf, err = valuesCodec.encoder(buf, item); err != nil {
						return buf, fmt.Errorf("cannot encode array: cannot encode item: %d; %v; %s", i, item, err)
					}
				}
			}
			buf, _ = longEncoder(buf, 0)
			return buf, nil
		},
	}, nil
}

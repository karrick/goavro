package goavro

import (
	"errors"
	"fmt"
	"io"
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

			// block count and block size
			if value, buf, err = longDecoder(buf); err != nil {
				return nil, buf, fmt.Errorf("cannot decode Map block count: %s", err)
			}
			blockCount := value.(int64)
<<<<<<< HEAD
			if blockCount < 0 {
				// NOTE: A negative block count implies there is a long encoded
				// block size following the negative block count. We have no use
				// for the block size in this decoder, so we read and discard
				// the value.
				blockCount = -blockCount // convert to its positive equivalent
				if _, buf, err = longDecoder(buf); err != nil {
					return nil, buf, fmt.Errorf("cannot decode Map block size: %s", err)
				}
			}
			// Ensure block count does not exceed some sane value.
			if blockCount > MaxBlockCount {
				return nil, buf, fmt.Errorf("cannot decode Map when block count exceeds MaxBlockCount: %d > %d", blockCount, MaxBlockCount)
=======

			// NOTE: While the attempt of a RAM optimization shown below is not
			// necessary, many encoders will encode all array items in a single
			// block.  We can optimize amount of RAM allocated by runtime for
			// the array by initializing the array for that number of items.
			initialSize := blockCount
			if initialSize < 0 {
				initialSize = -initialSize
>>>>>>> comments and error messages
			}
			// NOTE: While the attempt of a RAM optimization shown below is not
			// necessary, many encoders will encode all items in a single block.
			// We can optimize amount of RAM allocated by runtime for the array
			// by initializing the array for that number of items.
			mapValues := make(map[string]interface{}, blockCount)

			for blockCount != 0 {
				// Decode `blockCount` datum values from buffer
				for i := int64(0); i < blockCount; i++ {
					// first decode the key string
					if value, buf, err = stringDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode Map key: %s", err)
					}
					key := value.(string) // string decoder always returns a string
					if _, ok := mapValues[key]; ok {
						return nil, buf, fmt.Errorf("cannot decode Map: duplicate key: %q", key)
					}
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
				if blockCount < 0 {
					// NOTE: A negative block count implies there is a long
					// encoded block size following the negative block count. We
					// have no use for the block size in this decoder, so we
					// read and discard the value.
					blockCount = -blockCount // convert to its positive equivalent
					if _, buf, err = longDecoder(buf); err != nil {
						return nil, buf, fmt.Errorf("cannot decode Map block size: %s", err)
					}
				}
				// Ensure block count does not exceed some sane value.
				if blockCount > MaxBlockCount {
					return nil, buf, fmt.Errorf("cannot decode Map when block count exceeds MaxBlockCount: %d > %d", blockCount, MaxBlockCount)
				}
			}
			return mapValues, buf, nil
		},
		binaryEncoder: func(buf []byte, datum interface{}) ([]byte, error) {
			mapValues, ok := datum.(map[string]interface{})
			if !ok {
				return buf, fmt.Errorf("cannot encode Map: expected: map[string]interface{}; received: %T", datum)
			}

			keyCount := int64(len(mapValues))
			var alreadyEncoded, remainingInBlock int64

			for k, v := range mapValues {
				if remainingInBlock == 0 { // start a new block
					remainingInBlock = keyCount - alreadyEncoded
					if remainingInBlock > MaxBlockCount {
						// limit block count to MacBlockCount
						remainingInBlock = MaxBlockCount
					}
					buf, _ = longEncoder(buf, remainingInBlock)
				}

				// only fails when given non string, so elide error checking
				buf, _ = stringEncoder(buf, k)

				// encode the value
				if buf, err = valueCodec.binaryEncoder(buf, v); err != nil {
					return buf, fmt.Errorf("cannot encode Map value for key %q: %v: %s", k, v, err)
				}

				remainingInBlock--
				alreadyEncoded++
			}
			return longEncoder(buf, 0) // append tailing 0 block count to signal end of Map
		},
		textDecoder: func(buf []byte) (interface{}, []byte, error) {
			return genericMapTextDecoder(buf, valueCodec, nil) // codecFromKey == nil
		},
		textEncoder: func(buf []byte, datum interface{}) ([]byte, error) {
			return genericMapTextEncoder(buf, datum, valueCodec, nil)
		},
	}, nil
}

func genericMapTextDecoder(buf []byte, defaultCodec *Codec, codecFromKey map[string]*Codec) (map[string]interface{}, []byte, error) {
	var value interface{}
	var err error
	var b byte

	if buf, err = gobble(buf, '{'); err != nil {
		return nil, buf, err
	}

	lencodec := len(codecFromKey)
	mapValues := make(map[string]interface{}, lencodec)

	// NOTE: Also terminates when read '}' byte.
	for len(buf) > 0 {
		if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
			return nil, buf, io.ErrShortBuffer
		}
		// decode key string
		value, buf, err = stringTextDecoder(buf)
		if err != nil {
			return nil, buf, fmt.Errorf("cannot decode Map: expected key: %s", err)
		}
		key := value.(string)
		// Is key already used?
		if _, ok := mapValues[key]; ok {
			return nil, buf, fmt.Errorf("cannot decode Map: duplicate key: %q", key)
		}
		// Find a codec for the key
		fieldCodec := codecFromKey[key]
		if fieldCodec == nil {
			fieldCodec = defaultCodec
		}
		if fieldCodec == nil {
			return nil, buf, fmt.Errorf("cannot decode Map: cannot determine codec: %q", key)
		}
		// decode colon
		if buf, err = gobble(buf, ':'); err != nil {
			return nil, buf, err
		}
		// decode value
		if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
			return nil, buf, io.ErrShortBuffer
		}
		value, buf, err = fieldCodec.textDecoder(buf)
		if err != nil {
			return nil, buf, err
		}
		mapValues[key] = value
		// either comma or closing curly brace
		if buf, _ = advanceToNonWhitespace(buf); len(buf) == 0 {
			return nil, buf, io.ErrShortBuffer
		}
		switch b = buf[0]; b {
		case '}':
			return mapValues, buf[1:], nil
		case ',':
			buf = buf[1:]
		default:
			return nil, buf, fmt.Errorf("cannot decode Map: expected ',' or '}'; received: %q", b)
		}
	}
	return nil, buf, io.ErrShortBuffer
}

func genericMapTextEncoder(buf []byte, datum interface{}, defaultCodec *Codec, codecFromKey map[string]*Codec) ([]byte, error) {
	valueMap, ok := datum.(map[string]interface{})
	if !ok {
		return buf, fmt.Errorf("Map ought to be map[string]interface{}; received: %T", datum)
	}

	var err error

	buf = append(buf, '{')

	for key, value := range valueMap {
		// Find a codec for the key
		fieldCodec := codecFromKey[key]
		if fieldCodec == nil {
			fieldCodec = defaultCodec
		}
		if fieldCodec == nil {
			return buf, fmt.Errorf("cannot encode Map: cannot determine codec: %q", key)
		}
		// Encode key string
		buf, err = stringTextEncoder(buf, key)
		if err != nil {
			return buf, err
		}
		buf = append(buf, ':')
		// Encode value
		buf, err = fieldCodec.textEncoder(buf, value)
		if err != nil {
			// field was specified in datum; therefore its value was invalid
			return buf, fmt.Errorf("Map value for %q does not match its schema: %s", key, err)
		}
		buf = append(buf, ',')
	}

	return append(buf[:len(buf)-1], '}'), nil
}

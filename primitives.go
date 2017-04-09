package goavro

import (
	"encoding/binary"
	"fmt"
	"math"
)

func booleanDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 1 {
		return nil, nil, fmt.Errorf("cannot decode boolean: buffer underflow")
	}
	var b byte
	b, buf = buf[0], buf[1:]
	switch b {
	case byte(0):
		return false, buf, nil
	case byte(1):
		return true, buf, nil
	default:
		return nil, buf, fmt.Errorf("cannot decode boolean: received byte: %d", b)
	}
}

func booleanEncoder(buf []byte, datum interface{}) ([]byte, error) {
	value, ok := datum.(bool)
	if !ok {
		return buf, fmt.Errorf("cannot encode boolean: received: %T", datum)
	}
	var b byte
	if value {
		b = 1
	}
	return append(buf, b), nil
}

func bytesDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 1 {
		return nil, nil, fmt.Errorf("cannot decode bytes: buffer underflow")
	}
	var decoded interface{}
	var err error
	if decoded, buf, err = longDecoder(buf); err != nil {
		return nil, buf, fmt.Errorf("cannot decode bytes: %s", err)
	}
	size := decoded.(int64)
	if size < 0 {
		return nil, buf, fmt.Errorf("cannot decode bytes: negative length: %d", size)
	}
	if size > int64(len(buf)) {
		return nil, buf, fmt.Errorf("cannot decode bytes: length exceeds remaining buffer size: %d > %d", size, len(buf))
	}
	return buf[:size], buf[size:], nil
}

func bytesEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value []byte
	switch v := datum.(type) {
	case []byte:
		value = v
	case string:
		value = []byte(v)
	default:
		return buf, fmt.Errorf("cannot encode bytes: received: %T", v)
	}
	// longEncoder only fails when given non int, so elide error checking
	buf, _ = longEncoder(buf, len(value))
	return append(buf, value...), nil
}

func appendFloat(buf []byte, bits uint64, byteCount int) ([]byte, error) {
	for i := 0; i < byteCount; i++ {
		buf = append(buf, byte(bits&255))
		bits = bits >> 8
	}
	return buf, nil
}

const doubleEncodedLength = 8 // double requires 8 bytes

func doubleDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < doubleEncodedLength {
		return nil, nil, fmt.Errorf("cannot decode double: buffer underflow")
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(buf[:doubleEncodedLength])), buf[doubleEncodedLength:], nil
}

func doubleEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value float64
	switch v := datum.(type) {
	case float64:
		value = v
	case float32:
		value = float64(v)
	case int:
		value = float64(v)
	case int64:
		value = float64(v)
	case int32:
		value = float64(v)
	default:
		return buf, fmt.Errorf("cannot encode double: received %T", datum)
	}
	return appendFloat(buf, uint64(math.Float64bits(value)), doubleEncodedLength)
}

const floatEncodedLength = 4 // float requires 4 bytes

func floatDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < floatEncodedLength {
		return nil, nil, fmt.Errorf("cannot decode float: buffer underflow")
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(buf[:floatEncodedLength])), buf[floatEncodedLength:], nil
}

func floatEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value float32
	switch v := datum.(type) {
	case float32:
		value = v
	case float64:
		value = float32(v)
	case int:
		value = float32(v)
	case int64:
		value = float32(v)
	case int32:
		value = float32(v)
	default:
		return buf, fmt.Errorf("cannot encode float: received %T", datum)
	}
	return appendFloat(buf, uint64(math.Float32bits(value)), floatEncodedLength)
}

const (
	intDownShift  = uint32(31)
	intFlag       = byte(128)
	intMask       = byte(127)
	longDownShift = uint32(63)
)

func appendInt(buf []byte, encoded uint64) ([]byte, error) {
	if encoded == 0 {
		return append(buf, 0), nil
	}
	for encoded > 0 {
		b := byte(encoded) & intMask
		encoded = encoded >> 7
		if encoded != 0 {
			b |= intFlag // set high bit; we have more bytes
		}
		buf = append(buf, b)
	}
	return buf, nil
}

func intDecoder(buf []byte) (interface{}, []byte, error) {
	var offset, value int
	var shift uint
	for offset = 0; offset < len(buf); offset++ {
		b := buf[offset]
		value |= int(b&intMask) << shift
		if b&intFlag == 0 {
			return (int32(value>>1) ^ -int32(value&1)), buf[offset+1:], nil
		}
		shift += 7
	}
	return nil, nil, fmt.Errorf("cannot decode int: buffer underflow")
}

func intEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value int32
	switch v := datum.(type) {
	case int:
		value = int32(v)
	case int64:
		value = int32(v)
	case int32:
		value = v
	case float64:
		value = int32(v)
	case float32:
		value = int32(v)
	default:
		return buf, fmt.Errorf("cannot encode long: received: %T", datum)
	}
	encoded := uint64((uint32(value) << 1) ^ uint32(value>>intDownShift))
	return appendInt(buf, encoded)
}

func longDecoder(buf []byte) (interface{}, []byte, error) {
	var offset int
	var value uint64
	var shift uint
	for offset = 0; offset < len(buf); offset++ {
		b := buf[offset]
		value |= uint64(b&intMask) << shift
		if b&intFlag == 0 {
			return (int64(value>>1) ^ -int64(value&1)), buf[offset+1:], nil
		}
		shift += 7
	}
	return nil, nil, fmt.Errorf("cannot decode long: buffer underflow")
}

func longEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value int64
	switch v := datum.(type) {
	case int:
		value = int64(v)
	case int64:
		value = v
	case int32:
		value = int64(v)
	case float64:
		value = int64(v)
	case float32:
		value = int64(v)
	default:
		return buf, fmt.Errorf("cannot encode long: received: %T", datum)
	}
	encoded := (uint64(value) << 1) ^ uint64(value>>longDownShift)
	return appendInt(buf, encoded)
}

func nullDecoder(buf []byte) (interface{}, []byte, error) { return nil, buf, nil }

func nullEncoder(buf []byte, datum interface{}) ([]byte, error) {
	if datum != nil {
		return buf, fmt.Errorf("cannot encode null: received: %T", datum)
	}
	return buf, nil
}

func stringDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 1 {
		return nil, nil, fmt.Errorf("cannot decode string: buffer underflow")
	}
	var decoded interface{}
	var err error
	if decoded, buf, err = longDecoder(buf); err != nil {
		return nil, buf, err
	}
	size := decoded.(int64)
	if size < 0 {
		return nil, buf, fmt.Errorf("cannot decode string: negative length: %d", size)
	}
	if size > int64(len(buf)) {
		return nil, buf, fmt.Errorf("cannot decode string: length exceeds remaining buffer size: %d > %d", size, len(buf))
	}
	return string(buf[:size]), buf[size:], nil
}

func stringEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value []byte
	switch v := datum.(type) {
	case string:
		value = []byte(v)
	case []byte:
		value = v
	default:
		return buf, fmt.Errorf("cannot encode string: received: %T", v)
	}

	// longEncoder only fails when given non int, so elide error checking
	buf, _ = longEncoder(buf, len(value))
	return append(buf, value...), nil
}

package goavro

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

func booleanDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 1 {
		return nil, nil, io.ErrShortBuffer
	}
	var b byte
	b, buf = buf[0], buf[1:]
	switch b {
	case byte(0):
		return false, buf, nil
	case byte(1):
		return true, buf, nil
	default:
		return nil, buf, fmt.Errorf("boolean: expected: Go byte(0) or byte(1); received: byte(%d)", b)
	}
}

func booleanEncoder(buf []byte, datum interface{}) ([]byte, error) {
	value, ok := datum.(bool)
	if !ok {
		return buf, fmt.Errorf("boolean: expected: Go bool; received: %T", datum)
	}
	var b byte
	if value {
		b = 1
	}
	return append(buf, b), nil
}

func bytesDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 1 {
		return nil, nil, io.ErrShortBuffer
	}
	var decoded interface{}
	var err error
	if decoded, buf, err = longDecoder(buf); err != nil {
		return nil, buf, fmt.Errorf("bytes: %s", err)
	}
	size := decoded.(int64) // longDecoder always returns int64
	if size < 0 {
		return nil, buf, fmt.Errorf("bytes: negative length: %d", size)
	}
	if size > int64(len(buf)) {
		return nil, buf, io.ErrShortBuffer
	}
	return buf[:size], buf[size:], nil
}

// receives string and []byte transparently
func bytesEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value []byte
	switch v := datum.(type) {
	case []byte:
		value = v
	case string:
		value = []byte(v)
	default:
		return buf, fmt.Errorf("bytes: expected: Go string or []byte; received: %T", v)
	}
	// longEncoder only fails when given non int, so elide error checking
	buf, _ = longEncoder(buf, len(value))
	// append datum bytes
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
		return nil, nil, io.ErrShortBuffer
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(buf[:doubleEncodedLength])), buf[doubleEncodedLength:], nil
}

// receives any Go numeric type and casts to float64, possibly with data loss if the value the
// client sent is not represented in a float64.
func doubleEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value float64
	switch v := datum.(type) {
	case float64:
		value = v
	case float32:
		value = float64(v)
	case int:
		if int(float64(v)) != v {
			return buf, fmt.Errorf("double: provided Go int would lose precision: %d", v)
		}
		value = float64(v)
	case int64:
		if int64(float64(v)) != v {
			return buf, fmt.Errorf("double: provided Go int64 would lose precision: %d", v)
		}
		value = float64(v)
	case int32:
		if int32(float64(v)) != v {
			return buf, fmt.Errorf("double: provided Go int32 would lose precision: %d", v)
		}
		value = float64(v)
	default:
		return buf, fmt.Errorf("double: expected: Go numeric; received: %T", datum)
	}
	return appendFloat(buf, uint64(math.Float64bits(value)), doubleEncodedLength)
}

const floatEncodedLength = 4 // float requires 4 bytes

func floatDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < floatEncodedLength {
		return nil, nil, io.ErrShortBuffer
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(buf[:floatEncodedLength])), buf[floatEncodedLength:], nil
}

// receives any Go numeric type and casts to float32.  if cast is lossy, it returns an encoding
// error
func floatEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value float32
	switch v := datum.(type) {
	case float32:
		value = v
	case float64:
		// Assume runtime can cast special floats correctly
		if !math.IsNaN(v) && !math.IsInf(v, 1) && !math.IsInf(v, -1) && float64(float32(v)) != v {
			return buf, fmt.Errorf("float: provided Go double would lose precision: %f", v)
		}
		value = float32(v)
	case int:
		if int(float32(v)) != v {
			return buf, fmt.Errorf("float: provided Go int would lose precision: %d", v)
		}
		value = float32(v)
	case int64:
		if int64(float32(v)) != v {
			return buf, fmt.Errorf("float: provided Go int64 would lose precision: %d", v)
		}
		value = float32(v)
	case int32:
		if int32(float32(v)) != v {
			return buf, fmt.Errorf("float: provided Go int32 would lose precision: %d", v)
		}
		value = float32(v)
	default:
		return buf, fmt.Errorf("float: expected: Go numeric; received: %T", datum)
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
	return nil, nil, io.ErrShortBuffer
}

// receives any Go numeric type and casts to int32, possibly with data loss if the value the client
// sent is not represented in a int32.
func intEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value int32
	switch v := datum.(type) {
	case int:
		if int(int32(v)) != v {
			return buf, fmt.Errorf("int: provided Go int would lose precision: %d", v)
		}
		value = int32(v)
	case int64:
		if int64(int32(v)) != v {
			return buf, fmt.Errorf("int: provided Go int64 would lose precision: %d", v)
		}
		value = int32(v)
	case int32:
		value = v
	case float64:
		if float64(int32(v)) != v {
			return buf, fmt.Errorf("int: provided Go float64 would lose precision: %f", v)
		}
		value = int32(v)
	case float32:
		if float32(int32(v)) != v {
			return buf, fmt.Errorf("int: provided Go float32 would lose precision: %f", v)
		}
		value = int32(v)
	default:
		return buf, fmt.Errorf("long: expected: Go numeric; received: %T", datum)
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
	return nil, nil, io.ErrShortBuffer
}

// receives any Go numeric type and casts to int64, possibly with data loss if the value the client
// sent is not represented in a int64.
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
		if float64(int64(v)) != v {
			return buf, fmt.Errorf("long: provided Go float64 would lose precision: %f", v)
		}
		value = int64(v)
	case float32:
		if float32(int64(v)) != v {
			return buf, fmt.Errorf("long: provided Go float64 would lose precision: %f", v)
		}
		value = int64(v)
	default:
		return buf, fmt.Errorf("long: expected: Go numeric; received: %T", datum)
	}
	encoded := (uint64(value) << 1) ^ uint64(value>>longDownShift)
	return appendInt(buf, encoded)
}

func nullDecoder(buf []byte) (interface{}, []byte, error) { return nil, buf, nil }

func nullEncoder(buf []byte, datum interface{}) ([]byte, error) {
	if datum != nil {
		return buf, fmt.Errorf("null: expected: Go nil; received: %T", datum)
	}
	return buf, nil
}

func stringDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 1 {
		return nil, nil, io.ErrShortBuffer
	}
	var decoded interface{}
	var err error
	if decoded, buf, err = longDecoder(buf); err != nil {
		return nil, buf, err
	}
	size := decoded.(int64) // longDecoder always returns int64
	if size < 0 {
		return nil, buf, fmt.Errorf("string: negative length: %d", size)
	}
	if size > int64(len(buf)) {
		return nil, buf, io.ErrShortBuffer
	}
	return string(buf[:size]), buf[size:], nil
}

// receives string and []byte transparently
func stringEncoder(buf []byte, datum interface{}) ([]byte, error) {
	var value []byte
	switch v := datum.(type) {
	case string:
		value = []byte(v)
	case []byte:
		value = v
	default:
		return buf, fmt.Errorf("string: expected: Go string or []byte; received: %T", v)
	}
	// longEncoder only fails when given non int, so elide error checking
	buf, _ = longEncoder(buf, len(value))
	// append datum bytes
	return append(buf, value...), nil
}

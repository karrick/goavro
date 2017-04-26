package goavro_test

import "testing"

func TestSchemaArray(t *testing.T) {
	testSchemaValid(t, `{"type":"array","items":"bytes"}`)
}

func TestArrayItems(t *testing.T) {
	testSchemaInvalid(t, `{"type":"array","item":"int"}`, "Array ought to have items key")
	testSchemaInvalid(t, `{"type":"array","items":"integer"}`, "Array items ought to be valid Avro type")
	testSchemaInvalid(t, `{"type":"array","items":3}`, "Array items ought to be valid Avro type")
	testSchemaInvalid(t, `{"type":"array","items":int}`, "invalid character") // type name must be quoted
}

func TestArrayNull(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"array","items":"null"}`, []interface{}{}, []byte{0})
	testBinaryCodecPass(t, `{"type":"array","items":"null"}`, []interface{}{nil}, []byte{2, 0})
	testBinaryCodecPass(t, `{"type":"array","items":"null"}`, []interface{}{nil, nil}, []byte{4, 0})
}

func TestArrayReceiveSliceEmptyInterface(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"array","items":"boolean"}`, []interface{}{}, []byte{0})
	testBinaryCodecPass(t, `{"type":"array","items":"boolean"}`, []interface{}{false}, []byte{2, 0, 0})
	testBinaryCodecPass(t, `{"type":"array","items":"boolean"}`, []interface{}{true}, []byte{2, 1, 0})
	testBinaryCodecPass(t, `{"type":"array","items":"boolean"}`, []interface{}{false, false}, []byte{4, 0, 0, 0})
	testBinaryCodecPass(t, `{"type":"array","items":"boolean"}`, []interface{}{true, true}, []byte{4, 1, 1, 0})
}

func TestArrayReceiveSliceInt(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"array","items":"int"}`, []int{}, []byte{0})
	testBinaryCodecPass(t, `{"type":"array","items":"int"}`, []int{1}, []byte("\x02\x02\x00"))
	testBinaryCodecPass(t, `{"type":"array","items":"int"}`, []int{1, 2}, []byte("\x04\x02\x04\x00"))
}

func TestArrayBytes(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, []interface{}(nil), []byte{0})                           // item count == 0
	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, []interface{}{[]byte("foo")}, []byte("\x02\x06foo\x00")) // item count == 1, item 1 length == 3, foo, no more items
	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, []interface{}{[]byte("foo"), []byte("bar")}, []byte("\x04\x06foo\x06bar\x00"))

	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, [][]byte(nil), []byte{0})                           // item count == 0
	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, [][]byte{[]byte("foo")}, []byte("\x02\x06foo\x00")) // item count == 1, item 1 length == 3, foo, no more items
	testBinaryCodecPass(t, `{"type":"array","items":"bytes"}`, [][]byte{[]byte("foo"), []byte("bar")}, []byte("\x04\x06foo\x06bar\x00"))
}

func TestArrayEncodeError(t *testing.T) {
	// provided slice of primitive types that are not compatible with schema
	testBinaryEncodeFailBadDatumType(t, `{"type":"array","items":"int"}`, []string{"1"})
	testBinaryEncodeFailBadDatumType(t, `{"type":"array","items":"int"}`, []string{"1", "2"})
}

func TestArrayEncodeErrorFIXME(t *testing.T) {
	// NOTE: Would be better if returns error, however, because only the length is encoded, the
	// items encoder is never invoked to detect it is the wrong slice type
	if false {
		testBinaryEncodeFailBadDatumType(t, `{"type":"array","items":"int"}`, []string{})
	} else {
		testBinaryCodecPass(t, `{"type":"array","items":"int"}`, []string{}, []byte{0})
	}
}

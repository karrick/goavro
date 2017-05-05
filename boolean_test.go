package goavro_test

import "testing"

func TestSchemaPrimitiveCodecBoolean(t *testing.T) {
	testSchemaPrimativeCodec(t, "boolean")
}

func TestPrimitiveBooleanBinary(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, "boolean", 0)
	testBinaryEncodeFailBadDatumType(t, "boolean", 1)
	testBinaryDecodeFailShortBuffer(t, "boolean", nil)
	testBinaryCodecPass(t, "boolean", false, []byte{0})
	testBinaryCodecPass(t, "boolean", true, []byte{1})
}

func TestPrimitiveBooleanText(t *testing.T) {
	testTextEncodeFailBadDatumType(t, "boolean", 0)
	testTextEncodeFailBadDatumType(t, "boolean", 1)
	testTextDecodeFailShortBuffer(t, "boolean", nil)
	testTextCodecPass(t, "boolean", false, []byte("false"))
	testTextCodecPass(t, "boolean", true, []byte("true"))
}

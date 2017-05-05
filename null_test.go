package goavro_test

import "testing"

func TestSchemaPrimitiveNullCodec(t *testing.T) {
	testSchemaPrimativeCodec(t, "null")
}

func TestPrimitiveNullBinary(t *testing.T) {
	testBinaryEncodeFailBadDatumType(t, "null", false)
	testBinaryCodecPass(t, "null", nil, nil)
}

func TestPrimitiveNullText(t *testing.T) {
	testTextEncodeFailBadDatumType(t, "null", false)
	testTextCodecPass(t, "null", nil, []byte("null"))
}

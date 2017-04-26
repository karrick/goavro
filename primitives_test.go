package goavro_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/karrick/goavro"
)

func TestSchemaFailInvalidType(t *testing.T) {
	testSchemaInvalid(t, `{"type":"flubber"}`, "unknown type name")
}

func testPrimitiveCodec(t *testing.T, primitiveTypeName string) {
	if _, err := goavro.NewCodec(primitiveTypeName); err != nil {
		t.Errorf("Bare primitive type: Schema: %q; Actual: %#v; Expected: %#v", primitiveTypeName, err, nil)
	}
	quoted := `"` + primitiveTypeName + `"`
	if _, err := goavro.NewCodec(quoted); err != nil {
		t.Errorf("Bare primitive type: Schema: %q; Actual: %#v; Expected: %#v", quoted, err, nil)
	}
	full := fmt.Sprintf(`{"type":"%s"}`, primitiveTypeName)
	if _, err := goavro.NewCodec(full); err != nil {
		t.Errorf("Full primitive type: Schema: %q; Actual: %#v; Expected: %#v", full, err, nil)
	}
	extra := fmt.Sprintf(`{"type":"%s","ignoredKey":"ignoredValue"}`, primitiveTypeName)
	if _, err := goavro.NewCodec(extra); err != nil {
		t.Errorf("Full primitive type with extra attributes: Schema: %q; Actual: %#v; Expected: %#v", extra, err, nil)
	}
}

func TestPrimitiveBoolean(t *testing.T) {
	testPrimitiveCodec(t, "boolean")
	testBinaryEncodeFailBadDatumType(t, "boolean", 0)
	testBinaryEncodeFailBadDatumType(t, "boolean", 1)
	testBinaryDecodeFailBufferUnderflow(t, "boolean", nil)
	testBinaryCodecPass(t, "boolean", false, []byte{0})
	testBinaryCodecPass(t, "boolean", true, []byte{1})
}

func TestPrimitiveBytes(t *testing.T) {
	testPrimitiveCodec(t, "bytes")
	testBinaryEncodeFailBadDatumType(t, "bytes", 13)
	testBinaryDecodeFailBufferUnderflow(t, "bytes", nil)
	testBinaryDecodeFailBufferUnderflow(t, "bytes", []byte{2})
	testBinaryCodecPass(t, "bytes", []byte(""), []byte("\x00"))
	testBinaryCodecPass(t, "bytes", []byte("some bytes"), []byte("\x14some bytes"))
}

func TestPrimitiveDouble(t *testing.T) {
	testPrimitiveCodec(t, "double")
	testBinaryEncodeFailBadDatumType(t, "double", "some string")
	testBinaryDecodeFailBufferUnderflow(t, "double", []byte("\x00\x00\x00\x00\x00\x00\xf0"))
	testBinaryCodecPass(t, "double", 3.5, []byte("\x00\x00\x00\x00\x00\x00\f@"))
	testBinaryCodecPass(t, "double", math.Inf(-1), []byte("\x00\x00\x00\x00\x00\x00\xf0\xff"))
	testBinaryCodecPass(t, "double", math.Inf(1), []byte("\x00\x00\x00\x00\x00\x00\xf0\u007f"))
	testBinaryCodecPass(t, "double", math.NaN(), []byte("\x01\x00\x00\x00\x00\x00\xf8\u007f"))
}

func TestPrimitiveFloat(t *testing.T) {
	testPrimitiveCodec(t, "float")
	testBinaryEncodeFailBadDatumType(t, "float", "some string")
	testBinaryDecodeFailBufferUnderflow(t, "float", []byte("\x00\x00\x80"))
	testBinaryCodecPass(t, "float", 3.5, []byte("\x00\x00\x60\x40"))
	testBinaryCodecPass(t, "float", math.Inf(-1), []byte("\x00\x00\x80\xff"))
	testBinaryCodecPass(t, "float", math.Inf(1), []byte("\x00\x00\x80\u007f"))
	testBinaryCodecPass(t, "float", math.NaN(), []byte("\x00\x00\xc0\u007f"))
}

func TestPrimitiveInt(t *testing.T) {
	testPrimitiveCodec(t, "int")
	testBinaryEncodeFailBadDatumType(t, "int", "some string")
	testBinaryDecodeFailBufferUnderflow(t, "int", []byte{0xfd, 0xff, 0xff, 0xff})
	testBinaryCodecPass(t, "int", -1, []byte{0x01})
	testBinaryCodecPass(t, "int", -2147483647, []byte{0xfd, 0xff, 0xff, 0xff, 0xf})
	testBinaryCodecPass(t, "int", -3, []byte{0x05})
	testBinaryCodecPass(t, "int", -65, []byte("\x81\x01"))
	testBinaryCodecPass(t, "int", 0, []byte{0x00})
	testBinaryCodecPass(t, "int", 1, []byte{0x02})
	testBinaryCodecPass(t, "int", 1016, []byte("\xf0\x0f"))
	testBinaryCodecPass(t, "int", 1455301406, []byte{0xbc, 0x8c, 0xf1, 0xeb, 0xa})
	testBinaryCodecPass(t, "int", 2147483647, []byte{0xfe, 0xff, 0xff, 0xff, 0xf})
	testBinaryCodecPass(t, "int", 3, []byte("\x06"))
	testBinaryCodecPass(t, "int", 64, []byte("\x80\x01"))
	testBinaryCodecPass(t, "int", 66052, []byte("\x88\x88\x08"))
	testBinaryCodecPass(t, "int", 8454660, []byte("\x88\x88\x88\x08"))
}

func TestPrimitiveLong(t *testing.T) {
	testPrimitiveCodec(t, "long")
	testBinaryEncodeFailBadDatumType(t, "long", "some string")
	testBinaryDecodeFailBufferUnderflow(t, "long", []byte("\xff\xff\xff\xff"))
	testBinaryCodecPass(t, "long", (1<<63)-1, []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1})
	testBinaryCodecPass(t, "long", -(1 << 63), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1})
	testBinaryCodecPass(t, "long", -2147483648, []byte("\xff\xff\xff\xff\x0f"))
	testBinaryCodecPass(t, "long", -3, []byte("\x05"))
	testBinaryCodecPass(t, "long", -65, []byte("\x81\x01"))
	testBinaryCodecPass(t, "long", 0, []byte("\x00"))
	testBinaryCodecPass(t, "long", 1082196484, []byte("\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, "long", 1359702038045356208, []byte{0xe0, 0xc2, 0x8b, 0xa1, 0x96, 0xf3, 0xd0, 0xde, 0x25})
	testBinaryCodecPass(t, "long", 138521149956, []byte("\x88\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, "long", 17730707194372, []byte("\x88\x88\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, "long", 2147483647, []byte("\xfe\xff\xff\xff\x0f"))
	testBinaryCodecPass(t, "long", 2269530520879620, []byte("\x88\x88\x88\x88\x88\x88\x88\x08"))
	testBinaryCodecPass(t, "long", 3, []byte("\x06"))
	testBinaryCodecPass(t, "long", 5959107741628848600, []byte{0xb0, 0xe7, 0x8a, 0xe1, 0xe2, 0xba, 0x80, 0xb3, 0xa5, 0x1})
	testBinaryCodecPass(t, "long", 64, []byte("\x80\x01"))

	// https://github.com/linkedin/goavro/issues/49
	testBinaryCodecPass(t, "long", -5513458701470791632, []byte("\x9f\xdf\x9f\x8f\xc7\xde\xde\x83\x99\x01"))
}

func TestPrimitiveNull(t *testing.T) {
	testPrimitiveCodec(t, "null")
	testBinaryEncodeFailBadDatumType(t, "null", false)
	testBinaryCodecPass(t, "null", nil, nil)
}

func TestPrimitiveString(t *testing.T) {
	testPrimitiveCodec(t, "string")
	testBinaryEncodeFailBadDatumType(t, "string", 42)
	testBinaryDecodeFailBufferUnderflow(t, "string", nil)
	testBinaryDecodeFailBufferUnderflow(t, "string", []byte{2})
	testBinaryCodecPass(t, "string", "", []byte("\x00"))
	testBinaryCodecPass(t, "string", "some string", []byte("\x16some string"))
}

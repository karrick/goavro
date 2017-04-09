package goavro_test

import (
	"math"
	"strings"
	"testing"

	"github.com/karrick/goavro"
)

func TestNewCodecInvalidType(t *testing.T) {
	codec, err := goavro.NewCodec("invalid")
	if actual, expected := err, "invalid"; actual == nil || !strings.Contains(actual.Error(), expected) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if codec != nil {
		t.Errorf("Actual: %#v; Expected: %#v", codec, nil)
	}
}

func TestBoolean(t *testing.T) {
	testBadDatumType(t, "boolean", 0, nil)
	testBadDatumType(t, "boolean", 1, nil)
	testCodecBufferUnderflow(t, "boolean", []byte{0}, false)
	testCodecBufferUnderflow(t, "boolean", nil, true)
	testCodecBidirectional(t, "boolean", false, []byte{0})
	testCodecBidirectional(t, "boolean", true, []byte{1})
}

func TestBytes(t *testing.T) {
	testBadDatumType(t, "bytes", 13, nil)
	testCodecBufferUnderflow(t, "bytes", []byte{0}, false)
	testCodecBufferUnderflow(t, "bytes", nil, true)
	testCodecBidirectional(t, "bytes", []byte(""), []byte("\x00"))
	testCodecBidirectional(t, "bytes", []byte("some bytes"), []byte("\x14some bytes"))
}

func TestDouble(t *testing.T) {
	testBadDatumType(t, "double", "some string", nil)
	testCodecBufferUnderflow(t, "double", []byte{0, 0, 0, 0, 0, 0, 0, 0}, false)
	testCodecBufferUnderflow(t, "double", []byte{0, 0, 0, 0, 0, 0, 0}, true)
	testCodecBidirectional(t, "double", 3.5, []byte("\x00\x00\x00\x00\x00\x00\f@"))
	testCodecBidirectional(t, "double", math.Inf(-1), []byte("\x00\x00\x00\x00\x00\x00\xf0\xff"))
	testCodecBidirectional(t, "double", math.Inf(1), []byte("\x00\x00\x00\x00\x00\x00\xf0\u007f"))
	testCodecBidirectional(t, "double", math.NaN(), []byte("\x01\x00\x00\x00\x00\x00\xf8\u007f"))
}

func TestFloat(t *testing.T) {
	testBadDatumType(t, "float", "some string", nil)
	testCodecBufferUnderflow(t, "float", []byte{0, 0, 0, 0}, false)
	testCodecBufferUnderflow(t, "float", []byte{0, 0, 0}, true)
	testCodecBidirectional(t, "float", 3.5, []byte("\x00\x00\x60\x40"))
	testCodecBidirectional(t, "float", math.Inf(-1), []byte("\x00\x00\x80\xff"))
	testCodecBidirectional(t, "float", math.Inf(1), []byte("\x00\x00\x80\u007f"))
	testCodecBidirectional(t, "float", math.NaN(), []byte("\x00\x00\xc0\u007f"))
}

func TestInt(t *testing.T) {
	testBadDatumType(t, "int", "some string", nil)
	testCodecBufferUnderflow(t, "int", []byte{1}, false)
	testCodecBufferUnderflow(t, "int", nil, true)
	testCodecBidirectional(t, "int", -1, []byte{0x01})
	testCodecBidirectional(t, "int", -2147483647, []byte{0xfd, 0xff, 0xff, 0xff, 0xf})
	testCodecBidirectional(t, "int", -3, []byte{0x05})
	testCodecBidirectional(t, "int", -65, []byte("\x81\x01"))
	testCodecBidirectional(t, "int", 0, []byte{0x00})
	testCodecBidirectional(t, "int", 1, []byte{0x02})
	testCodecBidirectional(t, "int", 1016, []byte("\xf0\x0f"))
	testCodecBidirectional(t, "int", 1455301406, []byte{0xbc, 0x8c, 0xf1, 0xeb, 0xa})
	testCodecBidirectional(t, "int", 2147483647, []byte{0xfe, 0xff, 0xff, 0xff, 0xf})
	testCodecBidirectional(t, "int", 3, []byte("\x06"))
	testCodecBidirectional(t, "int", 64, []byte("\x80\x01"))
	testCodecBidirectional(t, "int", 66052, []byte("\x88\x88\x08"))
	testCodecBidirectional(t, "int", 8454660, []byte("\x88\x88\x88\x08"))
}

func TestLong(t *testing.T) {
	testBadDatumType(t, "long", "some string", nil)
	testCodecBufferUnderflow(t, "long", []byte{1}, false)
	testCodecBufferUnderflow(t, "long", nil, true)
	testCodecBidirectional(t, "long", (1<<63)-1, []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1})
	testCodecBidirectional(t, "long", -(1 << 63), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1})
	testCodecBidirectional(t, "long", -2147483648, []byte("\xff\xff\xff\xff\x0f"))
	testCodecBidirectional(t, "long", -3, []byte("\x05"))
	testCodecBidirectional(t, "long", -5513458701470791632, []byte("\x9f\xdf\x9f\x8f\xc7\xde\xde\x83\x99\x01")) // https://github.com/linkedin/goavro/issues/49
	testCodecBidirectional(t, "long", -65, []byte("\x81\x01"))
	testCodecBidirectional(t, "long", 0, []byte("\x00"))
	testCodecBidirectional(t, "long", 1082196484, []byte("\x88\x88\x88\x88\x08"))
	testCodecBidirectional(t, "long", 1359702038045356208, []byte{0xe0, 0xc2, 0x8b, 0xa1, 0x96, 0xf3, 0xd0, 0xde, 0x25})
	testCodecBidirectional(t, "long", 138521149956, []byte("\x88\x88\x88\x88\x88\x08"))
	testCodecBidirectional(t, "long", 17730707194372, []byte("\x88\x88\x88\x88\x88\x88\x08"))
	testCodecBidirectional(t, "long", 2147483647, []byte("\xfe\xff\xff\xff\x0f"))
	testCodecBidirectional(t, "long", 2269530520879620, []byte("\x88\x88\x88\x88\x88\x88\x88\x08"))
	testCodecBidirectional(t, "long", 3, []byte("\x06"))
	testCodecBidirectional(t, "long", 5959107741628848600, []byte{0xb0, 0xe7, 0x8a, 0xe1, 0xe2, 0xba, 0x80, 0xb3, 0xa5, 0x1})
	testCodecBidirectional(t, "long", 64, []byte("\x80\x01"))
}

func TestNull(t *testing.T) {
	testBadDatumType(t, "null", false, nil)
	testCodecBufferUnderflow(t, "null", nil, false)
	testCodecBidirectional(t, "null", nil, nil)
}

func TestString(t *testing.T) {
	testBadDatumType(t, "string", 42, nil)
	testCodecBufferUnderflow(t, "string", []byte{0}, false)
	testCodecBufferUnderflow(t, "string", nil, true)
	testCodecBidirectional(t, "string", "", []byte("\x00"))
	testCodecBidirectional(t, "string", "some string", []byte("\x16some string"))
}

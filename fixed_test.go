package goavro_test

import (
	"testing"

	"github.com/karrick/goavro"
)

func TestFixedOughtHaveNameKey(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"fixed","size":13}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestFixedNameOughtToBeString(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"fixed","size":13,"name":13}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestFixedNameInvalid(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"fixed","size":13,"name":"&invalid"}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestFixedOughtHaveSizeKey(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"fixed","name":"foo"}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestFixedSizeOughtToBeNumber(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"fixed","name":"foo","size":"13"}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestFixedDecodeBufferUnderflow(t *testing.T) {
	c, err := goavro.NewCodec(`{"type":"fixed","name":"foo","size":13}`)
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	val, buf, err := c.BinaryDecode([]byte("ab"))
	if val != nil {
		t.Errorf("Actual: %#v; Expected: %#v", val, "non-nil")
	}
	if actual, expected := string(buf), "ab"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestFixedDecodeWithExtra(t *testing.T) {
	c, err := goavro.NewCodec(`{"type":"fixed","name":"foo","size":4}`)
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	val, buf, err := c.BinaryDecode([]byte("abcdefgh"))
	if actual, expected := string(val.([]byte)), "abcd"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if actual, expected := string(buf), "efgh"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
}

func TestFixedEncodeUnsupportedType(t *testing.T) {
	c, err := goavro.NewCodec(`{"type":"fixed","name":"foo","size":4}`)
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	buf, err := c.BinaryEncode(nil, 13)
	if actual, expected := string(buf), ""; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestFixedEncodeTooLong(t *testing.T) {
	c, err := goavro.NewCodec(`{"type":"fixed","name":"foo","size":4}`)
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	buf, err := c.BinaryEncode(nil, "abcde")
	if actual, expected := string(buf), ""; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestFixedEncodeString(t *testing.T) {
	c, err := goavro.NewCodec(`{"type":"fixed","name":"foo","size":4}`)
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	buf, err := c.BinaryEncode(nil, "abcd")
	if actual, expected := string(buf), "abcd"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
}

func TestFixedEncodeByteSlice(t *testing.T) {
	c, err := goavro.NewCodec(`{"type":"fixed","name":"foo","size":4}`)
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	buf, err := c.BinaryEncode(nil, []byte("abcd"))
	if actual, expected := string(buf), "abcd"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
}

func TestFixedNameCanBeUsedLater(t *testing.T) {
	c, err := goavro.NewCodec(`{"type":"record","name":"record1","fields":[
{"name":"field1","type":{"type":"fixed","name":"fixed_4","size":4}},
{"name":"field2","type":"fixed_4"}]}`)
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
	buf, err := c.BinaryEncode(nil, map[string]interface{}{
		"field1": "abcd",
		"field2": "efgh",
	})
	if actual, expected := string(buf), "abcdefgh"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if err != nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, nil)
	}
}

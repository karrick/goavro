package goavro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/karrick/goavro"
)

func TestRecordMissingName(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"record","fields":[{"name":"field1","type":"int"}]}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestRecordMissingFields(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"record","name":"foo"}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestRecordFieldsNotSlice(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"record","name":"foo","fields":3}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestRecordFieldsNotSliceOfMaps(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"record","name":"foo","fields":[3]}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestRecordFieldsEmpty(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"record","name":"foo","fields":[]}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestRecordFieldsHasInvalidSchema(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"record","name":"foo","fields":[{"invalid"}]}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestRecordDecodedEmptyBuf(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"record","name":"foo","fields":[{"name":"field1","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	value, buf, err := codec.BinaryDecode(nil)
	if value != nil {
		t.Errorf("Actual: %#v; Expected: %#v", value, nil)
	}
	if buf != nil {
		t.Errorf("Actual: %#v; Expected: %#v", buf, nil)
	}
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestRecordEncodedDatumGood(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"record","name":"com.example.record","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"float"}]}`)
	if err != nil {
		t.Fatal(err)
	}

	datumIn := map[string]interface{}{
		"field1": 3,
		"field2": 3.5,
	}

	buf, err := codec.BinaryEncode(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, []byte{
		0x06,                   // field1 == 3
		0x00, 0x00, 0x60, 0x40, // field2 == 3.5
	}) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, []byte{byte(2)})
	}

	// round trip
	datumOut, buf, err := codec.BinaryDecode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := len(buf), 0; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	datumOutMap, ok := datumOut.(map[string]interface{})
	if !ok {
		t.Errorf("Actual: %#v; Expected: %#v", ok, true)
	}
	if actual, expected := len(datumOutMap), len(datumIn); actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	for k, v := range datumIn {
		if actual, expected := fmt.Sprintf("%v", datumOutMap[k]), fmt.Sprintf("%v", v); actual != expected {
			t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
		}
	}
}

// TODO: test record fields adopt record as enclosing namespace

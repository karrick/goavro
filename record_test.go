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

func TestRecordFieldsHasDuplicateName(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"record","name":"record1","fields":[{"name":"field1","type":"string"},{"name":"field1","type":"int"}]}`)
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

func TestRecordFieldTypeHasPrimitiveName(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"record","name":"record","namespace":"com.example","fields":[{"name":"field1","type":"string"},{"name":"field2","type":{"type":"int"}}]}`)
	if err != nil {
		t.Fatal(err)
	}

	datumIn := map[string]interface{}{
		// NOTE: order of datum input map keys ought not matter:
		"field2": 13,
		"field1": "thirteen",
	}

	buf, err := codec.BinaryEncode(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if expected := []byte{
		0x10, // field1 length = 8
		't', 'h', 'i', 'r', 't', 'e', 'e', 'n',
		0x1a, // field2 == 13
	}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, expected)
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

func TestRecordEnclosingNamespaceSimple(t *testing.T) {
	c, err := goavro.NewCodec(`
{
  "type": "record",
  "name": "org.foo.Y",
  "fields": [
	{"name":"X","type": {"type": "fixed", "size": 4, "name": "fixed_4"}},
	{"name":"Z","type": {"type": "fixed_4"}}
  ]
}
`)
	if err != nil {
		t.Fatal(err)
	}

	datumIn := map[string]interface{}{
		"X": "abcd",
		"Z": "efgh",
	}

	buf, err := c.BinaryEncode(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if expected := []byte("abcdefgh"); !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, expected)
	}

	// round trip
	datumOut, buf, err := c.BinaryDecode(buf)
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
		if actual, expected := fmt.Sprintf("%s", datumOutMap[k]), fmt.Sprintf("%s", v); actual != expected {
			t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
		}
	}
}

func TestRecordEnclosingNamespaceComplex(t *testing.T) {
	_, err := goavro.NewCodec(`{
	  "type": "record",
	  "name": "outer_record",
	  "namespace": "com.example",
	  "fields": [
	    {
	      "name": "outer_record_field_1",
	      "type": {
	        "type": "record",
	        "name": "inner_record",
	        "fields": [
	          {"type": "string", "name": "inner_record_field_1"},
	          {"type": "int", "name": "inner_record_field_2"}
	        ]
	      }
		},
		{"type": {"type": "fixed", "size": 4, "name": "fixed_4"}, "name": "outer_record_field_2"},
		{"type": {"type": "fixed_4"}, "name": "outer_record_field_3"}
	  ]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	// TODO: ensure inner_record is `com.example.inner_record`
}

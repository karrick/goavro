package goavro_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/karrick/goavro"
)

func testBadDatumType(t *testing.T, schema string, datum interface{}, expected []byte) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(nil, datum)
	if len(encoded) != len(expected) {
		t.Errorf("Schema: %q; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, encoded, expected)
	}
	if !bytes.Equal(encoded, expected) {
		t.Errorf("Schema: %q; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, encoded, expected)
	}
	if actual, expected := err, "received"; actual == nil || !strings.Contains(actual.Error(), expected) {
		t.Fatalf("Schema: %q; Datum: %v; %v", schema, datum, err)
	}
}

func testCodecBufferUnderflow(t *testing.T, schema string, buf []byte, isTooShort bool) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = codec.Decode(buf)
	if isTooShort {
		if actual, expected := err, fmt.Sprintf("cannot decode %s: buffer underflow", schema); actual == nil || actual.Error() != expected {
			t.Errorf("Schema: %q; Actual: %#v; Expected: %#v", schema, actual, expected)
		}
	} else {
		if err != nil {
			t.Errorf("Schema: %q; Actual: %#v; Expected: %#v", schema, err, nil)
		}
	}
}

func testCodecDecoder(t *testing.T, schema string, datum interface{}, encoded []byte) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	value, remaining, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Schema: %q; %s", schema, err)
	}

	// remaining ought to be empty because there is nothing remaining to be decoded
	if actual, expected := len(remaining), 0; actual != expected {
		t.Errorf("Schema: %q; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
	}

	// for testing purposes, to prevent big switch statement, convert each to string and compare.
	if actual, expected := fmt.Sprintf("%v", value), fmt.Sprintf("%v", datum); actual != expected {
		t.Errorf("Schema: %q; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
	}
}

func testCodecEncoder(t *testing.T, schema string, datum interface{}, buf []byte) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		t.Fatalf("Schma: %q: %s", schema, err)
	}

	encoded, err := codec.Encode(nil, datum)
	if err != nil {
		t.Fatalf("Schema: %q; Datum: %v; %s", schema, datum, err)
	}

	if actual, expected := encoded, buf; !bytes.Equal(actual, expected) {
		t.Errorf("Schema: %q; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
	}
}

func testCodecBidirectional(t *testing.T, schema string, datum interface{}, buf []byte) {
	testCodecEncoder(t, schema, datum, buf)
	testCodecDecoder(t, schema, datum, buf)
}

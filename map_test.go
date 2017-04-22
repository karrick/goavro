package goavro_test

import (
	"bytes"
	"testing"

	"github.com/karrick/goavro"
)

func TestMapPrimitiveWrappers(t *testing.T) {
	testCodecBidirectional(t, `{"type":"boolean"}`, false, []byte{0})
	testCodecBidirectional(t, `{"type":"boolean"}`, true, []byte{1})
	testCodecBidirectional(t, `{"type":"bytes"}`, []byte(""), []byte{0})
	testCodecBidirectional(t, `{"type":"bytes"}`, []byte("some bytes"), []byte("\x14some bytes"))
}

func TestMapInt(t *testing.T) {
	intMap := map[string]interface{}{"Helium": 2}
	testCodecBidirectional(t, `{"type":"map","values":"int"}`, intMap, []byte("\x02\x0cHelium\x04\x00"))
}

func TestMapString(t *testing.T) {
	stringMap := map[string]interface{}{"He": "Helium"}
	testCodecBidirectional(t, `{"type":"map","values":"string"}`, stringMap, []byte("\x02\x04He\x0cHelium\x00"))
}

func TestMapValueTypeEnum(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"map","values":{"type":"enum","name":"foo","symbols":["alpha","bravo"]}}`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.BinaryEncode(nil, map[string]interface{}{"someKey": "bravo"})
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := buf, []byte{
		0x2, // blockCount = 1 pair
		0xe, // key length = 7
		's', 'o', 'm', 'e', 'K', 'e', 'y',
		0x2, // value = index 1 ("bravo")
		0,   // blockCount = 0 pairs
	}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

func TestMapValueTypeRecord(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"map","values":{"type":"record","name":"foo","fields":[{"name":"field1","type":"string"},{"name":"field2","type":"int"}]}}`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.BinaryEncode(nil, map[string]interface{}{
		"map-key": map[string]interface{}{
			"field1": "unlucky",
			"field2": 13,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := buf, []byte{
		0x2,                               // blockCount = 1 key-value pair in top level map
		0xe,                               // first key length = 7
		'm', 'a', 'p', '-', 'k', 'e', 'y', // first key = "map-key"
		// this key's value is a record, which is encoded by concatenated its field values
		0x0e, // field one string length = 7
		'u', 'n', 'l', 'u', 'c', 'k', 'y',
		0x1a, // 13
		0,    // map has no more blocks
	}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

package goavro_test

import (
	"bytes"
	"testing"

	"github.com/karrick/goavro"
)

func TestEnumRequiresName(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"enum","symbols":["alpha","bravo"]}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumMissingSymbols(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"enum","name":"foo"}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumSymbolsNotSlice(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":3}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumSymbolsNotSliceOfStrings(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":[3]}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumSymbolsEmpty(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":[]}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumSymbolsHasInvalidString(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["&invalid"]}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumDecodedEmptyBuf(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	value, buf, err := codec.Decode(nil)
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

func TestEnumDecodedIndexLessThanZero(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	originalBuf := []byte{byte(1)}
	value, buf, err := codec.Decode(originalBuf)
	if value != nil {
		t.Errorf("Actual: %#v; Expected: %#v", value, nil)
	}
	if !bytes.Equal(buf, originalBuf) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, nil)
	}
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumDecodedIndexTooLarge(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	originalBuf := []byte{byte(4)}
	value, buf, err := codec.Decode(originalBuf)
	if value != nil {
		t.Errorf("Actual: %#v; Expected: %#v", value, nil)
	}
	if !bytes.Equal(buf, originalBuf) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, nil)
	}
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumDecodedIndexZero(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	value, buf, err := codec.Decode([]byte{byte(0)})
	if actual, expected := value.(string), "alpha"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if len(buf) != 0 {
		t.Errorf("Actual: %#v; Expected: %#v", buf, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnumDecodedIndexLargestValid(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	value, buf, err := codec.Decode([]byte{byte(2)})
	if actual, expected := value.(string), "bravo"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if len(buf) != 0 {
		t.Errorf("Actual: %#v; Expected: %#v", buf, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnumEncodedDatumNotString(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.Encode(nil, 13)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
	if !bytes.Equal(buf, []byte{}) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, []byte{})
	}
}

func TestEnumEncodedDatumNotInEnum(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.Encode(nil, "charlie")
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
	if !bytes.Equal(buf, []byte{}) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, []byte{})
	}
}

func TestEnumEncodedDatumGood(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.Encode(nil, "bravo")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, []byte{byte(2)}) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, []byte{byte(2)})
	}
}

// ??? while this particular test is worthwhile, this might not be best location for it
func TestEnumValueTypeOfMap(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"map","values":{"type":"enum","name":"foo","symbols":["alpha","bravo"]}}`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.Encode(nil, map[string]interface{}{"someKey": "bravo"})
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

func TestEnumNamedTypeSimple(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.Encode(nil, "bravo")
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := buf, []byte{0x2}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

func TestEnumNamedTypeFullName(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.Encode(nil, "bravo")
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := buf, []byte{0x2}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

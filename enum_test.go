package goavro_test

import (
	"bytes"
	"testing"

	"github.com/karrick/goavro"
)

func TestEnumMissingSymbols(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"enum"}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumSymbolsNotSlice(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"enum","symbols":3}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumSymbolsNotSliceOfStrings(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"enum","symbols":[3]}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumSymbolsEmpty(t *testing.T) {
	_, err := goavro.NewCodec(`{"type":"enum","symbols":[]}`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumDecodedEmptyBuf(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","symbols":["alpha","bravo"]}`)
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
	codec, err := goavro.NewCodec(`{"type":"enum","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	value, buf, err := codec.Decode([]byte{byte(1)})
	if value != nil {
		t.Errorf("Actual: %#v; Expected: %#v", value, nil)
	}
	if len(buf) != 0 {
		t.Errorf("Actual: %#v; Expected: %#v", buf, nil)
	}
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumDecodedIndexTooLarge(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","symbols":["alpha","bravo"]}`)
	if err != nil {
		t.Fatal(err)
	}
	value, buf, err := codec.Decode([]byte{byte(4)})
	if value != nil {
		t.Errorf("Actual: %#v; Expected: %#v", value, nil)
	}
	if len(buf) != 0 {
		t.Errorf("Actual: %#v; Expected: %#v", buf, nil)
	}
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestEnumDecodedIndexZero(t *testing.T) {
	codec, err := goavro.NewCodec(`{"type":"enum","symbols":["alpha","bravo"]}`)
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
	codec, err := goavro.NewCodec(`{"type":"enum","symbols":["alpha","bravo"]}`)
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
	codec, err := goavro.NewCodec(`{"type":"enum","symbols":["alpha","bravo"]}`)
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
	codec, err := goavro.NewCodec(`{"type":"enum","symbols":["alpha","bravo"]}`)
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
	codec, err := goavro.NewCodec(`{"type":"enum","symbols":["alpha","bravo"]}`)
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

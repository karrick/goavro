package goavro_test

import (
	"testing"
)

func TestSchemaEnum(t *testing.T) {
	testSchemaValid(t, `{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`)
}

func TestEnumName(t *testing.T) {
	testSchemaInvalid(t, `{"type":"enum","symbols":["alpha","bravo"]}`, "Enum ought to have valid name: schema ought to have name key")
	testSchemaInvalid(t, `{"type":"enum","name":3}`, "Enum ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"enum","name":""}`, "Enum ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"enum","name":"&foo","symbols":["alpha","bravo"]}`, "Enum ought to have valid name: schema name ought to start with")
	testSchemaInvalid(t, `{"type":"enum","name":"foo&","symbols":["alpha","bravo"]}`, "Enum ought to have valid name: schema name ought to have second and remaining")
}

func TestEnumSymbols(t *testing.T) {
	testSchemaInvalid(t, `{"type":"enum","name":"foo"}`, `Enum "foo" ought to have symbols key`)
	testSchemaInvalid(t, `{"type":"enum","name":"foo","symbols":3}`, `Enum "foo" symbols ought to be non-empty array of strings`)
	testSchemaInvalid(t, `{"type":"enum","name":"foo","symbols":[]}`, `Enum "foo" symbols ought to be non-empty array of strings`)
}

func TestEnumSymbolInvalid(t *testing.T) {
	testSchemaInvalid(t, `{"type":"enum","name":"foo","symbols":[3]}`, `Enum "foo" symbol 1 ought to be non-empty string`)
	testSchemaInvalid(t, `{"type":"enum","name":"foo","symbols":[""]}`, `Enum "foo" symbol 1 ought to be non-empty string`)
	testSchemaInvalid(t, `{"type":"enum","name":"foo","symbols":["string-with-invalid-characters"]}`, `Enum "foo" symbol 1 ought to have second and remaining`)
}

func TestEnumDecodeError(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`, nil, "buffer underflow")
	testBinaryDecodeFail(t, `{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`, []byte("\x01"), `Enum "foo": index ought to be between 0 and 1`)
	testBinaryDecodeFail(t, `{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`, []byte("\x04"), `Enum "foo": index ought to be between 0 and 1`)
}

func TestEnumEncodeError(t *testing.T) {
	testBinaryEncodeFail(t, `{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`, 13, `Enum "foo": expected string; received: int`)
	testBinaryEncodeFail(t, `{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`, "charlie", `Enum "foo": string ought to be member of symbols`)
}

func TestEnumEncode(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`, "alpha", []byte("\x00"))
	testBinaryCodecPass(t, `{"type":"enum","name":"foo","symbols":["alpha","bravo"]}`, "bravo", []byte("\x02"))
}

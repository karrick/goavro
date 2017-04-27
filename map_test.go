package goavro_test

import (
	"testing"
)

func TestSchemaMapValueBytes(t *testing.T) {
	// NOTE: This schema also used to read and write files in OCF format
	testSchemaValid(t, `{"type":"map","values":"bytes"}`)
}

func TestMapValues(t *testing.T) {
	testSchemaInvalid(t, `{"type":"map","value":"int"}`, "Map ought to have values key")
	testSchemaInvalid(t, `{"type":"map","values":"integer"}`, "Map values ought to be valid Avro type")
	testSchemaInvalid(t, `{"type":"map","values":3}`, "Map values ought to be valid Avro type")
	testSchemaInvalid(t, `{"type":"map","values":int}`, "invalid character") // type name must be quoted
}

func TestMapDecodeFail(t *testing.T) {
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, nil, "cannot decode Map block count")           // leading block count
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x01"), "cannot decode Map block size") // when block count < 0
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04"), "cannot decode Map key")
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04"), "cannot decode Map key")
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04a"), "cannot decode Map key")
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04ab"), "cannot decode Map value")
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04ab\x02"), "boolean: expected")
	testBinaryDecodeFail(t, `{"type":"map","values":"boolean"}`, []byte("\x02\x04ab\x01"), "cannot decode Map block count") // trailing block count
}

func TestMap(t *testing.T) {
	testBinaryCodecPass(t, `{"type":"map","values":"null"}`, map[string]interface{}{"ab": nil}, []byte("\x02\x04ab\x00"))
	testBinaryCodecPass(t, `{"type":"map","values":"boolean"}`, map[string]interface{}{"ab": true}, []byte("\x02\x04ab\x01\x00"))
}

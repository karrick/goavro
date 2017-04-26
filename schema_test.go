package goavro_test

import (
	"testing"
)

// NOTE: This file includes test cases that apply to more than one non-primitive data type.

func TestSchemaWeather(t *testing.T) {
	testSchemaValid(t, `
{"type": "record", "name": "test.Weather",
 "doc": "A weather reading.",
 "fields": [
     {"name": "station", "type": "string", "order": "ignore"},
     {"name": "time", "type": "long"},
     {"name": "temp", "type": "int"}
 ]
}
`)
}

func TestSchemaFooBarSpecificRecord(t *testing.T) {
	testSchemaValid(t, `
{
    "type": "record",
    "name": "FooBarSpecificRecord",
    "namespace": "org.apache.avro",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "nicknames", "type":
            {"type": "array", "items": "string"}},
        {"name": "relatedids", "type": 
            {"type": "array", "items": "int"}},
        {"name": "typeEnum", "type": 
            ["null", { 
                    "type": "enum",
                    "name": "TypeEnum",
                    "namespace": "org.apache.avro",
                    "symbols" : ["a","b", "c"]
                }],
            "default": null
        }
    ]
}
`)
}

func TestSchemaInterop(t *testing.T) {
	testSchemaValid(t, `
{"type": "record", "name":"Interop", "namespace": "org.apache.avro",
  "fields": [
      {"name": "intField", "type": "int"},
      {"name": "longField", "type": "long"},
      {"name": "stringField", "type": "string"},
      {"name": "boolField", "type": "boolean"},
      {"name": "floatField", "type": "float"},
      {"name": "doubleField", "type": "double"},
      {"name": "bytesField", "type": "bytes"},
      {"name": "nullField", "type": "null"},
      {"name": "arrayField", "type": {"type": "array", "items": "double"}},
      {"name": "mapField", "type":
       {"type": "map", "values":
        {"type": "record", "name": "Foo",
         "fields": [{"name": "label", "type": "string"}]}}},
      {"name": "unionField", "type":
       ["boolean", "double", {"type": "array", "items": "bytes"}]},
      {"name": "enumField", "type":
       {"type": "enum", "name": "Kind", "symbols": ["A","B","C"]}},
      {"name": "fixedField", "type":
       {"type": "fixed", "name": "MD5", "size": 16}},
      {"name": "recordField", "type":
       {"type": "record", "name": "Node",
        "fields": [
            {"name": "label", "type": "string"},
            {"name": "children", "type": {"type": "array", "items": "Node"}}]}}
  ]
}
`)
}

func TestSchemaFixedNameCanBeUsedLater(t *testing.T) {
	schema := `{"type":"record","name":"record1","fields":[
                   {"name":"field1","type":{"type":"fixed","name":"fixed_4","size":4}},
                   {"name":"field2","type":"fixed_4"}]}`

	datum := map[string]interface{}{
		"field1": "abcd",
		"field2": "efgh",
	}

	testBinaryEncodePass(t, schema, datum, []byte("abcdefgh"))
}

func TestMapValueTypeEnum(t *testing.T) {
	schema := `{"type":"map","values":{"type":"enum","name":"foo","symbols":["alpha","bravo"]}}`

	datum := map[string]interface{}{"someKey": "bravo"}

	expected := []byte{
		0x2, // blockCount = 1 pair
		0xe, // key length = 7
		's', 'o', 'm', 'e', 'K', 'e', 'y',
		0x2, // value = index 1 ("bravo")
		0,   // blockCount = 0 pairs
	}

	testBinaryCodecPass(t, schema, datum, expected)
}

func TestMapValueTypeRecord(t *testing.T) {
	schema := `{"type":"map","values":{"type":"record","name":"foo","fields":[{"name":"field1","type":"string"},{"name":"field2","type":"int"}]}}`

	datum := map[string]interface{}{
		"map-key": map[string]interface{}{
			"field1": "unlucky",
			"field2": 13,
		},
	}

	expected := []byte{
		0x2,                               // blockCount = 1 key-value pair in top level map
		0xe,                               // first key length = 7
		'm', 'a', 'p', '-', 'k', 'e', 'y', // first key = "map-key"
		// this key's value is a record, which is encoded by concatenated its field values
		0x0e, // field one string length = 7
		'u', 'n', 'l', 'u', 'c', 'k', 'y',
		0x1a, // 13
		0,    // map has no more blocks
	}

	// cannot decode because order of map key enumeration random, and records are returned as a Go map
	testBinaryEncodePass(t, schema, datum, expected)
}

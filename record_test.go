package goavro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/karrick/goavro"
)

func TestRecordName(t *testing.T) {
	testSchemaInvalid(t, `{"type":"record","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`, "Record ought to have valid name: schema ought to have name key")
	testSchemaInvalid(t, `{"type":"record","name":3}`, "Record ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"record","name":""}`, "Record ought to have valid name: schema name ought to be non-empty string")
	testSchemaInvalid(t, `{"type":"record","name":"&foo","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`, "Record ought to have valid name: schema name ought to start with")
	testSchemaInvalid(t, `{"type":"record","name":"foo&","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`, "Record ought to have valid name: schema name ought to have second and remaining")
}

func TestRecordFields(t *testing.T) {
	testSchemaInvalid(t, `{"type":"record","name":"r1"}`, `Record "r1" ought to have fields key`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":3}`, `Record "r1" fields ought to be non-empty array`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[]}`, `Record "r1" fields ought to be non-empty array`)
}

func TestRecordFieldInvalid(t *testing.T) {
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[3]}`, `Record "r1" field 1 ought to be valid Avro named type`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[""]}`, `Record "r1" field 1 ought to be valid Avro named type`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[{}]}`, `Record "r1" field 1 ought to be valid Avro named type`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[{"type":"int"}]}`, `Record "r1" field 1 ought to have valid name`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[{"name":"f1"}]}`, `Record "r1" field 1 ought to be valid Avro named type`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":"integer"}]}`, `Record "r1" field 1 ought to be valid Avro named type`)
	testSchemaInvalid(t, `{"type":"record","name":"r1","fields":[{"name":"f1","type":"int"},{"name":"f1","type":"long"}]}`, `Record "r1" field 2 ought to have unique name`)
}

func TestSchemaRecord(t *testing.T) {
	testSchemaValid(t, `{
  "name": "person",
  "type": "record",
  "fields": [
    {
      "name": "height",
      "type": "long"
    },
    {
      "name": "weight",
      "type": "long"
    },
    {
      "name": "name",
      "type": "string"
    }
  ]
}`)
}

func TestSchemaRecordFieldWithDefaults(t *testing.T) {
	testSchemaValid(t, `{
  "name": "person",
  "type": "record",
  "fields": [
    {
      "name": "height",
      "type": "long"
    },
    {
      "name": "weight",
      "type": "long"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "hacker",
      "type": "boolean",
      "default": false
    }
  ]
}`)
}

func TestRecordDecodedEmptyBuffer(t *testing.T) {
	testBinaryDecodeFailShortBuffer(t, `{"type":"record","name":"foo","fields":[{"name":"field1","type":"int"}]}`, nil)
}

func TestRecordFieldTypeHasPrimitiveName(t *testing.T) {
	codec, err := goavro.NewCodec(`{
  "type": "record",
  "name": "r1",
  "namespace": "com.example",
  "fields": [
    {
      "name": "f1",
      "type": "string"
    },
    {
      "name": "f2",
      "type": {
        "type": "int"
      }
    }
  ]
}`)
	if err != nil {
		t.Fatal(err)
	}

	datumIn := map[string]interface{}{
		"f1": "thirteen",
		"f2": 13,
	}

	buf, err := codec.BinaryEncode(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if expected := []byte{
		0x10, // field1 size = 8
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

func TestSchemaRecordRecursive(t *testing.T) {
	testSchemaValid(t, `{
  "type": "record",
  "name": "recursive",
  "fields": [
    {
      "name": "label",
      "type": "string"
    },
    {
      "name": "children",
      "type": {
        "type": "array",
        "items": "recursive"
      }
    }
  ]
}`)
}

func TestSchemaNamespaceRecursive(t *testing.T) {
	testSchemaValid(t, `{
  "type": "record",
  "name": "Container",
  "namespace": "namespace1",
  "fields": [
    {
      "name": "contained",
      "type": {
        "type": "record",
        "name": "MutuallyRecursive",
        "fields": [
          {
            "name": "label",
            "type": "string"
          },
          {
            "name": "children",
            "type": {
              "type": "array",
              "items": {
                "type": "record",
                "name": "MutuallyRecursive",
                "namespace": "namespace2",
                "fields": [
                  {
                    "name": "value",
                    "type": "int"
                  },
                  {
                    "name": "children",
                    "type": {
                      "type": "array",
                      "items": "namespace1.MutuallyRecursive"
                    }
                  },
                  {
                    "name": "morechildren",
                    "type": {
                      "type": "array",
                      "items": "MutuallyRecursive"
                    }
                  }
                ]
              }
            }
          },
          {
            "name": "anotherchild",
            "type": "namespace2.MutuallyRecursive"
          }
        ]
      }
    }
  ]
}`)
}

func TestSchemaRecordNamespaceComposite(t *testing.T) {
	testSchemaValid(t, `{
  "type": "record",
  "namespace": "x",
  "name": "Y",
  "fields": [
    {
      "name": "e",
      "type": {
        "type": "record",
        "name": "Z",
        "fields": [
          {
            "name": "f",
            "type": "x.Z"
          }
        ]
      }
    }
  ]
}`)
}

func TestSchemaRecordNamespaceFullName(t *testing.T) {
	testSchemaValid(t, `{
  "type": "record",
  "name": "x.Y",
  "fields": [
    {
      "name": "e",
      "type": {
        "type": "record",
        "name": "Z",
        "fields": [
          {
            "name": "f",
            "type": "x.Y"
          },
          {
            "name": "g",
            "type": "x.Z"
          }
        ]
      }
    }
  ]
}`)
}

func TestSchemaRecordNamespaceEnum(t *testing.T) {
	testSchemaValid(t, `{"type": "record", "name": "org.apache.avro.tests.Hello", "fields": [
  {"name": "f1", "type": {"type": "enum", "name": "MyEnum", "symbols": ["Foo", "Bar", "Baz"]}},
  {"name": "f2", "type": "org.apache.avro.tests.MyEnum"},
  {"name": "f3", "type": "MyEnum"},
  {"name": "f4", "type": {"type": "enum", "name": "other.namespace.OtherEnum", "symbols": ["one", "two", "three"]}},
  {"name": "f5", "type": "other.namespace.OtherEnum"},
  {"name": "f6", "type": {"type": "enum", "name": "ThirdEnum", "namespace": "some.other", "symbols": ["Alice", "Bob"]}},
  {"name": "f7", "type": "some.other.ThirdEnum"}
]}`)
}

func TestSchemaRecordNamespaceFixed(t *testing.T) {
	testSchemaValid(t, `{"type": "record", "name": "org.apache.avro.tests.Hello", "fields": [
  {"name": "f1", "type": {"type": "fixed", "name": "MyFixed", "size": 16}},
  {"name": "f2", "type": "org.apache.avro.tests.MyFixed"},
  {"name": "f3", "type": "MyFixed"},
  {"name": "f4", "type": {"type": "fixed", "name": "other.namespace.OtherFixed", "size": 18}},
  {"name": "f5", "type": "other.namespace.OtherFixed"},
  {"name": "f6", "type": {"type": "fixed", "name": "ThirdFixed", "namespace": "some.other", "size": 20}},
  {"name": "f7", "type": "some.other.ThirdFixed"}
]}`)
}

func TestRecordNamespace(t *testing.T) {
	c, err := goavro.NewCodec(`{
  "type": "record",
  "name": "org.foo.Y",
  "fields": [
    {
      "name": "X",
      "type": {
        "type": "fixed",
        "size": 4,
        "name": "fixed_4"
      }
    },
    {
      "name": "Z",
      "type": {
        "type": "fixed_4"
      }
    }
  ]
}`)
	if err != nil {
		t.Fatal(err)
	}

	datumIn := map[string]interface{}{
		"X": []byte("abcd"),
		"Z": []byte("efgh"),
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

func TestRecordEncodeFail(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "r1",
  "fields": [
    {"name": "f1", "type": "string"},
    {"name": "f2", "type": "string"}
  ]
}`

	testBinaryEncodeFail(t, schema, map[string]interface{}{"f1": "foo"}, `field value for "f2" was not specified`)
	testBinaryEncodeFail(t, schema, map[string]interface{}{"f1": "foo", "f2": 13}, `field value for "f2" does not match its schema`)
}

func TestRecordTextDecodeFail(t *testing.T) {
	schema := `{"name":"r1","type":"record","fields":[{"name":"string","type":"string"},{"name":"bytes","type":"bytes"}]}`
	testTextDecodeFail(t, schema, []byte(`    "string"  :  "silly"  ,   "bytes"  : "silly" } `), "expected: '{'")
	testTextDecodeFail(t, schema, []byte(`  {  16  :  "silly"  ,   "bytes"  : "silly" } `), "expected initial \"")
	testTextDecodeFail(t, schema, []byte(`  {  "badName"  :  "silly"  ,   "bytes"  : "silly" } `), "cannot determine codec")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  ,  "silly"  ,   "bytes"  : "silly" } `), "expected: ':'")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  13  ,   "bytes"  : "silly" } `), "expected initial \"")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" :   "bytes"  : "silly" } `), "expected ',' or '}'")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" ,   "bytes"  : "silly"  `), "short buffer")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly"  `), "short buffer")
	testTextDecodeFail(t, schema, []byte(`  {  "string"  :  "silly" } `), "only found 1 of 2 fields")
}

func TestRecordTextCodecPass(t *testing.T) {
	silly := "⌘ "
	testTextEncodePass(t, `{"name":"r1","type":"record","fields":[{"name":"string","type":"string"}]}`, map[string]interface{}{"string": silly}, []byte(`{"string":"\u0001\u2318 "}`))
	testTextEncodePass(t, `{"name":"r1","type":"record","fields":[{"name":"bytes","type":"bytes"}]}`, map[string]interface{}{"bytes": []byte(silly)}, []byte(`{"bytes":"\u0001\u00E2\u008C\u0098 "}`))
	testTextDecodePass(t, `{"name":"r1","type":"record","fields":[{"name":"string","type":"string"},{"name":"bytes","type":"bytes"}]}`, map[string]interface{}{"string": silly, "bytes": []byte(silly)}, []byte(` { "string" : "\u0001\u2318 " , "bytes" : "\u0001\u00E2\u008C\u0098 " }`))
}

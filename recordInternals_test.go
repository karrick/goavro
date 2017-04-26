package goavro

import (
	"testing"
)

func TestRecordEnclosingNamespaceComplex(t *testing.T) {
	c, err := NewCodec(`{"type": "record", "name": "org.apache.avro.tests.Hello", "fields": [
  {"name": "f1", "type": {"type": "fixed", "name": "MyFixed", "size": 16}},
  {"name": "f2", "type": "org.apache.avro.tests.MyFixed"},
  {"name": "f3", "type": "MyFixed"},
  {"name": "f4", "type": {"type": "fixed", "name": "other.namespace.OtherFixed", "size": 18}},
  {"name": "f5", "type": "other.namespace.OtherFixed"},
  {"name": "f6", "type": {"type": "fixed", "name": "ThirdFixed", "namespace": "some.other", "size": 20}},
  {"name": "f7", "type": "some.other.ThirdFixed"}
]}`)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure following type names are created:
	var outer, fixed1, fixed2, fixed3 bool
	for _, name := range typeNames(c.symbolTable) {
		switch name {
		case "org.apache.avro.tests.Hello":
			outer = true
		case "org.apache.avro.tests.MyFixed":
			fixed1 = true
		case "other.namespace.OtherFixed":
			fixed2 = true
		case "some.other.ThirdFixed":
			fixed3 = true
		}
	}
	if actual, expected := outer, true; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if actual, expected := fixed1, true; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if actual, expected := fixed2, true; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if actual, expected := fixed3, true; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

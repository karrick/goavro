package goavro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/karrick/goavro"
)

func TestUnion(t *testing.T) {
	testCodecBidirectional(t, `["null"]`, nil, []byte("\x00"))
	testCodecBidirectional(t, `["null","int"]`, nil, []byte("\x00"))
	testCodecBidirectional(t, `["int","null"]`, nil, []byte("\x02"))

	testCodecBidirectional(t, `["null","int"]`, 3, []byte("\x02\x06"))
	testCodecBidirectional(t, `["null","long"]`, 3, []byte("\x02\x06"))

	testCodecBidirectional(t, `["int","null"]`, 3, []byte("\x00\x06"))
	testCodecEncoder(t, `["int","null"]`, 3, []byte("\x00\x06")) // can encode a bare 3
}

func _TestUnionWillTryToEncodeDatumValuesInSchemaOrder(t *testing.T) {
	// all of these values fit in an Avro int, and that's before Avro long, so will use Avro int
	testCodecBidirectional(t, `["null","int","long","float","double"]`, 3, []byte("\x02\x06"))
	testCodecBidirectional(t, `["null","int","long","float","double"]`, int(3), []byte("\x02\x06"))
	testCodecBidirectional(t, `["null","int","long","float","double"]`, int32(3), []byte("\x02\x06"))
	testCodecBidirectional(t, `["null","int","long","float","double"]`, int64(3), []byte("\x02\x06"))

	// choses the first schema the datum fits in
	testCodecBidirectional(t, `["null","int","long","float","double"]`, (1<<31)-1, []byte("\x02\xfe\xff\xff\xff\x0f"))
	testCodecBidirectional(t, `["null","long","int","float","double"]`, (1<<31)-1, []byte("\x02\xfe\xff\xff\xff\x0f"))
	testCodecBidirectional(t, `["null","int","long","float","double"]`, (1 << 34), []byte("\x04\x80\x80\x80\x80\x80\x01"))

	// all of these values fit in an Avro float, and that's before Avro double, so will use Avro float
	testCodecBidirectional(t, `["null","int","long","float","double"]`, 3.5, []byte("\x06\x00\x00\x60\x40"))
	testCodecBidirectional(t, `["null","int","long","float","double"]`, float32(3.5), []byte("\x06\x00\x00\x60\x40"))
	testCodecBidirectional(t, `["null","int","long","float","double"]`, float64(3.5), []byte("\x06\x00\x00\x60\x40"))
}

func _TestUnionWillCoerceTypeIfPossible(t *testing.T) {
	testCodecBidirectional(t, `["null","long","float","double"]`, int32(3), []byte("\x02\x06"))
	testCodecBidirectional(t, `["null","int","float","double"]`, int64(3), []byte("\x02\x06"))
	testCodecBidirectional(t, `["null","int","long","double"]`, float32(3.5), []byte("\x06\x00\x00\x00\x00\x00\x00\f@"))
	testCodecBidirectional(t, `["null","int","long","float"]`, float64(3.5), []byte("\x06\x00\x00\x60\x40"))
}

func TestUnionWithArray(t *testing.T) {
	testCodecBidirectional(t, `["null",{"type":"array","items":"int"}]`, nil, []byte("\x00"))

	testCodecBidirectional(t, `["null",{"type":"array","items":"int"}]`, []interface{}{}, []byte("\x02\x00"))
	testCodecBidirectional(t, `["null",{"type":"array","items":"int"}]`, []interface{}{1}, []byte("\x02\x02\x02\x00"))
	testCodecBidirectional(t, `["null",{"type":"array","items":"int"}]`, []interface{}{1, 2}, []byte("\x02\x04\x02\x04\x00"))
}

func TestUnionWithMap(t *testing.T) {
	testCodecBidirectional(t, `["null",{"type":"map","values":"string"}]`, nil, []byte("\x00"))
	testCodecBidirectional(t, `["string",{"type":"map","values":"string"}]`, map[string]interface{}{"He": "Helium"}, []byte("\x02\x02\x04He\x0cHelium\x00"))
	testCodecBidirectional(t, `["string",{"type":"map","values":"string"}]`, "Helium", []byte("\x00\x0cHelium"))
}

func TestUnionOfEnumsWithSameType(t *testing.T) {
	_, err := goavro.NewCodec(`[{"type":"enum","name":"com.example.foo","symbols":["alpha","bravo"]},"com.example.foo"]`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestUnionOfEnumsWithDifferentTypeButInvalidString(t *testing.T) {
	codec, err := goavro.NewCodec(`[{"type":"enum","name":"com.example.colors","symbols":["red","green","blue"]},{"type":"enum","name":"com.example.animals","symbols":["dog","cat"]}]`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.BinaryEncode(nil, "bravo")
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
	if actual, expected := buf, []byte{}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

func TestUnionOfEnumsWithSameNames(t *testing.T) {
	_, err := goavro.NewCodec(`[{"type":"enum","name":"com.example.one","symbols":["red","green","blue"]},{"type":"enum","name":"one","namespace":"com.example","symbols":["dog","cat"]}]`)
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
}

func TestUnionOfEnumsWithDifferentTypeValidString(t *testing.T) {
	codec, err := goavro.NewCodec(`[{"type":"enum","name":"com.example.colors","symbols":["red","green","blue"]},{"type":"enum","name":"com.example.animals","symbols":["dog","cat"]}]`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.BinaryEncode(nil, "dog")
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := buf, []byte{0x2, 0x0}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}

	// round trip back to native string
	value, buf, err := codec.BinaryDecode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := value.(string), "dog"; actual != expected {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
	if actual, expected := buf, []byte{}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

func TestUnionMapRecordFitsInRecord(t *testing.T) {
	// when encoding union with child object, named types, such as records, enums, and fixed, are named

	// union value may be either map or a record
	codec, err := goavro.NewCodec(`["null",{"type":"map","values":"double"},{"type":"record","name":"com.example.record","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"float"}]}]`)
	if err != nil {
		t.Fatal(err)
	}

	// the provided datum value could be encoded by either the map or the record schemas above
	datumIn := map[string]interface{}{
		"field1": 3,
		"field2": 3.5,
	}
	if false { // boxing
		datumIn = map[string]interface{}{
			"com.example.record": datumIn,
		}
	}

	buf, err := codec.BinaryEncode(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, []byte{
		0x04,                   // prefer record (union item 2) over map (union item 1)
		0x06,                   // field1 == 3
		0x00, 0x00, 0x60, 0x40, // field2 == 3.5
	}) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, []byte{byte(2)})
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

func TestUnionMapRecordDatumHasFieldNotInRecord(t *testing.T) {
	// when encoding union with child object, named types, such as records, enums, and fixed, are named

	// union value may be either map or a record
	codec, err := goavro.NewCodec(`["null",{"type":"map","values":"string"},{"type":"record","name":"com.example.record","fields":[{"name":"field1","type":"string"}]}]`)
	if err != nil {
		t.Fatal(err)
	}

	// the provided datum value could be encoded by either the map or the record schemas above
	datumIn := map[string]interface{}{
		"field1": "a",
		"field2": "b",
	}
	if false { // boxing
		datumIn = map[string]interface{}{
			"com.example.record": datumIn,
		}
	}

	buf, err := codec.BinaryEncode(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	option1 := []byte{
		0x02, // map (union item 1)
		0x04, // two key-value pairs

		0x0c,                         // key one length 6
		'f', 'i', 'e', 'l', 'd', '1', // key one = "field1"
		0x02, // value one length 1
		'a',  // value one = "a"

		0x0c,                         // key two length 6
		'f', 'i', 'e', 'l', 'd', '2', // key two = "field2"
		0x02, // value two length 1
		'b',  // value two = "b"

		0x00, // no more key-value pairs
	}
	option2 := []byte{
		0x02, // map (union item 1)
		0x04, // two key-value pairs

		0x0c,                         // key one length 6
		'f', 'i', 'e', 'l', 'd', '2', // key one = "field2"
		0x02, // value one length 1
		'b',  // value one = "b"

		0x0c,                         // key two length 6
		'f', 'i', 'e', 'l', 'd', '1', // key two = "field1"
		0x02, // value two length 1
		'a',  // value two = "a"

		0x00, // no more key-value pairs
	}

	if !bytes.Equal(buf, option1) && !bytes.Equal(buf, option2) {
		t.Errorf("Actual: %#v; Expected: %#v", buf, option1)
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

func TestUnionMapRecordDatumMissingRecordField(t *testing.T) {
	// when encoding union with child object, named types, such as records, enums, and fixed, are named

	// union value may be either map or a record
	codec, err := goavro.NewCodec(`["null",{"type":"map","values":"string"},{"type":"record","name":"com.example.record","fields":[{"name":"field1","type":"string"},{"name":"field2","type":"string"}]}]`)
	if err != nil {
		t.Fatal(err)
	}

	// the provided datum value could be encoded by either the map or the record schemas above
	datumIn := map[string]interface{}{
		"field1": "a",
	}
	if false { // boxing
		datumIn = map[string]interface{}{
			"com.example.record": datumIn,
		}
	}

	buf, err := codec.BinaryEncode(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	option := []byte{
		0x02, // map (union item 1)
		0x02, // one key-value pair

		0x0c,                         // key one length 6
		'f', 'i', 'e', 'l', 'd', '1', // key one = "field1"
		0x02, // value one length 1
		'a',  // value one = "a"

		0x00, // no more key-value pairs
	}
	if !bytes.Equal(buf, option) {
		t.Errorf("Actual: %#v; Expected either: %#v", buf, option)
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

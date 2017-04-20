package goavro_test

import (
	"bytes"
	"testing"

	"github.com/karrick/goavro"
)

func TestUnion(t *testing.T) {
	testCodecBidirectional(t, `["null"]`, nil, []byte("\x00"))
	testCodecBidirectional(t, `["null","int"]`, nil, []byte("\x00"))
	testCodecBidirectional(t, `["null","int"]`, 3, []byte("\x02\x06"))
	testCodecBidirectional(t, `["int","null"]`, nil, []byte("\x02"))
	testCodecBidirectional(t, `["int","null"]`, 3, []byte("\x00\x06"))
}

func TestUnionWillTryToEncodeDatumValuesInSchemaOrder(t *testing.T) {
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

func TestUnionWillCoerceTypeIfPossible(t *testing.T) {
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
	buf, err := codec.Encode(nil, "bravo")
	if err == nil {
		t.Errorf("Actual: %#v; Expected: %#v", err, "non-nil")
	}
	if actual, expected := buf, []byte{}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}
}

func TestUnionOfEnumsWithDifferentTypeValidString(t *testing.T) {
	codec, err := goavro.NewCodec(`[{"type":"enum","name":"com.example.colors","symbols":["red","green","blue"]},{"type":"enum","name":"com.example.animals","symbols":["dog","cat"]}]`)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := codec.Encode(nil, "dog")
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := buf, []byte{0x2, 0x0}; !bytes.Equal(buf, expected) {
		t.Errorf("Actual: %#v; Expected: %#v", actual, expected)
	}

	// round trip back to native string
	value, buf, err := codec.Decode(buf)
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

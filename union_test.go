package goavro_test

import "testing"

func TestUnion(t *testing.T) {
	testCodecBidirectional(t, `["null"]`, nil, []byte("\x00"))
	testCodecBidirectional(t, `["null","int"]`, nil, []byte("\x00"))
	testCodecBidirectional(t, `["null","int"]`, 3, []byte("\x02\x06"))
	testCodecBidirectional(t, `["int","null"]`, nil, []byte("\x02"))
	testCodecBidirectional(t, `["int","null"]`, 3, []byte("\x00\x06"))
}

func TestUnionWillUseExactTypeIfAvailable(t *testing.T) {
	// NOTE: when compiled on 32-bit architecture where `3` might be int32, following test will fail.
	testCodecBidirectional(t, `["null","int","long","float","double"]`, 3, []byte("\x02\x06"))

	testCodecBidirectional(t, `["null","int","long","float","double"]`, int32(3), []byte("\x02\x06"))
	testCodecBidirectional(t, `["null","int","long","float","double"]`, int64(3), []byte("\x04\x06"))

	testCodecBidirectional(t, `["null","int","long","float","double"]`, 3.5, []byte("\x08\x00\x00\x00\x00\x00\x00\f@"))
	testCodecBidirectional(t, `["null","int","long","float","double"]`, float32(3.5), []byte("\x06\x00\x00\x60\x40"))
	testCodecBidirectional(t, `["null","int","long","float","double"]`, float64(3.5), []byte("\x08\x00\x00\x00\x00\x00\x00\f@"))
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

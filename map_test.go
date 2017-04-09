package goavro_test

import "testing"

func TestMapPrimitiveWrappers(t *testing.T) {
	testCodecBidirectional(t, `{"type":"boolean"}`, false, []byte{0})
	testCodecBidirectional(t, `{"type":"boolean"}`, true, []byte{1})
	testCodecBidirectional(t, `{"type":"bytes"}`, []byte(""), []byte{0})
	testCodecBidirectional(t, `{"type":"bytes"}`, []byte("some bytes"), []byte("\x14some bytes"))
}

func TestMapInt(t *testing.T) {
	intMap := map[string]interface{}{"Helium": 2}
	testCodecBidirectional(t, `{"type":"map","values":"int"}`, intMap, []byte("\x02\x0cHelium\x04\x00"))
}

func TestMapString(t *testing.T) {
	stringMap := map[string]interface{}{"He": "Helium"}
	testCodecBidirectional(t, `{"type":"map","values":"string"}`, stringMap, []byte("\x02\x04He\x0cHelium\x00"))
}

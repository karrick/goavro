package goavro_test

import "testing"

func TestArrayNull(t *testing.T) {
	testCodecBidirectional(t, `{"type":"array","items":"null"}`, []interface{}{}, []byte{0})
	testCodecBidirectional(t, `{"type":"array","items":"null"}`, []interface{}{nil}, []byte{2, 0})
	testCodecBidirectional(t, `{"type":"array","items":"null"}`, []interface{}{nil, nil}, []byte{4, 0})
}

func TestArrayReceiveSliceEmptyInterface(t *testing.T) {
	testCodecBidirectional(t, `{"type":"array","items":"boolean"}`, []interface{}{}, []byte{0})
	testCodecBidirectional(t, `{"type":"array","items":"boolean"}`, []interface{}{false}, []byte{2, 0, 0})
	testCodecBidirectional(t, `{"type":"array","items":"boolean"}`, []interface{}{true}, []byte{2, 1, 0})
	testCodecBidirectional(t, `{"type":"array","items":"boolean"}`, []interface{}{false, false}, []byte{4, 0, 0, 0})
	testCodecBidirectional(t, `{"type":"array","items":"boolean"}`, []interface{}{true, true}, []byte{4, 1, 1, 0})
}

func TestArrayReceiveSliceIntInterface(t *testing.T) {
	testCodecBidirectional(t, `{"type":"array","items":"int"}`, []int{}, []byte("\x00"))
	testCodecBidirectional(t, `{"type":"array","items":"int"}`, []int{1}, []byte("\x02\x02\x00"))
	testCodecBidirectional(t, `{"type":"array","items":"int"}`, []int{1, 2}, []byte("\x04\x02\x04\x00"))

	// NOTE: does not error because only the length is encoded, but the items encoder is never
	// invoked to detect it is the wrong slice type
	testCodecBidirectional(t, `{"type":"array","items":"int"}`, []string{}, []byte{0})
}

func TestArrayBadType(t *testing.T) {
	testBadDatumType(t, `{"type":"array","items":"int"}`, []string{"1"}, []byte{2})
	testBadDatumType(t, `{"type":"array","items":"int"}`, []string{"1", "2"}, []byte{4})
}

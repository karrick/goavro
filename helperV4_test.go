package goavro_test

import (
	"bytes"
	"encoding/json"
	"testing"

	v4 "github.com/linkedin/goavro"
)

func newCodecUsingV4(tb testing.TB, schema string) v4.Codec {
	codec, err := v4.NewCodec(schema)
	if err != nil {
		tb.Fatal(err)
	}
	return codec
}

func nativeFromAvroUsingV4(tb testing.TB, avroBlob []byte) ([]interface{}, v4.Codec) {
	ocf, err := v4.NewReader(v4.FromReader(bytes.NewReader(avroBlob)))
	if err != nil {
		tb.Fatal(err)
	}

	var nativeData []interface{}
	for ocf.Scan() {
		datum, err := ocf.Read()
		if err != nil {
			break // Read error sets OCFReader error
		}
		nativeData = append(nativeData, datum)
	}
	if err := ocf.Close(); err != nil {
		tb.Fatal(err)
	}

	codec, err := v4.NewCodec(ocf.DataSchema)
	if err != nil {
		tb.Fatal(err)
	}
	return nativeData, codec
}

func binaryFromNativeUsingV4(tb testing.TB, codec v4.Codec, nativeData []interface{}) [][]byte {
	binaryData := make([][]byte, len(nativeData))
	for i, datum := range nativeData {
		bb := new(bytes.Buffer)
		err := codec.Encode(bb, datum)
		if err != nil {
			tb.Fatal(err)
		}
		binaryData[i] = bb.Bytes()
	}
	return binaryData
}

func nativeFromBinaryUsingV4(tb testing.TB, codec v4.Codec, binaryData [][]byte) []interface{} {
	nativeData := make([]interface{}, len(binaryData))
	for i, binaryDatum := range binaryData {
		bb := bytes.NewReader(binaryDatum)
		nativeDatum, err := codec.Decode(bb)
		if err != nil {
			tb.Fatal(err)
		}
		if bb.Len() > 0 {
			tb.Fatalf("Decode ought to have emptied buffer")
		}
		nativeData[i] = nativeDatum
	}
	return nativeData
}

func textFromNativeUsingJSONMarshal(tb testing.TB, _ v4.Codec, nativeData []interface{}) [][]byte {
	textData := make([][]byte, len(nativeData))
	for i, nativeDatum := range nativeData {
		textDatum, err := json.Marshal(nativeDatum)
		if err != nil {
			tb.Fatal(err)
		}
		textData[i] = textDatum
	}
	return textData
}

func nativeFromTextUsingJSONUnmarshal(tb testing.TB, _ v4.Codec, textData [][]byte) []interface{} {
	nativeData := make([]interface{}, len(textData))
	for i, textDatum := range textData {
		var nativeDatum interface{}
		err := json.Unmarshal(textDatum, &nativeDatum)
		if err != nil {
			tb.Fatal(err)
		}
		nativeData[i] = nativeDatum
	}
	return nativeData
}

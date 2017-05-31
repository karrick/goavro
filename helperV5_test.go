package goavro_test

import (
	"bytes"
	"testing"

	v5 "github.com/karrick/goavro"
)

func newCodecUsingV5(tb testing.TB, schema string) *v5.Codec {
	codec, err := v5.NewCodec(schema)
	if err != nil {
		tb.Fatal(err)
	}
	return codec
}

func nativeFromAvroUsingV5(tb testing.TB, avroBlob []byte) ([]interface{}, *v5.Codec) {
	ocf, err := v5.NewOCFReader(bytes.NewReader(avroBlob))
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
	if err := ocf.Err(); err != nil {
		tb.Fatal(err)
	}
	return nativeData, ocf.Codec()
}

func binaryFromNativeUsingV5(tb testing.TB, codec *v5.Codec, nativeData []interface{}) [][]byte {
	binaryData := make([][]byte, len(nativeData))
	for i, datum := range nativeData {
		binaryDatum, err := codec.BinaryFromNative(nil, datum)
		if err != nil {
			tb.Fatal(err)
		}
		binaryData[i] = binaryDatum
	}
	return binaryData
}

func textFromNativeUsingV5(tb testing.TB, codec *v5.Codec, nativeData []interface{}) [][]byte {
	textData := make([][]byte, len(nativeData))
	for i, nativeDatum := range nativeData {
		textDatum, err := codec.TextualFromNative(nil, nativeDatum)
		if err != nil {
			tb.Fatal(err)
		}
		textData[i] = textDatum
	}
	return textData
}

func nativeFromBinaryUsingV5(tb testing.TB, codec *v5.Codec, binaryData [][]byte) []interface{} {
	nativeData := make([]interface{}, len(binaryData))
	for i, binaryDatum := range binaryData {
		nativeDatum, buf, err := codec.NativeFromBinary(binaryDatum)
		if err != nil {
			tb.Fatal(err)
		}
		if len(buf) > 0 {
			tb.Fatalf("BinaryDecode ought to have returned nil buffer: %v", buf)
		}
		nativeData[i] = nativeDatum
	}
	return nativeData
}

func nativeFromTextUsingV5(tb testing.TB, codec *v5.Codec, textData [][]byte) []interface{} {
	nativeData := make([]interface{}, len(textData))
	for i, textDatum := range textData {
		nativeDatum, buf, err := codec.NativeFromTextual(textDatum)
		if err != nil {
			tb.Fatal(err)
		}
		if len(buf) > 0 {
			tb.Fatalf("TextDecode ought to have returned nil buffer: %v", buf)
		}
		nativeData[i] = nativeDatum
	}
	return nativeData
}

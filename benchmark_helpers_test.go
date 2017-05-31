package goavro_test

import (
	"io/ioutil"
	"testing"
)

func benchmarkNewCodecUsingV4(b *testing.B, avscPath string) {
	schema, err := ioutil.ReadFile(avscPath)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newCodecUsingV4(b, string(schema))
	}
}

func benchmarkNewCodecUsingV5(b *testing.B, avscPath string) {
	schema, err := ioutil.ReadFile(avscPath)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newCodecUsingV5(b, string(schema))
	}
}

func benchmarkNativeFromAvroUsingV4(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = nativeFromAvroUsingV4(b, avroBlob)
	}
}

func benchmarkNativeFromAvroUsingV5(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = nativeFromAvroUsingV5(b, avroBlob)
	}
}

func benchmarkBinaryFromNativeUsingV4(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV4(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = binaryFromNativeUsingV4(b, codec, nativeData)
	}
}

func benchmarkBinaryFromNativeUsingV5(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV5(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = binaryFromNativeUsingV5(b, codec, nativeData)
	}
}

func benchmarkNativeFromBinaryUsingV4(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV4(b, avroBlob)
	binaryData := binaryFromNativeUsingV4(b, codec, nativeData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nativeFromBinaryUsingV4(b, codec, binaryData)
	}
}

func benchmarkNativeFromBinaryUsingV5(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV5(b, avroBlob)
	binaryData := binaryFromNativeUsingV5(b, codec, nativeData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nativeFromBinaryUsingV5(b, codec, binaryData)
	}
}

func benchmarkTextualFromNativeUsingJSONMarshal(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV4(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = textFromNativeUsingJSONMarshal(b, codec, nativeData)
	}
}

func benchmarkTextualFromNativeUsingV5(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV5(b, avroBlob)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = textFromNativeUsingV5(b, codec, nativeData)
	}
}

func benchmarkNativeFromTextualUsingJSONUnmarshal(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV4(b, avroBlob)
	textData := textFromNativeUsingJSONMarshal(b, codec, nativeData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nativeFromTextUsingJSONUnmarshal(b, codec, textData)
	}
}

func benchmarkNativeFromTextualUsingV5(b *testing.B, avroPath string) {
	avroBlob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}
	nativeData, codec := nativeFromAvroUsingV5(b, avroBlob)
	textData := textFromNativeUsingV5(b, codec, nativeData)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = nativeFromTextUsingV5(b, codec, textData)
	}
}

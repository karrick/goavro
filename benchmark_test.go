package goavro_test

import "testing"

func BenchmarkNewCodecUsingV4(b *testing.B) {
	benchmarkNewCodecUsingV4(b, "fixtures/quickstop.avsc")
}

func BenchmarkNewCodecUsingV5(b *testing.B) {
	benchmarkNewCodecUsingV5(b, "fixtures/quickstop.avsc")
}

func BenchmarkNativeFromAvroUsingV4(b *testing.B) {
	benchmarkNativeFromAvroUsingV4(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromAvroUsingV5(b *testing.B) {
	benchmarkNativeFromAvroUsingV5(b, "fixtures/quickstop-null.avro")
}

func BenchmarkBinaryFromNativeUsingV4(b *testing.B) {
	benchmarkBinaryFromNativeUsingV4(b, "fixtures/quickstop-null.avro")
}

func BenchmarkBinaryFromNativeUsingV5(b *testing.B) {
	benchmarkBinaryFromNativeUsingV5(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromBinaryUsingV4(b *testing.B) {
	benchmarkNativeFromBinaryUsingV4(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromBinaryUsingV5(b *testing.B) {
	benchmarkNativeFromBinaryUsingV5(b, "fixtures/quickstop-null.avro")
}

func BenchmarkTextFromNativeUsingJSONMarshal(b *testing.B) {
	benchmarkTextFromNativeUsingJSONMarshal(b, "fixtures/quickstop-null.avro")
}

func BenchmarkTextFromNativeUsingV5(b *testing.B) {
	benchmarkTextFromNativeUsingV5(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromTextUsingJSONUnmarshal(b *testing.B) {
	benchmarkNativeFromTextUsingJSONUnmarshal(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromTextUsingV5(b *testing.B) {
	benchmarkNativeFromTextUsingV5(b, "fixtures/quickstop-null.avro")
}

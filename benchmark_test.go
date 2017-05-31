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

func BenchmarkTextualFromNativeUsingJSONMarshal(b *testing.B) {
	benchmarkTextualFromNativeUsingJSONMarshal(b, "fixtures/quickstop-null.avro")
}

func BenchmarkTextualFromNativeUsingV5(b *testing.B) {
	benchmarkTextualFromNativeUsingV5(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromTextualUsingJSONUnmarshal(b *testing.B) {
	benchmarkNativeFromTextualUsingJSONUnmarshal(b, "fixtures/quickstop-null.avro")
}

func BenchmarkNativeFromTextualUsingV5(b *testing.B) {
	benchmarkNativeFromTextualUsingV5(b, "fixtures/quickstop-null.avro")
}

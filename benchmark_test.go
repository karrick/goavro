package goavro_test

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	v5 "github.com/karrick/goavro"
	v4 "github.com/linkedin/goavro"
)

func sliceOfData(tb testing.TB) ([]interface{}, *v5.Codec) {
	// fh, err := os.Open("fixtures/weather-null.avro") // 1491868942.avro")
	fh, err := os.Open("fixtures/quickstop-deflate.avro")
	if err != nil {
		tb.Fatal(err)
	}
	defer fh.Close()

	ocfr, err := v5.NewOCFReader(fh)
	if err != nil {
		tb.Fatal(err)
	}
	var data []interface{}

	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			break // Read error sets OCFReader error
		}
		data = append(data, datum)
	}
	if err := ocfr.Err(); err != nil {
		tb.Fatal(err)
	}
	return data, ocfr.Codec()
}

func sliceOfBuffers(tb testing.TB, c *v5.Codec, data []interface{}) [][]byte {
	encodedData := make([][]byte, len(data))
	for i, datum := range data {
		buf, err := c.TextEncode(nil, datum)
		if err != nil {
			tb.Fatal(err)
		}
		encodedData[i] = buf
	}
	return encodedData
}

func BenchmarkMarshalJSON(b *testing.B) {
	var buf []byte
	var err error
	data, _ := sliceOfData(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, datum := range data {
			buf, err = json.Marshal(datum)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	_ = buf
}

func BenchmarkTextEncode(b *testing.B) {
	var datum interface{}
	var err error
	decodedData, codec := sliceOfData(b)
	bufs := sliceOfBuffers(b, codec, decodedData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, buf := range bufs {
			datum, _, err = codec.TextDecode(buf)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	_ = datum
}

func BenchmarkUnmarshalJSON(b *testing.B) {
	var datum interface{}
	var err error
	decodedData, codec := sliceOfData(b)
	bufs := sliceOfBuffers(b, codec, decodedData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, buf := range bufs {
			err = json.Unmarshal(buf, &datum)
			if err != nil {
				b.Fatalf("%s: %s", err, buf)
			}
		}
	}
	_ = datum
}

func BenchmarkTextDecode(b *testing.B) {
	var buf []byte
	var err error
	data, codec := sliceOfData(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, datum := range data {
			buf, err = codec.TextEncode(nil, datum)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	_ = buf
}

func benchmarkNewCodecV4(b *testing.B, avscPath string) {
	schemaSpecification, err := ioutil.ReadFile(avscPath)
	if err != nil {
		b.Fatal(err)
	}
	schemaString := string(schemaSpecification)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = v4.NewCodec(schemaString)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkNewCodecV5(b *testing.B, avscPath string) {
	schemaSpecification, err := ioutil.ReadFile(avscPath)
	if err != nil {
		b.Fatal(err)
	}
	schemaString := string(schemaSpecification)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = v5.NewCodec(schemaString)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewCodecV4(b *testing.B) {
	benchmarkNewCodecV4(b, "fixtures/quickstop.avsc")
}

func BenchmarkNewCodecV5(b *testing.B) {
	benchmarkNewCodecV5(b, "fixtures/quickstop.avsc")
}

func benchmarkDecodeV4(b *testing.B, avscPath, avroPath string) {
	schemaSpecification, err := ioutil.ReadFile(avscPath)
	if err != nil {
		b.Fatal(err)
	}
	c, err := v4.NewCodec(string(schemaSpecification))
	if err != nil {
		b.Fatal(err)
	}
	blob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = c.Decode(bytes.NewReader(blob))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkDecodeV5(b *testing.B, avscPath, avroPath string) {
	schemaSpecification, err := ioutil.ReadFile(avscPath)
	if err != nil {
		b.Fatal(err)
	}
	c, err := v5.NewCodec(string(schemaSpecification))
	if err != nil {
		b.Fatal(err)
	}
	blob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := c.BinaryDecode(blob)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkEncodeV4(b *testing.B, avscPath, avroPath string) {
	schemaSpecification, err := ioutil.ReadFile(avscPath)
	if err != nil {
		b.Fatal(err)
	}
	c, err := v4.NewCodec(string(schemaSpecification))
	if err != nil {
		b.Fatal(err)
	}
	blob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}

	datum, err := c.Decode(bytes.NewReader(blob))
	if err != nil {
		b.Fatal(err)
	}

	bb := bytes.NewBuffer(make([]byte, 0, len(blob)))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bb.Reset()
		err := c.Encode(bb, datum)
		if err != nil {
			b.Fatal(err)
		}
	}
	if false {
		b.Logf("original file size: %d; encoded blob size: %d", len(blob), bb.Len())
	}
}

func benchmarkEncodeV5(b *testing.B, avscPath, avroPath string) {
	schemaSpecification, err := ioutil.ReadFile(avscPath)
	if err != nil {
		b.Fatal(err)
	}
	c, err := v5.NewCodec(string(schemaSpecification))
	if err != nil {
		b.Fatal(err)
	}
	blob, err := ioutil.ReadFile(avroPath)
	if err != nil {
		b.Fatal(err)
	}

	datum, _, err := c.BinaryDecode(blob)
	if err != nil {
		b.Fatal(err)
	}

	// b.Log(datum)

	buf := make([]byte, 0, len(blob))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf, err = c.BinaryEncode(buf, datum)
		if err != nil {
			b.Fatal(err)
		}
	}
	if false {
		b.Logf("original file size: %d; encoded blob size: %d", len(blob), len(buf))
	}
}

func BenchmarkDecodeV4(b *testing.B) {
	benchmarkDecodeV4(b, "fixtures/quickstop.avsc", "fixtures/quickstop-null.avro")
}

func BenchmarkDecodeV5(b *testing.B) {
	benchmarkDecodeV5(b, "fixtures/quickstop.avsc", "fixtures/quickstop-null.avro")
}

func BenchmarkEncodeV4(b *testing.B) {
	benchmarkEncodeV4(b, "fixtures/quickstop.avsc", "fixtures/quickstop-null.avro")
}

func BenchmarkEncodeV5(b *testing.B) {
	benchmarkEncodeV5(b, "fixtures/quickstop.avsc", "fixtures/quickstop-null.avro")
}

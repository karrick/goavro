package goavro

import (
	"strings"
	"testing"
)

func TestCrashers_OCFReader(t *testing.T) {
	var crashers = map[string]string{
		"scan: negative block sizes": "Obj\x01\x04\x16avro.schema\x96\x05{" +
			"\"type\":\"record\",\"nam" +
			"e\":\"c0000000\",\"00000" +
			"0000\":\"00000000000\"," +
			"\"fields\":[{\"name\":\"u" +
			"0000000\",\"type\":\"str" +
			"ing\",\"000\":\"00000000" +
			"0000\"},{\"name\":\"c000" +
			"000\",\"type\":\"string\"" +
			",\"000\":\"000000000000" +
			"00000000000000000000" +
			"0\"},{\"name\":\"t000000" +
			"00\",\"type\":\"long\",\"0" +
			"00\":\"000000000000000" +
			"0000000000000000\"}]," +
			"\"0000\":\"000000000000" +
			"00000000000000000000" +
			"00000000\"}\x14000000000" +
			"0\b0000\x000000000000000" +
			"0000\xd90",
	}

	for testName, f := range crashers {
		t.Logf("Testing: %s", testName)
		NewOCFReader(strings.NewReader(f))
	}
}

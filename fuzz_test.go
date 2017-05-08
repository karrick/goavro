package goavro

import (
	"bytes"
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
		"bytesReader: size overflow":           "Obj\x010\xa2\x8f\xdc\xf8\xa30",
		"metadataReader: initialSize overflow": "Obj\x01\xa6ʌ\xce0",
	}

	for testName, f := range crashers {
		t.Logf("Testing: %s", testName)
		NewOCFReader(strings.NewReader(f))
	}
}

func TestCrashers_OCF_e2e(t *testing.T) {
	var crashers = map[string]string{
		"map: initialSize overflow": "Obj\x01\x04\x14avro.codec\bnul" +
			"l\x16avro.schema\xa2\x0e{\"typ" +
			"e\":\"record\",\"name\":\"" +
			"test_schema\",\"fields" +
			"\":[{\"name\":\"string\"," +
			"\"type\":\"string\",\"doc" +
			"\":\"Meaningless strin" +
			"g of characters\"},{\"" +
			"name\":\"simple_map\",\"" +
			"type\":{\"type\":\"map\"," +
			"\"values\":\"int\"}},{\"n" +
			"ame\":\"complex_map\",\"" +
			"type\":{\"type\":\"map\"," +
			"\"values\":{\"type\":\"ma" +
			"p\",\"values\":\"string\"" +
			"}}},{\"name\":\"union_s" +
			"tring_null\",\"type\":[" +
			"\"null\",\"string\"]},{\"" +
			"name\":\"union_int_lon" +
			"g_null\",\"type\":[\"int" +
			"\",\"long\",\"null\"]},{\"" +
			"name\":\"union_float_d" +
			"ouble\",\"type\":[\"floa" +
			"t\",\"double\"]},{\"name" +
			"\":\"fixed3\",\"type\":{\"" +
			"type\":\"fixed\",\"name\"" +
			":\"fixed3\",\"size\":3}}" +
			",{\"name\":\"fixed2\",\"t" +
			"ype\":{\"type\":\"fixed\"" +
			",\"name\":\"fixed2\",\"si" +
			"ze\":2}},{\"name\":\"enu" +
			"m\",\"type\":{\"type\":\"e" +
			"num\",\"name\":\"Suit\",\"" +
			"symbols\":[\"SPADES\",\"" +
			"HEARTS\",\"DIAMONDS\",\"" +
			"CLUBS\"]}},{\"name\":\"r" +
			"ecord\",\"type\":{\"type" +
			"\":\"record\",\"name\":\"r" +
			"ecord\",\"fields\":[{\"n" +
			"ame\":\"value_field\",\"" +
			"type\":\"string\"}],\"al" +
			"iases\":[\"Reco\x9adAlias" +
			"\"]}},{\"name\":\"array_" +
			"of_boolean\",\"type\":{" +
			"\"type\":\"array\",\"item" +
			"s\":\"boolean\"}},{\"nam" +
			"e\":\"bytes\",\"type\":\"b" +
			"ytes\"}]}\x00\xfeJ\x17\u007f\xb4r\x11\x0e\x96&\x0e" +
			"\xda<\xed\x86\xf6\x06\xfa\x05(OMG SPARK I" +
			"S AWESOME\x04\x06abc\x02\x06bcd\x0e" +
			"\x00\x02\x06key\x03\x80\x00\x02d\x02a\x02b\x00\x00\x01\x00\x00" +
			"\x00\x00\x00\x04\x00\xdb\x0fI@\x02\x03\x04\x11\x12\x00\xb6\x01Two" +
			" things are infinite" +
			": the universe and h" +
			"uman stupidity; and " +
			"I'm not sure about u" +
			"niverse.\x06\x01\x00\x00\x00\x06ABCT\x00e" +
			"rran is IMBA!\x04\x06qqq\x84\x01" +
			"\x06mmm\x00\x00\x02\x06key\x04\x023\x024\x021\x02K" +
			"��~\x02\x84\x01\x02`\xaa\xaa\xaa\xaa\xaa\x1a@\a" +
			"\a\a\x01\x02\x06\x9e\x01Life did no\xef\xbf" +
			"\xbd\ttend to make us pe" +
			"rfect. Whoever is pe" +
			"rfect `elongs in a m" +
			"useum.\x00\x00$The cake is" +
			" a LIE!\x00\x02\x06key\x00\x00\x00\x04\x02\x00\x00" +
			"\x00\x00\x00\x00\x00\x00\x11\"\t\x10\x90\x04\x16TEST_ST" +
			"R123\x00\x04\x00\x02S\xfeJ\x17\u007f\xb4r\x11\x0e\x96&\x0e" +
			"\xda<\xed\x86\xf6",
		"array: initialSize overflow": "Obj\x01\x04\x14avro.codec\bnul" +
			"l\x16avro.schema\xa2\x0e{\"typ" +
			"e\":\"record\",\"name\":\"" +
			"test_schema\",\"fields" +
			"\":[{\"name\":\"string\"," +
			"\"type\":\"string\",\"doc" +
			"\":\"Meaningless strin" +
			"g of characters\"},{\"" +
			"name\":\"simple_map\",\"" +
			"type\":{\"type\":\"map\"," +
			"\"values\":\"int\"}},{\"n" +
			"ame\":\"complex_map\",\"" +
			"type\":{\"type\":\"map\"," +
			"\"values\":{\"type\":\"ma" +
			"p\",\"values\":\"string\"" +
			"}}},{\"name\":\"union_s" +
			"tring_null\",\"type\":[" +
			"\"null\",\"string\"]},{\"" +
			"name\":\"union_int_lon" +
			"g_null\",\"type\":[\"int" +
			"\",\"long\",\"null\"]},{\"" +
			"name\":\"union_float_d" +
			"ouble\",\"type\":[\"floa" +
			"t\",\"double\"]},{\"name" +
			"\":\"fixed3\",\"type\":{\"" +
			"type\":\"fixed\",\"name\"" +
			":\"fixed3\",\"size\":3}}" +
			",{\"name\":\"fixed2\",\"t" +
			"ype\":{\"type\":\"fixed\"" +
			",\"name\":\"fixed2\",\"si" +
			"ze\":2}},{\"name\":\"enu" +
			"m\",\"type\":{\"type\":\"e" +
			"num\",\"name\":\"Suit\",\"" +
			"symbols\":[\"SPADES\",\"" +
			"HEARTS\",\"DIAMONDS\",\"" +
			"CLUBS\"]}},{\"name\":\"r" +
			"ecord\",\"type\":{\"type" +
			"\":\"record\",\"name\":\"r" +
			"ecord\",\"fields\":[{\"n" +
			"ame\":\"value_field\",\"" +
			"type\":\"string\"}],\"al" +
			"iases\":[\"Reco\x9adAlias" +
			"\"]}},{\"name\":\"array_" +
			"of_boolean\",\"type\":{" +
			"\"type\":\"array\",\"item" +
			"s\":\"boolean\"}},{\"nam" +
			"e\":\"bytes\",\"type\":\"b" +
			"ytes\"}]}\x00\xfeJ\x17\u007f\xb4r\x11\x0e\x96&\x0e" +
			"\xda<\xed\x86\xf6\x06\xfa\x05(OMG SPARK I" +
			"S AWESOME\x04\x06abc\x02\x06bcd\x0e" +
			"\x00\x02\x06key\x03\x80\x00\x02d\x02a\x02b\x00\x00\x01\x00\x00" +
			"\x00\x00\x00\x04\x00\xdb\x0fI@\x02\x03\x04\x11\x12\x00\xb6\x01Two" +
			" things are infinite" +
			": the universe and h" +
			"uman stupidity; and " +
			"I'm not sure about u" +
			"n������\xef" +
			"\xbf\xbd�is IMBA!\x04\x06qqq\x84\x01" +
			"\x06mmm\x00\x00\x02\x06key\x04\x023\x024\x021\x022" +
			"\x00\x00\x02\x06123\x02\x84\x01\x02`\xaa\xaa\xaa\xaa\xaa\x1a@\a" +
			"\a\a\x01\x02\x06\x9e\x01Life did no\xef\xbf" +
			"\xbd\ttend to make us pe" +
			"rfect. Whoever is pe" +
			"rfect `elongs in a m" +
			"useum.\x00\x00$The cake is" +
			" a LIE!\x00\x02\x06key\x00\x00\x00\x04\x02\x00\x00" +
			"\x00\x00\x00\x00\x00\x00\x11\"\t\x10\x90\x04\x16TEST_ST" +
			"R123\x00\x04\x00\x02S\xfeJ\x17\u007f\xb4r\x11\x0e\x96&\x0e" +
			"\xda<\xed\x86\xf6",
	}

	for testName, f := range crashers {
		t.Logf("Testing: %s", testName)

		// TODO: replace this with a call out to the e2e Fuzz function
		ocfr, err := NewOCFReader(strings.NewReader(f))
		if err != nil {
			continue
		}

		var datums []interface{}
		for ocfr.Scan() {
			datum, err := ocfr.Read()
			if err != nil {
				continue
			}
			datums = append(datums, datum)
		}

		b := new(bytes.Buffer)
		ocfw, err := NewOCFWriter(
			OCFWriterConfig{
				W:      b,
				Schema: ocfr.Schema(),
			})
		if err := ocfw.Append(datums); err != nil {
			panic(err)
		}
	}
}

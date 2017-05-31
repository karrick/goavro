package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/karrick/goavro"
)

const (
	magicBytes     = "Obj\x01"
	metadataSchema = `{"type":"map","values":"bytes"}`
	syncLength     = 16
)

func usage() {
	executable, err := os.Executable()
	if err != nil {
		executable = os.Args[0]
	}
	base := filepath.Base(executable)
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", base)
	fmt.Fprintf(os.Stderr, "\t%s [-compress null|deflate|snappy] schema.avsc input.dat output.avro\n", base)
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	compress := flag.String("compress", "null", "compression codec ('null', 'deflate', 'snappy'; default: 'null')")
	flag.Parse()

	var compression goavro.Compression
	switch *compress {
	case goavro.CompressionNullLabel:
		// the goavro.Compression zero value specifies the null codec
	case goavro.CompressionDeflateLabel:
		compression = goavro.CompressionDeflate
	case goavro.CompressionSnappyLabel:
		compression = goavro.CompressionSnappy
	default:
		bail(fmt.Errorf("unsupported compression codec: %s", *compress))
	}

	if len(flag.Args()) != 3 {
		usage()
	}

	schemaBytes, err := ioutil.ReadFile(flag.Arg(0))
	if err != nil {
		bail(err)
	}

	dataBytes, err := ioutil.ReadFile(flag.Arg(1))
	if err != nil {
		bail(err)
	}

	bd, err := goavro.NewCodec(string(schemaBytes))
	if err != nil {
		bail(err)
	}

	datum, _, err := bd.NativeFromBinary(dataBytes)
	if err != nil {
		bail(err)
	}

	fh, err := os.Create(flag.Arg(2))
	if err != nil {
		bail(err)
	}
	defer func(ioc io.Closer) {
		if err := ioc.Close(); err != nil {
			bail(err)
		}
	}(fh)

	ocfw, err := goavro.NewOCFWriter(goavro.OCFWriterConfig{
		W:           fh,
		Schema:      string(schemaBytes),
		Compression: compression,
	})
	if err != nil {
		bail(err)
	}

	if err = ocfw.Append([]interface{}{datum}); err != nil {
		bail(err)
	}
}

func bail(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(1)
}

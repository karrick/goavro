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

func usage() {
	executable, err := os.Executable()
	if err != nil {
		executable = os.Args[0]
	}
	base := filepath.Base(executable)
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", base)
	fmt.Fprintf(os.Stderr, "\t%s [-v] [-summary] [-bc N] [-compression null|deflate|snappy] [-schema new-schema.avsc] [from-file to-file]\n", base)
	fmt.Fprintf(os.Stderr, "\tAs a special case, when there are no filename arguments, %s will read\n", base)
	fmt.Fprintf(os.Stderr, "\tfrom its standard input and write to its standard output.\n")
	flag.PrintDefaults()
	os.Exit(2)
}

var (
	blockCount                      *int
	compressionName, schemaPathname *string
	summary, verbose                *bool
)

func init() {
	compressionName = flag.String("compression", "", "compression codec ('null', 'deflate', 'snappy'; default: use existing compression)")
	blockCount = flag.Int("bc", 0, "max count of items in each block (default: zero implies no limit)")
	schemaPathname = flag.String("schema", "", "pathname to new schema (default: use existing schema)")
	summary = flag.Bool("summary", false, "print summary information to stderr")
	verbose = flag.Bool("v", false, "print verbose information to stderr (implies: -summary)")
}

func main() {
	flag.Parse()

	if *blockCount < 0 {
		bail(fmt.Errorf("count must be greater or equal to 0: %d", *blockCount))
	}

	if *verbose {
		*summary = true
	}

	var fromF io.ReadCloser
	var toF io.WriteCloser
	var err error

	switch len(flag.Args()) {
	case 0:
		stat, err := os.Stdin.Stat()
		if err != nil {
			bail(err)
		}
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			usage()
		}
		if *summary {
			fmt.Fprintf(os.Stderr, "reading from stdin\n")
		}
		stat, err = os.Stdout.Stat()
		if err != nil {
			bail(err)
		}
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			usage()
		}
		if *summary {
			fmt.Fprintf(os.Stderr, "writing to stdout\n")
		}
		fromF = os.Stdin
		toF = os.Stdout
	case 2:
		if *summary {
			fmt.Fprintf(os.Stderr, "reading from %s\n", flag.Arg(0))
		}
		fromF, err = os.Open(flag.Arg(0))
		if err != nil {
			bail(err)
		}
		defer func(ioc io.Closer) {
			if err := ioc.Close(); err != nil {
				bail(err)
			}
		}(fromF)

		toF, err = os.Create(flag.Arg(1))
		if err != nil {
			bail(err)
		}
		defer func(ioc io.Closer) {
			if err := ioc.Close(); err != nil {
				bail(err)
			}
		}(toF)
		if *summary {
			fmt.Fprintf(os.Stderr, "writing to %s\n", flag.Arg(1))
		}
	default:
		usage()
	}

	// NOTE: Convert fromF to OCFReader
	ocfr, err := goavro.NewOCFReader(fromF)
	if err != nil {
		bail(err)
	}

	compression := ocfr.CompressionID()

	inputCompressionName := ocfr.CompressionName()
	outputCompressionName := inputCompressionName

	if *compressionName != "" {
		switch *compressionName {
		case goavro.CompressionNullLabel:
			compression = goavro.CompressionNull
		case goavro.CompressionDeflateLabel:
			compression = goavro.CompressionDeflate
		case goavro.CompressionSnappyLabel:
			compression = goavro.CompressionSnappy
		default:
			bail(fmt.Errorf("unsupported compression codec: %s", *compressionName))
		}
		outputCompressionName = *compressionName
	}

	if *summary {
		fmt.Fprintf(os.Stderr, "input compression algorithm: %s\n", inputCompressionName)
		fmt.Fprintf(os.Stderr, "output compression algorithm: %s\n", outputCompressionName)
	}

	// NOTE: Either use schema from reader, or attempt to use new schema
	var newSchema string
	if *schemaPathname == "" {
		newSchema = ocfr.Schema()
	} else {
		schemaBytes, err := ioutil.ReadFile(*schemaPathname)
		if err != nil {
			bail(err)
		}
		newSchema = string(schemaBytes)
	}

	// NOTE: Convert toF to OCFWriter
	ocfw, err := goavro.NewOCFWriter(goavro.OCFWriterConfig{
		W:           toF,
		Compression: compression,
		Schema:      newSchema,
	})
	if err != nil {
		bail(err)
	}

	if err := transcode(ocfr, ocfw); err != nil {
		bail(err)
	}
}

func transcode(from *goavro.OCFReader, to *goavro.OCFWriter) error {
	var blocksRead, blocksWritten, itemsRead int

	var block []interface{}
	if *blockCount > 0 {
		block = make([]interface{}, 0, *blockCount)
	}

	for from.Scan() {
		datum, err := from.Read()
		if err != nil {
			break
		}

		itemsRead++
		block = append(block, datum)

		endOfBlock := from.RemainingItems() == 0
		if endOfBlock {
			blocksRead++
			if *verbose {
				fmt.Fprintf(os.Stderr, "read block with %d items\n", len(block))
			}
		}

		// NOTE: When blockCount is 0, user wants each destination block to have
		// the same number of items as its corresponding source block. However,
		// when blockCount is greater than 0, user wants specified block count
		// sizes.
		if (*blockCount == 0 && endOfBlock) || (*blockCount > 0 && len(block) == *blockCount) {
			if err := writeBlock(to, block); err != nil {
				return err
			}
			blocksWritten++
			block = block[:0] // set slice length to 0 in order to re-use allocated underlying array
		}
	}

	var err error

	// append all remaining items (condition can only be true used when *blockCount > 0)
	if len(block) > 0 {
		if err = writeBlock(to, block); err == nil {
			blocksWritten++
		}
	}

	// if no write error, then return any read error encountered
	if err == nil {
		err = from.Err()
	}

	if *summary {
		fmt.Fprintf(os.Stderr, "read %d items\n", itemsRead)
		fmt.Fprintf(os.Stderr, "wrote %d blocks\n", blocksWritten)
	}

	return err
}

func writeBlock(to *goavro.OCFWriter, block []interface{}) error {
	if *verbose {
		fmt.Fprintf(os.Stderr, "writing block with %d items\n", len(block))
	}
	return to.Append(block)
}

func bail(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(1)
}

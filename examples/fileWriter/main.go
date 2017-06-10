package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/karrick/goavro"
)

func main() {
	compressionName := flag.String("compression", "null", "compression codec ('null', 'deflate', 'snappy'; default: 'null')")
	flag.Parse()

	schemaBytes, err := ioutil.ReadFile("../../fixtures/weather.avsc")
	if err != nil {
		bail(err)
	}

	fh, err := os.Create("weather.avro")
	if err != nil {
		bail(err)
	}
	defer func(ioc io.Closer) {
		if err := ioc.Close(); err != nil {
			bail(err)
		}
	}(fh)

	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               fh,
		Schema:          string(schemaBytes),
		CompressionName: *compressionName,
	})
	if err != nil {
		bail(err)
	}

	err = ocfw.Append([]interface{}{
		map[string]interface{}{"station": "011990-99999", "time": -619524000000, "temp": 0},
		map[string]interface{}{"station": "011990-99999", "time": -619506000000, "temp": 22},
	})
	if err != nil {
		bail(err)
	}
	err = ocfw.Append([]interface{}{
		map[string]interface{}{"station": "011990-99999", "time": -619484400000, "temp": -11},
		map[string]interface{}{"station": "012650-99999", "time": -655531200000, "temp": 111},
		map[string]interface{}{"station": "012650-99999", "time": -655509600000, "temp": 78},
	})
	if err != nil {
		bail(err)
	}
}

func bail(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(1)
}

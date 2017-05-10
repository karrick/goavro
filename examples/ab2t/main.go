package main

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/karrick/goavro"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		if err := dumpFromReader(os.Stdin); err != nil {
			bail(err)
		}
		return
	}
	for _, arg := range args {
		fh, err := os.Open(arg)
		if err != nil {
			bail(err)
		}
		if err := dumpFromReader(fh); err != nil {
			bail(err)
		}
		if err := fh.Close(); err != nil {
			bail(err)
		}
	}
}

func dumpFromReader(ior io.Reader) error {
	ocfr, err := goavro.NewOCFReader(ior)
	if err != nil {
		return err
	}

	codec := ocfr.Codec()
	data := make(chan interface{}, 100)
	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func(codec *goavro.Codec, data <-chan interface{}, wg *sync.WaitGroup) {
		for datum := range data {
			buf, err := codec.TextEncode(nil, datum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			fmt.Println(string(buf))
		}
		wg.Done()
	}(codec, data, wg)

	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			continue
		}
		data <- datum
	}
	close(data)
	wg.Wait()

	return ocfr.Err()
}

func bail(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(1)
}

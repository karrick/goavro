package main

import (
	"fmt"
	"io"
	"os"

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
	c := ocfr.Codec()
	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			return err
		}
		buf, err := c.TextEncode(nil, datum)
		if err != nil {
			return err
		}
		fmt.Println(string(buf))
	}
	return ocfr.Err()
}

func bail(err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err)
	os.Exit(1)
}

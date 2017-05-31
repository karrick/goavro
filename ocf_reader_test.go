package goavro_test

import (
	"bytes"
	"testing"

	"github.com/karrick/goavro"
)

func TestOCFReaderCannotReadMagicBytes(t *testing.T) {
	bb := bytes.NewBuffer([]byte("Obj")) // missing fourth byte
	_, err := goavro.NewOCFReader(bb)
	ensureError(t, err, "cannot read magic bytes")
}

func TestOCFReaderInvalidMagicBytes(t *testing.T) {
	bb := bytes.NewBuffer([]byte("...."))
	_, err := goavro.NewOCFReader(bb)
	ensureError(t, err, "invalid magic bytes")
}

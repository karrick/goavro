package goavro_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/karrick/goavro"
)

func TestOCFReaderCannotReadMagicBytes(t *testing.T) {
	bb := bytes.NewBuffer([]byte("Obj")) // missing fourth byte
	_, err := goavro.NewOCFReader(bb)
	if err == nil || !strings.Contains(err.Error(), "cannot read magic bytes") {
		t.Errorf("Actual: %v; Expected: %v", err, "cannot read magic bytes")
	}
}

func TestOCFReaderInvalidMagicBytes(t *testing.T) {
	bb := bytes.NewBuffer([]byte("...."))
	_, err := goavro.NewOCFReader(bb)
	if err == nil || !strings.Contains(err.Error(), "invalid magic bytes") {
		t.Errorf("Actual: %v; Expected: %v", err, "invalid magic bytes")
	}
}

package goavro

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"time"

	"github.com/golang/snappy"
)

// OCFWriterConfig is used to specify creation parameters for OCFWriter.
type OCFWriterConfig struct {
	W           io.Writer   // W specifies the io.Writer to send the encode the data, (required).
	Schema      string      // Schema specifies the Avro schema for the data to be encoded, (required).
	Compression Compression // Codec specifies the compression codec used, (optional). If omitted, defaults to "null" codec.
}

// OCFWriter is used to create an Avro Object Container File (OCF).
type OCFWriter struct {
	iow         io.Writer
	codec       *Codec
	syncMarker  []byte
	compression Compression
}

// NewOCFWriter returns a newly created OCFWriter which may be used to create an Avro Object
// Container File (OCF).
func NewOCFWriter(config OCFWriterConfig) (*OCFWriter, error) {
	if config.W == nil {
		return nil, errors.New("cannot create OCFWriter without io.WriteCloser: IOW")
	}
	if config.Schema == "" {
		return nil, errors.New("cannot create OCFWriter without Schema")
	}

	ocfw := &OCFWriter{iow: config.W}

	var avroCodec string
	switch config.Compression {
	case CompressionNull:
		avroCodec = CompressionNullLabel
	case CompressionDeflate:
		avroCodec = CompressionDeflateLabel
		ocfw.compression = CompressionDeflate
	case CompressionSnappy:
		avroCodec = CompressionSnappyLabel
		ocfw.compression = CompressionSnappy
	default:
		return nil, fmt.Errorf("cannot compress using unrecognized compression: %d", config.Compression)
	}

	var err error
	ocfw.codec, err = NewCodec(config.Schema)
	if err != nil {
		return nil, err
	}

	avroSchema, err := compactSchema(config.Schema)
	if err != nil {
		// this error is not expected, because NewCodec above already vetted schema
		return nil, err
	}

	// Create buffer for OCF header.  First 4 bytes are magic, and we'll use copy to fill them in,
	// so initialize buffer's length with 4, and its capacity equal to length of avro schema plus a constant.
	buf := make([]byte, 4, len(avroSchema)+48) // OCF header is usually about 48 bytes longer than its compressed schema
	_ = copy(buf, []byte(magicBytes))

	// file metadata, including the schema
	hm := map[string]interface{}{"avro.schema": avroSchema, "avro.codec": avroCodec}
	buf, err = metadataCodec.BinaryEncode(buf, hm)
	if err != nil {
		return nil, err
	}

	// The 16-byte, randomly-generated sync marker for this file.
	r := rand.New(rand.NewSource(time.Now().Unix()))
	ocfw.syncMarker = make([]byte, syncLength)
	for i := 0; i < syncLength; i++ {
		ocfw.syncMarker[i] = byte(r.Intn(256))
	}
	buf = append(buf, ocfw.syncMarker...)

	// emit OCF header
	_, err = ocfw.iow.Write(buf)
	if err != nil {
		return nil, err
	}

	return ocfw, nil
}

// Append appends one or more data items to an OCF file in a block.
func (ocf *OCFWriter) Append(data []interface{}) error {
	var block []byte // working buffer for encoding data values
	var err error

	for _, datum := range data {
		block, err = ocf.codec.BinaryEncode(block, datum)
		if err != nil {
			return err
		}
	}

	switch ocf.compression {
	case CompressionNull:
		// no-op

	case CompressionDeflate:
		// compress into new bytes buffer.
		bb := bytes.NewBuffer(make([]byte, 0, len(block)))

		// Writing bytes to cw will compress bytes and send to bb.
		cw, _ := flate.NewWriter(bb, flate.DefaultCompression)
		if _, err := cw.Write(block); err != nil {
			return err
		}
		if err := cw.Close(); err != nil {
			return err
		}
		block = bb.Bytes()

	case CompressionSnappy:
		compressed := snappy.Encode(nil, block)

		// OCF requires snappy to have CRC32 checksum after each snappy block
		compressed = append(compressed, []byte{0, 0, 0, 0}...)                                // expand slice so checksum will fit
		binary.BigEndian.PutUint32(compressed[len(compressed)-4:], crc32.ChecksumIEEE(block)) // checksum of decompressed block

		block = compressed

	default:
		return fmt.Errorf("cannot compress block using unrecognized compression: %d", ocf.compression)
	}

	// create file data block
	buf, _ := longEncoder(nil, len(data)) // block count (number of data items)
	buf, _ = longEncoder(buf, len(block)) // block size (number of bytes in block)
	buf = append(buf, block...)           // serialized objects
	buf = append(buf, ocf.syncMarker...)  // sync marker

	_, err = ocf.iow.Write(buf)
	return err
}

// compactSchema returns the compacted schema for the header file
func compactSchema(schema string) (string, error) {
	// first: decode JSON
	var schemaBlob interface{}
	if err := json.Unmarshal([]byte(schema), &schemaBlob); err != nil {
		return "", fmt.Errorf("cannot unmarshal schema: %v", err)
	}
	// second: re-encode into compressed JSON
	canonicalSchema, err := json.Marshal(schemaBlob)
	if err != nil {
		return "", fmt.Errorf("cannot marshal schema: %s", err)
	}
	return string(canonicalSchema), nil
}

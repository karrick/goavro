package goavro

import "math"

// Compression are values used to specify compression algorithm used to
// compress and decompress Avro Object Container File (OCF) streams.
type Compression uint8

const (
	// CompressionNull is used when OCF blocks are not compressed.
	CompressionNull Compression = iota

	// CompressionDeflate is used when OCF blocks are compressed using the
	// deflate algorithm.
	CompressionDeflate

	// CompressionSnappy is used when OCF blocks are compressed using the snappy
	// algorithm.
	CompressionSnappy
)

const (
	// CompressionNullLabel is used when OCF blocks are not compressed.
	CompressionNullLabel = "null"

	// CompressionDeflateLabel is used when OCF blocks are compressed using the
	// deflate algorithm.
	CompressionDeflateLabel = "deflate"

	// CompressionSnappyLabel is used when OCF blocks are compressed using the
	// snappy algorithm.
	CompressionSnappyLabel = "snappy"
)

const (
	magicString    = "Obj\x01"
	metadataSchema = `{"type":"map","values":"bytes"}`
	syncLength     = 16
)

var (
	// MaxBlockCount is the maximum number of data items allowed in a single
	// binary block that will be decoded from a binary stream. This check is to
	// ensure decoding binary data will not cause the library to over allocate
	// RAM, potentially creating a denial of service on the system.
	//
	// If a particular application needs to decode binary Avro data that
	// potentially has more data items in a single block, then this variable may
	// be modified at your discretion.
	MaxBlockCount = int64(math.MaxInt32)

	// MaxBlockSize is the maximum number of bytes that will be allocated for a
	// single block of data items when decoding from a binary stream. This check
	// is to ensure decoding binary data will not cause the library to over
	// allocate RAM, potentially creating a denial of service on the system.
	//
	// If a particular application needs to decode binary Avro data that
	// potentially has more bytes in a single block, then this variable may be
	// modified at your discretion.
	MaxBlockSize = int64(math.MaxInt32)

	magicBytes    = []byte(magicString)
	metadataCodec *Codec
)

func init() {
	metadataCodec, _ = NewCodec(metadataSchema)
}

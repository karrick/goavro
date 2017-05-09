package goavro

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
	magicBytes     = "Obj\x01"
	metadataSchema = `{"type":"map","values":"bytes"}`
	syncLength     = 16
)

var (
	metadataCodec *Codec
)

func init() {
	metadataCodec, _ = NewCodec(metadataSchema)
}

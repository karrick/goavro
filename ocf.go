package goavro

// Compression are values used to specify compression algorithm used to
// compress and decompress Avro Object Container File (OCF) streams.
type Compression uint8

const (
	CompressionNull    Compression = iota // CompressionNull is used when OCF blocks are not compressed.
	CompressionDeflate                    // CompressionDeflate is used when OCF blocks are compressed using the deflate algorithm.
	CompressionSnappy                     // CompressionSnappy is used when OCF blocks are compressed using the snappy algorithm.
)

const (
	CompressionNullLabel    = "null"
	CompressionDeflateLabel = "deflate"
	CompressionSnappyLabel  = "snappy"
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

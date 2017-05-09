package goavro

import (
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
)

// OCFReader structure is used to read Object Container Files (OCF).
type OCFReader struct {
	schema string
	br     *bufio.Reader
	bd     BinaryDecoder
	datum  interface{}
	err    error

	readReady      bool
	compression    Compression
	remainingItems int64 // initialized to block count for each block, and decremented to 0 by end of block
	block          []byte
	syncMarker     []byte
}

const readBlockSize = 4096 // read and process data by blocks

// NewOCFReader initializes and returns a new structure used to read an Avro Object Container File
// (OCF).
//
//    func example(ior io.Reader) error {
//    	ocfr, err := goavro.NewOCFReader(ior)
//    	if err != nil {
//    		return err
//    	}
//    	for ocfr.Scan() {
//    		datum, err := ocfr.Read()
//    		if err != nil {
//    			return err
//    		}
//    		fmt.Println(datum)
//    	}
//    	return ocfr.Err()
//    }
func NewOCFReader(ior io.Reader) (*OCFReader, error) {
	// NOTE: Wrap provided io.Reader in a buffered reader, which provides
	// io.ByteReader interface, along with improving the performance of
	// streaming file data.
	br := bufio.NewReader(ior)

	// read and verify magic bytes
	magic := make([]byte, 4)
	_, err := io.ReadFull(br, magic)
	if err != nil {
		return nil, fmt.Errorf("cannot read magic bytes: %s", err)
	}
	if bytes.Compare(magic, magicBytes) != 0 {
		return nil, fmt.Errorf("cannot decode OCF with invalid magic bytes: %#q", magic)
	}

	// decode header metadata
	metadata, err := metadataReader(br)
	if err != nil {
		return nil, fmt.Errorf("cannot read metadata header: %s", err)
	}

	// ensure avro.codec valid
	var compression Compression
	value, ok := metadata["avro.codec"]
	// NOTE: If ok is false and "avro.codec" was not included in the metadata header, assumes compression codec is null
	if ok {
		switch avroCodec := string(value); avroCodec {
		case "", CompressionNullLabel:
			// no action, because zero value of CompressionCodec specifies "null" compression
		case CompressionDeflateLabel:
			compression = CompressionDeflate
		case CompressionSnappyLabel:
			compression = CompressionSnappy
		default:
			return nil, fmt.Errorf("cannot decompress using unrecognized compression algorithm from avro.codec: %q", avroCodec)
		}
	}

	// create decoder for avro.schema
	value, ok = metadata["avro.schema"]
	if !ok {
		return nil, errors.New("cannot read without avro.schema")
	}
	bd, err := NewCodec(string(value))
	if err != nil {
		return nil, fmt.Errorf("cannot create codec from invalid avro.schema: %s", err)
	}

	// read and store sync marker
	sm := make([]byte, 16)
	n, err := io.ReadAtLeast(br, sm, 16)
	if err != nil {
		return nil, fmt.Errorf("cannot read sync marker: only read %d bytes: %s", n, err)
	}

	return &OCFReader{br: br, bd: bd, syncMarker: sm, compression: compression, schema: string(value)}, nil
}

// Err returns the last error encountered while reading the OCF file. It does
// not reset the read error.
func (ocfr *OCFReader) Err() error {
	return ocfr.err
}

// Scan returns true when there is at least one more data item to be read from
// the Avro OCF. Scan ought to be called prior to calling the Read method each
// time the Read method is invoked. See the documentation for
// goavro.NewOCFReader for an example of how to use Scan.
func (ocfr *OCFReader) Scan() bool {
	ocfr.readReady = false

	if ocfr.err != nil {
		return false
	}

	// NOTE: If there are no more remaining data items from the existing block,
	// then attempt to slurp in the next block.
	if ocfr.remainingItems <= 0 {
		if len(ocfr.block) > 0 {
			ocfr.err = fmt.Errorf("extra bytes between final datum in previous block and block sync marker: %d", len(ocfr.block))
			return false
		}

		// Read the block count and update the number of remaining items for this block
		ocfr.remainingItems, ocfr.err = longReader(ocfr.br)
		if ocfr.err != nil {
			if ocfr.err == io.EOF {
				ocfr.err = nil // merely end of file, rather than error
			} else {
				ocfr.err = fmt.Errorf("cannot read block count: %s", ocfr.err)
			}
			return false
		}
		if ocfr.remainingItems <= 0 {
			ocfr.err = fmt.Errorf("cannot decode when block count is not greater than 0: %d", ocfr.remainingItems)
			return false
		}
		if ocfr.remainingItems > MaxBlockCount {
			ocfr.err = fmt.Errorf("cannot decode when block count exceeds MaxBlockCount: %d > %d", ocfr.remainingItems, MaxBlockCount)
		}

		var blockSize int64
		blockSize, ocfr.err = longReader(ocfr.br)
		if ocfr.err != nil {
			ocfr.err = fmt.Errorf("cannot read block size: %d; %s", blockSize, ocfr.err)
			return false
		}
		if blockSize <= 0 {
			ocfr.err = fmt.Errorf("cannot decode when block size is not greater than 0: %d", ocfr.remainingItems)
			return false
		}
		if blockSize > MaxBlockSize {
			ocfr.err = fmt.Errorf("cannot decode when block size exceeds MaxBlockSize: %d > %d", blockSize, MaxBlockSize)
			return false
		}

		// read entire block into buffer
		ocfr.block = make([]byte, blockSize)
		_, ocfr.err = io.ReadFull(ocfr.br, ocfr.block)
		if ocfr.err != nil {
			ocfr.err = fmt.Errorf("cannot read block of %d bytes: %s", blockSize, ocfr.err)
			return false
		}

		switch ocfr.compression {
		case CompressionNull:
			//

		case CompressionDeflate:
			// NOTE: flate.NewReader wraps with io.ByteReader if argument does
			// not implement that interface.
			rc := flate.NewReader(bytes.NewBuffer(ocfr.block))
			ocfr.block, ocfr.err = ioutil.ReadAll(rc)
			if ocfr.err != nil {
				_ = rc.Close()
				return false
			}
			if ocfr.err = rc.Close(); ocfr.err != nil {
				return false
			}

		case CompressionSnappy:
			index := len(ocfr.block) - 4 // last 4 bytes is crc32 of decoded block
			if index <= 0 {
				ocfr.err = fmt.Errorf("cannot decompress snappy without CRC32 checksum: %d", len(ocfr.block))
				return false
			}
			decoded, err := snappy.Decode(nil, ocfr.block[:index])
			if err != nil {
				ocfr.err = fmt.Errorf("cannot decompress: %s", err)
				return false
			}
			actualCRC := crc32.ChecksumIEEE(decoded)
			expectedCRC := binary.BigEndian.Uint32(ocfr.block[index : index+4])
			if actualCRC != expectedCRC {
				ocfr.err = fmt.Errorf("snappy CRC32 checksum mismatch: %x != %x", actualCRC, expectedCRC)
				return false
			}
			ocfr.block = decoded

		}

		// read and ensure sync marker matches
		sync := make([]byte, 16)
		var n int
		if n, ocfr.err = io.ReadFull(ocfr.br, sync); ocfr.err != nil {
			ocfr.err = fmt.Errorf("cannot read sync marker: only read %d bytes: %s", n, ocfr.err)
			return false
		}
		if !bytes.Equal(sync, ocfr.syncMarker) {
			ocfr.err = fmt.Errorf("sync marker mismatch: %v != %v", sync, ocfr.syncMarker)
			return false
		}
	}

	ocfr.readReady = true
	return true
}

// Read consumes one data item from the Avro OCF stream and returns it. Read is
// designed to be called only once after each invocation of the Scan method.
// See the documentation for goavro.NewOCFReader for an example of how to use
// Read.
func (ocfr *OCFReader) Read() (interface{}, error) {
	// NOTE: Test previous error before testing readReady to prevent overwriting previous error.
	if ocfr.err != nil {
		return nil, ocfr.err
	}
	if !ocfr.readReady {
		ocfr.err = errors.New("Read called without successful Scan")
		return nil, ocfr.err
	}
	ocfr.readReady = false

	// decode one data item from block
	ocfr.datum, ocfr.block, ocfr.err = ocfr.bd.BinaryDecode(ocfr.block)
	if ocfr.err != nil {
		return false, ocfr.err
	}
	ocfr.remainingItems--

	return ocfr.datum, nil
}

// Schema returns the schema found within the OCF file.
func (ocfr *OCFReader) Schema() string {
	return ocfr.schema
}

// longReader reads bytes from bufio.Reader until has complete long value, or
// read error. It _could_ accept io.ByteReader interface, but receiving the
// exact needed structure is faster.
func longReader(br *bufio.Reader) (int64, error) {
	var value uint64
	var shift uint
	var b byte
	var err error
	for {
		if b, err = br.ReadByte(); err != nil {
			return 0, err // NOTE: must send back unaltered error to detect io.EOF
		}
		value |= uint64(b&intMask) << shift
		if b&intFlag == 0 {
			return (int64(value>>1) ^ -int64(value&1)), nil
		}
		shift += 7
	}
}

// metadataReader reads bytes from bufio.Reader until has entire map value, or
// read error. It _could_ accept io.Reader interface, but receiving the exact
// needed structure is faster.
func metadataReader(br *bufio.Reader) (map[string][]byte, error) {
	var err error
	var value interface{}

	// block count and block size
	if value, err = longReader(br); err != nil {
		return nil, fmt.Errorf("cannot decode Map block count: %s", err)
	}
	blockCount := value.(int64)
	if blockCount < 0 {
		// NOTE: A negative block count implies there is a long encoded
		// block size following the negative block count. We have no use
		// for the block size in this decoder, so we read and discard
		// the value.
		blockCount = -blockCount // convert to its positive equivalent
		if _, err = longReader(br); err != nil {
			return nil, fmt.Errorf("cannot decode Map block size: %s", err)
		}
	}
	// Ensure block count does not exceed some sane value.
	if blockCount > MaxBlockCount {
		return nil, fmt.Errorf("cannot decode Map when block count exceeds MaxBlockCount: %d > %d", blockCount, MaxBlockCount)
	}
	// NOTE: While the attempt of a RAM optimization shown below is not
	// necessary, many encoders will encode all items in a single block.
	// We can optimize amount of RAM allocated by runtime for the array
	// by initializing the array for that number of items.
	mapValues := make(map[string][]byte, blockCount)

	for blockCount != 0 {
		// Decode `blockCount` datum values from buffer
		for i := int64(0); i < blockCount; i++ {
			// first decode the key string
			keyBytes, err := bytesReader(br)
			if err != nil {
				return nil, fmt.Errorf("cannot decode Map key: %s", err)
			}
			key := string(keyBytes)
			// metadata values are always bytes
			buf, err := bytesReader(br)
			if err != nil {
				return nil, fmt.Errorf("cannot decode Map value for key %q: %s", key, err)
			}
			mapValues[key] = buf
		}
		// Decode next blockCount from buffer, because there may be more blocks
		if value, err = longReader(br); err != nil {
			return nil, fmt.Errorf("cannot decode Map block count: %s", err)
		}
		blockCount = value.(int64)
		if blockCount < 0 {
			// NOTE: A negative block count implies there is a long
			// encoded block size following the negative block count. We
			// have no use for the block size in this decoder, so we
			// read and discard the value.
			blockCount = -blockCount // convert to its positive equivalent
			if _, err = longReader(br); err != nil {
				return nil, fmt.Errorf("cannot decode Map block size: %s", err)
			}
		}
		// Ensure block count does not exceed some sane value.
		if blockCount > MaxBlockCount {
			return nil, fmt.Errorf("cannot decode Map when block count exceeds MaxBlockCount: %d > %d", blockCount, MaxBlockCount)
		}
	}
	return mapValues, nil
}

// bytesReader reads bytes from bufio.Reader and returns byte slice of specified
// length or the error encountered while trying to read those bytes. It _could_
// accept io.Reader interface, but receiving the exact needed structure is
// faster.
func bytesReader(br *bufio.Reader) ([]byte, error) {
	size, err := longReader(br)
	if err != nil {
		return nil, fmt.Errorf("cannot read bytes length: %s", err)
	}
	if size < 0 {
		return nil, fmt.Errorf("cannot read bytes: length is negative: %d", size)
	}
	if size > MaxBlockSize {
		return nil, fmt.Errorf("cannot read bytes: length exceeds MaxBlockSize: %d > %d", size, MaxBlockSize)
	}
	buf := make([]byte, size)
	_, err = io.ReadAtLeast(br, buf, int(size))
	if err != nil {
		return nil, fmt.Errorf("cannot read bytes: %s", err)
	}
	return buf, nil
}

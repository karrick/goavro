package goavro

import (
	"io"
	"io/ioutil"
)

// Decoder interface specifies structures that may be decoded.
//
// Deprecated: Use BinaryDecoder instead.
type Decoder interface {
	Decode(io.Reader) (interface{}, error)
}

// Encoder interface specifies structures that may be encoded.
//
// Deprecated: Use BinaryEncoder instead.
type Encoder interface {
	Encode(io.Writer, interface{}) error
}

// v4 is the codec interface from version 4 of the goavro engine.
type v4 interface {
	Decoder
	Encoder
}

// Decode will read and consume from the specified io.Reader until EOF or error, and return the next
// datum from the stream, or an error explaining why the stream cannot be converted into the Codec's
// schema.
//
// WARNING: The previous goavro engine, v4, would not read until EOF, but only read enough bytes to
// decode exactly one datum from the io.Reader.  While there are advantages to this behavior the
// performance penalty of reading byte-by-byte, even through a bytes.Buffer or similar construct,
// are too high.  If the old behavior is desired, then stick to using the previous v4 engine.
//
// Deprecated: Use BinaryDecode instead.
func (c *Codec) Decode(ior io.Reader) (interface{}, error) {
	buf, err := ioutil.ReadAll(ior)
	if err != nil {
		return nil, err
	}
	datum, _, err := c.BinaryDecode(buf)
	// TODO: figure out how to unread buf bytes...
	return datum, err
}

// Encode will write the specified datum to the specified io.Writer,
// or return an error explaining why the datum cannot be converted
// into the Codec's schema.
//
// Deprecated: Use BinaryEncode instead.
func (c *Codec) Encode(iow io.Writer, datum interface{}) error {
	buf, err := c.BinaryEncode(nil, datum)
	if err != nil {
		return err
	}
	_, err = iow.Write(buf)
	return err
}

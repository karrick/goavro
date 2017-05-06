package goavro

import (
	"encoding/hex"
<<<<<<< HEAD
	"errors"
	"fmt"
	"io"
	"unicode"
	"unicode/utf16"
=======
	"fmt"
	"io"
	"strconv"
<<<<<<< HEAD
>>>>>>> Avro bytes text working; Avro string text still needs Unicode help for emojis
=======
	"unicode"
	"unicode/utf16"
>>>>>>> bytes and string text encode and decode pass tests
	"unicode/utf8"
)

////////////////////////////////////////
// Binary Decode
////////////////////////////////////////

func bytesDecoder(buf []byte) (interface{}, []byte, error) {
	if len(buf) < 1 {
		return nil, nil, io.ErrShortBuffer
	}
	var decoded interface{}
	var err error
	if decoded, buf, err = longDecoder(buf); err != nil {
		return nil, buf, fmt.Errorf("bytes: %s", err)
	}
	size := decoded.(int64) // always returns int64
	if size < 0 {
		return nil, buf, fmt.Errorf("bytes: negative size: %d", size)
	}
	if size > int64(len(buf)) {
		return nil, buf, io.ErrShortBuffer
	}
	return buf[:size], buf[size:], nil
}

func stringDecoder(buf []byte) (interface{}, []byte, error) {
	d, b, err := bytesDecoder(buf)
	if err != nil {
		return nil, buf, err
	}
	return string(d.([]byte)), b, nil
}

////////////////////////////////////////
// Binary Encode
////////////////////////////////////////

func bytesEncoder(buf []byte, datum interface{}) ([]byte, error) {
	someBytes, ok := datum.([]byte)
	if !ok {
		return buf, fmt.Errorf("bytes: expected: []byte; received: %T", datum)
	}
	buf, _ = longEncoder(buf, len(someBytes)) // only fails when given non integer
	return append(buf, someBytes...), nil     // append datum bytes
}

func stringEncoder(buf []byte, datum interface{}) ([]byte, error) {
	someBytes, ok := datum.(string)
	if !ok {
		return buf, fmt.Errorf("bytes: expected: string; received: %T", datum)
	}
	buf, _ = longEncoder(buf, len(someBytes)) // only fails when given non integer
	return append(buf, someBytes...), nil     // append datum bytes
}

////////////////////////////////////////
// Text Decode
////////////////////////////////////////

func bytesTextDecoder(buf []byte) (interface{}, []byte, error) {
<<<<<<< HEAD
<<<<<<< HEAD
=======
	// var tmp [utf8.UTFMax]byte
	var tmp [1]byte
=======
>>>>>>> bytes and string text encode and decode pass tests
	// scan each character, being mindful of escape sequence. once find unescaped quote, we're done
>>>>>>> Avro bytes text working; Avro string text still needs Unicode help for emojis
	buflen := len(buf)
	if buflen < 2 {
		return nil, buf, io.ErrShortBuffer
	}
	if buf[0] != '"' {
		return nil, buf, fmt.Errorf("expected initial \"; found: %c", buf[0])
	}
<<<<<<< HEAD
	var newBytes []byte
	var escaped bool
	// Loop through bytes following initial double quote, but note we will
	// return immediately when find unescaped double quote.
	for i := 1; i < buflen; i++ {
		b := buf[i]
		if escaped {
			escaped = false
			if b2, ok := unescapeSpecialJSON(b); ok {
				newBytes = append(newBytes, b2)
				continue
			}
			if b == 'u' {
				// NOTE: Need at least 4 more bytes to read uint16, but subtract
				// 1 because do not want to count the trailing quote and
				// subtract another 1 because already consumed u but have yet to
				// increment i.
				if i > buflen-6 {
					return nil, buf, io.ErrShortBuffer
				}
				// NOTE: Avro bytes represent binary data, and do not
				// necessarily represent text. Therefore, Avro bytes are not
				// encoded in UTF-16. Each \u is followed by 4 hexidecimal
				// digits, the first and second of which must be 0.
				v, err := parseUint64FromHexSlice(buf[i+3 : i+5])
				if err != nil {
					return nil, buf, err
				}
				i += 4 // absorb 4 characters: one 'u' and three of the digits
				newBytes = append(newBytes, byte(v))
				continue
			}
			newBytes = append(newBytes, b)
=======

	var newBytes []byte
	var escaped bool

	// Loop through all remaining bytes, but note we will terminate early when
	// find unescaped double quote. NOTE: Declaring i outside of loop so we can
	// use it after loop completes.
	var i, l int

	for i, l = 1, buflen-1; i < l; i++ {
		b := buf[i]
		if escaped {
			escaped = false
			if b2, ok := unescapeSpecialJSON(b); ok {
				newBytes = append(newBytes, b2)
				continue
			}
			if b == 'u' {
				// NOTE: Need at least 4 more bytes to read uint16, but subtract
				// 1 because do not want to count the trailing quote and
				// subtract another 1 because already consumed u but have yet to
				// increment i.
				if i > buflen-6 {
					return nil, buf, io.ErrShortBuffer
				}
				// NOTE: Avro bytes represent binary data, and do not
				// necessarily represent text. Therefore, Avro bytes are not
				// encoded in UTF-16. Each \u is followed by 4 hexidecimal
				// digits, the first of which must be 0.
				v, err := parseUint64FromHexSlice(buf[i+3 : i+5])
				if err != nil {
					return nil, buf, err
				}
				i += 4 // absorb 4 characters: one 'u' and three of the digits
				newBytes = append(newBytes, byte(v))
				continue
			}
<<<<<<< HEAD
			escaped = false
>>>>>>> Avro bytes text working; Avro string text still needs Unicode help for emojis
=======
			newBytes = append(newBytes, b)
>>>>>>> bytes and string text encode and decode pass tests
			continue
		}
		if b == '\\' {
			escaped = true
			continue
		}
		if b == '"' {
<<<<<<< HEAD
			return newBytes, buf[i+1:], nil
		}
		newBytes = append(newBytes, b)
	}
	return nil, buf, fmt.Errorf("expected final \"; found: %c", buf[buflen-1])
}

func stringTextDecoder(buf []byte) (interface{}, []byte, error) {
=======
			break
		}
		newBytes = append(newBytes, b)
	}
	if b := buf[buflen-1]; b != '"' {
		return nil, buf, fmt.Errorf("expected final \"; found: %c", b)
	}
	return newBytes, buf[i+1:], nil
}

func stringTextDecoder(buf []byte) (interface{}, []byte, error) {
	// scan each character, being mindful of escape sequence. once find unescaped quote, we're done
>>>>>>> Avro bytes text working; Avro string text still needs Unicode help for emojis
	buflen := len(buf)
	if buflen < 2 {
		return nil, buf, io.ErrShortBuffer
	}
	if buf[0] != '"' {
		return nil, buf, fmt.Errorf("expected initial \"; found: %c", buf[0])
	}
<<<<<<< HEAD
	var newBytes []byte
	var escaped bool
	// Loop through bytes following initial double quote, but note we will
	// return immediately when find unescaped double quote.
	for i := 1; i < buflen; i++ {
		b := buf[i]
		if escaped {
			escaped = false
			if b2, ok := unescapeSpecialJSON(b); ok {
				newBytes = append(newBytes, b2)
				continue
			}
			if b == 'u' {
				// NOTE: Need at least 4 more bytes to read uint16, but subtract
				// 1 because do not want to count the trailing quote and
				// subtract another 1 because already consumed u but have yet to
				// increment i.
=======

	var newBytes []byte
	var escaped bool

	// Loop through all remaining bytes, but note we will terminate early when
	// find unescaped double quote. NOTE: Declaring i outside of loop so we can
	// use it after loop completes.
	var i, l int

	for i, l = 1, buflen-1; i < l; i++ {
		b := buf[i]
		if escaped {
<<<<<<< HEAD
			switch b {
			case '"':
				newBytes = append(newBytes, '"')
			case '\\':
				newBytes = append(newBytes, '\\')
			case '/':
				newBytes = append(newBytes, '/')
			case 'b':
				newBytes = append(newBytes, '\b')
			case 'f':
				newBytes = append(newBytes, '\f')
			case 'n':
				newBytes = append(newBytes, '\n')
			case 'r':
				newBytes = append(newBytes, '\r')
			case 't':
				newBytes = append(newBytes, '\t')
			case 'u':
>>>>>>> Avro bytes text working; Avro string text still needs Unicode help for emojis
=======
			escaped = false
			if b2, ok := unescapeSpecialJSON(b); ok {
				newBytes = append(newBytes, b2)
				continue
			}
			if b == 'u' {
				// NOTE: Need at least 4 more bytes to read uint16, but subtract
				// 1 because do not want to count the trailing quote and
				// subtract another 1 because already consumed u but have yet to
				// increment i.
>>>>>>> bytes and string text encode and decode pass tests
				if i > buflen-6 {
					return nil, buf, io.ErrShortBuffer
				}
				v, err := parseUint64FromHexSlice(buf[i+1 : i+5])
				if err != nil {
					return nil, buf, err
				}
<<<<<<< HEAD
<<<<<<< HEAD
				i += 4 // absorb 4 characters: one 'u' and three of the digits

				nbl := len(newBytes)
				newBytes = append(newBytes, []byte{0, 0, 0, 0}...) // grow to make room for UTF-8 encoded rune

				r := rune(v)
				if utf16.IsSurrogate(r) {
					i++ // absorb final hexidecimal digit from previous value

					// Expect second half of surrogate pair
					if i > buflen-6 || buf[i] != '\\' || buf[i+1] != 'u' {
						return nil, buf, errors.New("missing second half of surrogate pair")
					}

					v, err = parseUint64FromHexSlice(buf[i+2 : i+6])
					if err != nil {
						return nil, buf, err
					}
					i += 5 // absorb 5 characters: two for '\u', and 3 of the 4 digits

					// Get code point by combining high and low surrogate bits
					r = utf16.DecodeRune(r, rune(v))
				}

				width := utf8.EncodeRune(newBytes[nbl:], r) // append UTF-8 encoded version of code point
				newBytes = newBytes[:nbl+width]             // trim off excess bytes
				continue
=======
				width := utf8.EncodeRune(tmp[:], rune(v))
				newBytes = append(newBytes, tmp[:width]...)
				i += 4 // absorb 4 hexidecimal characters
			default:
				newBytes = append(newBytes, b)
>>>>>>> Avro bytes text working; Avro string text still needs Unicode help for emojis
=======
				i += 4 // absorb 4 characters: one 'u' and three of the digits

				r1 := rune(v)
				if !utf16.IsSurrogate(r1) {
					// NOTE: decode UTF-16 rune, then encode it back as UTF-8:

					// Get Unicode Code Point from UTF-16
					r1 = utf16.Decode([]uint16{uint16(v)})[0]

					// Get UTF8 from Unicode Code Point.
					// NOTE: All code points which do not require surrogate
					// pairs in UTF-16 will require either 1, 2, or 3 bytes in
					// UTF-8.
					ol := len(newBytes)
					newBytes = append(newBytes, []byte{0, 0, 0, 0}...) // grow to make room for UTF-8 encoded rune
					width := utf8.EncodeRune(newBytes[ol:], r1)        // append UTF-8 encoded version of code point
					newBytes = newBytes[:ol+width]                     // trim off excess bytes
					continue
				}

				i++ // absorb final hexidecimal digit from previous value

				// Expect second half of surrogate pair
				if i > buflen-6 || buf[i] != '\\' || buf[i+1] != 'u' {
					return nil, buf, fmt.Errorf("missing second half of surrogate pair for: \\u%X", v)
				}
				v, err = parseUint64FromHexSlice(buf[i+2 : i+6])
				if err != nil {
					return nil, buf, err
				}
				i += 5 // absorb 5 characters: two for '\u', and 3 of the digits

				r2 := rune(v)
				if !utf16.IsSurrogate(r2) {
					return nil, buf, fmt.Errorf("second half of surrogate pair is invalid: %#U", r2)
				}
				r3 := utf16.DecodeRune(r1, r2)

				// NOTE: All code points requiring surrogate pairs in UTF-16 require 4 bytes in UTF-8.
				ol := len(newBytes)
				newBytes = append(newBytes, []byte{0, 0, 0, 0}...)
				_ = utf8.EncodeRune(newBytes[ol:], r3)
				continue
>>>>>>> bytes and string text encode and decode pass tests
			}
			newBytes = append(newBytes, b)
			continue
		}
		if b == '\\' {
			escaped = true
			continue
		}
		if b == '"' {
<<<<<<< HEAD
			return string(newBytes), buf[i+1:], nil
		}
		newBytes = append(newBytes, b)
	}
	return nil, buf, fmt.Errorf("expected final \"; found: %c", buf[buflen-1])
}

func parseUint64FromHexSlice(buf []byte) (uint64, error) {
	var value uint64
	for _, b := range buf {
		diff := uint64(b - '0')
		if diff < 0 {
			return 0, hex.InvalidByteError(b)
		}
		if diff < 10 {
			value = (value << 4) | diff
			continue
		}
		b10 := b + 10
		diff = uint64(b10 - 'A')
		if diff < 10 {
			return 0, hex.InvalidByteError(b)
		}
		if diff < 16 {
			value = (value << 4) | diff
			continue
		}
		diff = uint64(b10 - 'a')
		if diff < 10 {
			return 0, hex.InvalidByteError(b)
		}
		if diff < 16 {
			value = (value << 4) | diff
			continue
		}
		return 0, hex.InvalidByteError(b)
	}
	return value, nil
}

func unescapeSpecialJSON(b byte) (byte, bool) {
	// NOTE: The following 8 special JSON characters must be escaped:
	switch b {
	case '"', '\\', '/':
		return b, true
	case 'b':
		return '\b', true
	case 'f':
		return '\f', true
	case 'n':
		return '\n', true
	case 'r':
		return '\r', true
	case 't':
		return '\t', true
	}
	return b, false
=======
			break
		}
		newBytes = append(newBytes, b)
	}
	if b := buf[l]; b != '"' {
		return nil, buf, fmt.Errorf("expected final \"; found: %c", b)
	}
	return string(newBytes), buf[i+1:], nil
>>>>>>> Avro bytes text working; Avro string text still needs Unicode help for emojis
}

////////////////////////////////////////
// Text Encode
////////////////////////////////////////

const hexDigits = "0123456789ABCDEF"

// While slices in Go are never constants, we can initialize them once and reuse
// them many times.
var (
	sliceQuote          = []byte("\\\"")
	sliceBackslash      = []byte("\\\\")
	sliceSlash          = []byte("\\/")
	sliceBackspace      = []byte("\\b")
	sliceFormfeed       = []byte("\\f")
	sliceNewline        = []byte("\\n")
	sliceCarriageReturn = []byte("\\r")
	sliceTab            = []byte("\\t")
	sliceUnicode        = []byte("\\u")
)

func bytesTextEncoder(buf []byte, datum interface{}) ([]byte, error) {
	someBytes, ok := datum.([]byte)
	if !ok {
<<<<<<< HEAD
		return buf, fmt.Errorf("bytes: expected: []byte; received: %T", datum)
	}
	buf = append(buf, '"') // prefix buffer with double quote
	for _, b := range someBytes {
		if escaped, ok := escapeSpecialJSON(b); ok {
			buf = append(buf, escaped...)
			continue
		}
		if r := rune(b); r < utf8.RuneSelf && unicode.IsPrint(r) {
			buf = append(buf, b)
			continue
=======
		// panic("string rather than []byte")
		return buf, fmt.Errorf("bytes: expected: []byte; received: %T", datum)
	}
	buf = append(buf, '"') // prefix buffer with double quote
	for _, b := range someBytes {
<<<<<<< HEAD
		buf = appendMaybeEscapedByte(buf, b)
	}
	return append(buf, '"'), nil
}

func appendMaybeEscapedByte(buf []byte, b byte) []byte {
	if b < utf8.RuneSelf {
		// NOTE: The following 6 special JSON characters must be escaped:
		switch b {
		case '"', '\\', '/':
			return append(buf, []byte{'\\', b}...)
		case '\b':
			return append(buf, []byte("\\b")...)
		case '\f':
			return append(buf, []byte("\\f")...)
		case '\n':
			return append(buf, []byte("\\n")...)
		case '\r':
			return append(buf, []byte("\\r")...)
		case '\t':
			return append(buf, []byte("\\t")...)
		default:
			return append(buf, b) // typical characters
>>>>>>> Avro bytes text working; Avro string text still needs Unicode help for emojis
		}
		// This Code Point _could_ be encoded as a single byte, however, it's
		// above standard ASCII range (b > 127), therefore must encode using its
		// four-byte hexidecimal equivalent, which will always start with the high byte 00
		buf = appendUnicodeHex(buf, uint16(b))
	}
<<<<<<< HEAD
	return append(buf, '"'), nil // postfix buffer with double quote
=======
	return append(append(append(buf, []byte("\\u00")...), hexDigits[b>>4]), hexDigits[b&0x0f])
>>>>>>> Avro bytes text working; Avro string text still needs Unicode help for emojis
=======
		if escaped, ok := escapeSpecialJSON(b); ok {
			buf = append(buf, escaped...)
			continue
		}
		if r := rune(b); r < utf8.RuneSelf && unicode.IsPrint(r) {
			buf = append(buf, b)
			continue
		}
		// This Code Point _could_ be encoded as a single byte, however, it's
		// above standard ASCII range (b > 127), therefore must encode using its
		// four-byte hexidecimal equivalent, which will always start with the high byte 00
		buf = append(append(append(buf, []byte("\\u00")...), hexDigits[b>>4]), hexDigits[b&0x0f])
	}
	return append(buf, '"'), nil // postfix buffer with double quote
>>>>>>> bytes and string text encode and decode pass tests
}

func stringTextEncoder(buf []byte, datum interface{}) ([]byte, error) {
	someString, ok := datum.(string)
	if !ok {
<<<<<<< HEAD
		return buf, fmt.Errorf("bytes: expected: string; received: %T", datum)
	}
	buf = append(buf, '"') // prefix buffer with double quote
	for _, r := range someString {
		if escaped, ok := escapeSpecialJSON(byte(r)); ok {
			buf = append(buf, escaped...)
			continue
		}
		if r < utf8.RuneSelf && unicode.IsPrint(r) {
			buf = append(buf, byte(r))
			continue
		}
		// NOTE: Attempt to encode code point as UTF-16 surrogate pair
		r1, r2 := utf16.EncodeRune(r)
		if r1 != unicode.ReplacementChar || r2 != unicode.ReplacementChar {
			// code point does require surrogate pair, and thus two uint16 values
			buf = appendUnicodeHex(buf, uint16(r1))
			buf = appendUnicodeHex(buf, uint16(r2))
			continue
		}
		// Code Point does not require surrogate pair.
		buf = appendUnicodeHex(buf, uint16(r))
	}
	return append(buf, '"'), nil // postfix buffer with double quote
=======
		// panic("[]byte rather than string")
		return buf, fmt.Errorf("bytes: expected: string; received: %T", datum)
	}
	buf = append(buf, '"') // prefix buffer with double quote
	for _, r := range someString {
		if escaped, ok := escapeSpecialJSON(byte(r)); ok {
			buf = append(buf, escaped...)
			continue
		}
		if r < utf8.RuneSelf && unicode.IsPrint(r) {
			buf = append(buf, byte(r))
			continue
		}
		// NOTE: Attempt to encode code point as UTF-16 surrogate pair
		r1, r2 := utf16.EncodeRune(r)
		if r1 != '\ufffd' || r2 != '\ufffd' {
			// code point does require surrogate pair, and thus two uint16 values
			buf = append(buf, []byte(fmt.Sprintf("\\u%04X\\u%04X", r1, r2))...)
			continue
		}
		// code point does not require surrogate pair, so single uint16 will do
		// want 4 nibbles
		buf = append(buf, sliceUnicode...)
		if r < 0x1000 {
			buf = append(buf, '0')
			if r < 0x100 {
				buf = append(buf, '0')
				if r < 0x10 {
					buf = append(buf, '0')
				}
			}
		}
		buf = strconv.AppendInt(buf, int64(r), 16)
	}
	return append(buf, '"'), nil // postfix buffer with double quote
}

func escapeSpecialJSON(b byte) ([]byte, bool) {
	// NOTE: The following 8 special JSON characters must be escaped:
	switch b {
	case '"':
		return sliceQuote, true
	case '\\':
		return sliceBackslash, true
	case '/':
		return sliceSlash, true
	case '\b':
		return sliceBackspace, true
	case '\f':
		return sliceFormfeed, true
	case '\n':
		return sliceNewline, true
	case '\r':
		return sliceCarriageReturn, true
	case '\t':
		return sliceTab, true
	}
	return nil, false
}

func unescapeSpecialJSON(b byte) (byte, bool) {
	// NOTE: The following 8 special JSON characters must be escaped:
	switch b {
	case '"', '\\', '/':
		return b, true
	case 'b':
		return '\b', true
	case 'f':
		return '\f', true
	case 'n':
		return '\n', true
	case 'r':
		return '\r', true
	case 't':
		return '\t', true
	}
	return b, false
}

func parseUint64FromHexSlice(buf []byte) (uint64, error) {
	var value uint64
	for _, b := range buf {
		diff := uint64(b - '0')
		if diff < 0 {
			return 0, hex.InvalidByteError(b)
		}
		if diff < 10 {
			value = (value << 4) | diff
			continue
		}
		b10 := b + 10
		diff = uint64(b10 - 'A')
		if diff < 10 {
			return 0, hex.InvalidByteError(b)
		}
		if diff < 16 {
			value = (value << 4) | diff
			continue
		}
		diff = uint64(b10 - 'a')
		if diff < 10 {
			return 0, hex.InvalidByteError(b)
		}
		if diff < 16 {
			value = (value << 4) | diff
			continue
		}
		return 0, hex.InvalidByteError(b)
	}
<<<<<<< HEAD
	return strconv.AppendInt(append(append(buf, []byte("\\u")...)), int64(r), 16)
>>>>>>> Avro bytes text working; Avro string text still needs Unicode help for emojis
}

func appendUnicodeHex(buf []byte, v uint16) []byte {
	// Start with '\u' prefix:
	buf = append(buf, sliceUnicode...)
	// And tack on 4 hexidecimal digits:
	buf = append(buf, hexDigits[(v&0xF000)>>12])
	buf = append(buf, hexDigits[(v&0xF00)>>8])
	buf = append(buf, hexDigits[(v&0xF0)>>4])
	buf = append(buf, hexDigits[(v&0xF)])
	return buf
=======
	return value, nil
>>>>>>> bytes and string text encode and decode pass tests
}

const hexDigits = "0123456789ABCDEF"

func escapeSpecialJSON(b byte) ([]byte, bool) {
	// NOTE: The following 8 special JSON characters must be escaped:
	switch b {
	case '"':
		return sliceQuote, true
	case '\\':
		return sliceBackslash, true
	case '/':
		return sliceSlash, true
	case '\b':
		return sliceBackspace, true
	case '\f':
		return sliceFormfeed, true
	case '\n':
		return sliceNewline, true
	case '\r':
		return sliceCarriageReturn, true
	case '\t':
		return sliceTab, true
	}
	return nil, false
}

// While slices in Go are never constants, we can initialize them once and reuse
// them many times. We define these slices at library load time and reuse them
// when encoding JSON.
var (
	sliceQuote          = []byte("\\\"")
	sliceBackslash      = []byte("\\\\")
	sliceSlash          = []byte("\\/")
	sliceBackspace      = []byte("\\b")
	sliceFormfeed       = []byte("\\f")
	sliceNewline        = []byte("\\n")
	sliceCarriageReturn = []byte("\\r")
	sliceTab            = []byte("\\t")
	sliceUnicode        = []byte("\\u")
)

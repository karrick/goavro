# goavro

## Description

Goavro is a library written in Go that supports translating binary and
textual Avro data to Go native data types, and conversely translating
Go native data types to binary or textual Avro data. It encodes by
appending to an existing or empty Go byte slice, and decodes by
consuming bytes from an existing Go byte slice.

A goavro `Codec` is created as a stateless structure that can be
safely used in multiple go routines simultaneously.

With the exeption of features not yet supported, goavro attempts to be
fully compliant with the most recent version of the
[Avro specification](http://avro.apache.org/docs/1.8.1/spec.html).

## Resources

* [Avro CLI Examples](https://github.com/miguno/avro-cli-examples)
* [Avro](http://avro.apache.org/)
* [Google Snappy](https://code.google.com/p/snappy/)
* [JavaScript Object Notation, JSON](http://www.json.org/)

## Contrast With Code Generation Tools

If you have the ability to rebuild and redeploy your software whenever
data schemas change, code generation tools might be the best solution
for your application.

There are numerous excellent tools for generating source code to
translate data between native and Avro binary or textual data. One
such tool is linkedin below. If a particular application is designed
to work with a rarely changing schema, programs that use code
generated functions can potentially be more performant than a program
that uses goavro to create a `Codec` at run time.

* [gogen-avro](https://github.com/alanctgardner/gogen-avro)

I recommend benchmarking the resultant programs using typical data
using both the code generated functions and using goavro to see which
performs better. Not all code generated functions will out perform
goavro for all data corpuses.

If you don't have the ability to rebuild and redeploy software updates
whenever a data schema change occurs, goavro could be a great fit for
your needs. With goavro at runtime your program can be given a new
schema, compile it into a `Codec`, and immediately start encoding or
decoding data using that `Codec`. Because Avro encoding specifies that
encoded data always be accompanied by a schema this is not usually a
problem. If the schema change is backwards compatible, and the portion
of your program that handles the decoded data is still able to
reference the decoded fields, there is nothing that needs to be done
when the schema change is detected by your program when using goavro
`Codec` instances to encode or decode data.

## Usage

Documentation is available via
[![GoDoc](https://godoc.org/github.com/karrick/goavro?status.svg)](https://godoc.org/github.com/karrick/goavro).

```Go
package main

import (
    "fmt"

    "github.com/karrick/goavro"
)

func main() {
    codec, err := goavro.NewCodec(`
        {
          "type": "record",
          "name": "LongList",
          "fields" : [
            {"name": "next", "type": ["null", "LongList"], "default": null}
          ]
        }`)
    if err != nil {
        fmt.Println(err)
    }

    // NOTE: May omit fields when using default value
    textual := []byte(`{"next":{"LongList":{}}}`)

    // Convert textual Avro data (in Avro JSON format) to native Go form
    native, _, err := codec.NativeFromTextual(textual)
    if err != nil {
        fmt.Println(err)
    }

    // Convert native Go form to binary Avro data
    binary, err := codec.BinaryFromNative(nil, native)
    if err != nil {
        fmt.Println(err)
    }

    // Convert binary Avro data back to native Go form
    native, _, err = codec.NativeFromBinary(binary)
    if err != nil {
        fmt.Println(err)
    }

    // Convert native Go form to textual Avro data
    textual, err = codec.TextualFromNative(nil, native)
    if err != nil {
        fmt.Println(err)
    }

    // NOTE: Textual encoding will show all fields, even those with values that
    // match their default values
    fmt.Println(string(textual))
    // Output: {"next":{"LongList":{"next":null}}}
}
```

Also please see the example programs in the `examples` directory for
reference. The `ab2t` program is similar to the reference standard
`avrocat` program and converts Avro OCF files to Avro JSON
encoding. The Avro-ReWrite program, `arw`, can be used to rewrite an
Avro OCF file while optionally changing the block counts, the
compression algorithm. `arw` can also upgrade the schema provided the
existing datum values can be encoded with the newly provided schema.

### Translating Data

A `Codec` provides four methods for translating between a byte slice
of either binary or textual Avro data and native Go data.

The following methods convert data between native Go data and byte
slices of the binary Avro representation:

    BinaryFromNative
    NativeFromBinary

The following methods convert data between native Go data and byte
slices of the textual Avro representation:

    NativeFromTextual
    TextualFromNative

Each `Codec` also exposes the `Schema` method to return a simplified
version of the JSON schema string used to create the `Codec`.

#### Translating From Avro to Go Data

Goavro does not use Go's structure tags to translate data between
native Go types and Avro encoded data.

When translating from either binary or textual Avro to native Go data,
goavro returns primitive Go data values for corresponding Avro data
values. That is, a Go `nil` is returned for an Avro `null`; a Go
`bool` for an Avro `boolean`; a Go `[]byte` for an Avro `bytes`; a Go
`float32` for an Avro `float`, a Go `float64` for an Avro `double`; a
Go `int64` for an Avro `long`; a Go `int32` for an Avro `int`; and a
Go `string` for an Avro `string`.

For complex Avro data types, a Go `[]interface{}` is returned for an
Avro `array`; a Go `string` for an Avro `enum`; a Go `[]byte` for an
Avro `fixed`; a Go `map[string]interface{}` for an Avro `map` and
`record`.

Because of encoding rules for Avro unions, when an union's value is
`null`, a simple Go `nil` is returned. However when an union's value
is non-`nil`, a Go `map[string]interface{}` with a single key is
returned for the union. The map's single key is the Avro type name and
its value is the datum's value.

#### Translating From Go to Avro Data

Goavro does not use Go's structure tags to translate data between
native Go types and Avro encoded data.

When translating from native Go to either binary or textual Avro data,
goavro generally requires the same native Go data types as the decoder
would provide, with some exceptions for programmer convenience. Goavro
will accept any numerical data type provided there is no precision
lost when encoding the value. For instance, providing `float64(3.0)`
to an encoder expecting an Avro `int` would succeed, while sending
`float64(3.5)` to the same encoder would return an error.

When providing a slice of items for an encoder, the encoder will
accept either `[]interface{}`, or any slice of the required type. For
instance, when the Avro schema specifies:
`{"type":"array","items":"string"}`, the encoder will accept either
`[]interface{}`, or `[]string`. If given `[]int`, the encoder will
return an error when it attempts to encode the first non-string array
value using the string encoder.

When providing a value for an Avro union, the encoder will accept
`nil` for a `null` value. If the value is non-`nil`, it must be a
`map[string]interface{}` with a single key-value pair, where the key
is the Avro type name and the value is the datum's value. As a
convenience, the `Union` function wraps any datum value in a map as
specified above.

```Go
func ExampleUnion() {
    codec, err := goavro.NewCodec(`["null","string","int"]`)
    if err != nil {
        fmt.Println(err)
    }
    buf, err := codec.TextFromNative(nil, goavro.Union("string", "some string"))
    if err != nil {
        fmt.Println(err)
    }
    fmt.Println(string(buf))
    // Output: {"string":"some string"}
}
```

## Implementation Notes

### API

In general it is poor form to define a library API which shares the
same function or method names but provides a different method
signature to an accepted standard. Go has particular strong emphasis
on what a Reader and Writer are, and they conflict with what the Avro
specification describes as a reader and a writer. Thus goavro shys
away from using the terms _reader_ and _writer_ as most Avro tools and
libraries would normally use.

In Go, an `io.Reader` reads data from the stream specified at object
instantiation time into a preallocated slice of bytes and returns both
the number of bytes read along with an error. In the Avro
specification, a reader is a function that decodes Avro data and
returns data in native form.

A Go `io.Writer` writes bytes from a slice of bytes to a stream
specified at its instantiation time and returns the number of bytes
written along with an error. In the Avro specification, a writer is a
function that encodes data from native form to either binary or text
Avro bytes.

### Record Field Default Values

The Avro specification allows for providing default values for each
Avro Record field. The default value is to be used when reading
instances that lack the respective field.

When reading binary Avro data, a Record is decoded by reading bytes
for the first Record field, immediately followed by the second Record
field, and so on. No fields may be skipped in a Record's binary
encoding, so a default value is deemed unusable. If this assessment is
wrong, please open a Bug, and provide one or more suitable examples,
and the developers will be happy to revisit the issue.

When decoding from textual Avro data that is missing a particular
record field name, if the record field has a default value, it will be
used in place of the missing value.

When encoding from native Go data that is missing a particular record
field name, if the record field has a default value, it will be used
in place of the missing value.

## Limitations

With the exeption of features not yet supported, goavro attempts to be
fully compliant with the most recent version of the
[Avro specification](http://avro.apache.org/docs/1.8.1/spec.html). The
following limitations may change as future releases of goavro may
include support for some of these features.

### Aliases

The Avro specification allows an implementation to optionally map a
writer's schema to a reader's schema using aliases. Although goavro
can compile schemas with aliases, it does not implement this feature.

### Canonicalization of Schemas

The Avro specification describes the process by which schemas are
canonlicalized. Goavro does not canonicalize schema strings when
creating a `Codec`, although it does eliminate extra whitespace.

### Default maximum block count and block size

To prevent over allocation of memory when decoding Avro arrays, bytes,
maps, strings, and OCF data, goavro returns an error whenever a block
count exceeds `MaxBlockCount`, or a block size exceeds
`MaxBlockSize`. Both of these tokens are set to `math.MaxInt32`, or
~2.2 GiB, but are declared as variables so a user can change the limit
if deemed necessary.

### Kafka Streams

[Kafka](http://kafka.apache.org) is the reason goavro was
written. Similar to Avro Object Container Files being a layer of
abstraction above Avro Data Serialization format, Kafka's use of Avro
is a layer of abstraction that also sits above Avro Data Serialization
format, but has its own schema. Goavro itself is not a Kafka
library. Goavro coupled with a Kafka library is used everyday to
process hundreds of billions of datum values everyday where goavro was
developed.

### Logical Types

Goavro does not implement Logical Types as required by the Avro
specification.

### RPC Support

Goavro does not implement any high level RPC mechanics required by the
Avro specification. Avro protocol declarations, messages, message
transports, message framing, handshakes, and call format are all
unsupported by this library.

### Record Field Aliases

The Avro specification allows for providing a JSON array of strings as
alternate names for a Record field. While goavro can create `Codec`
instances that specify `aliases`, that list is ignored.

### Record Field Order

The Avro specification allows for providing a sory order string,
either `ascending`, `descending`, or `ignore`, for use when sorting
records. While goavro can create `Codec` instances that specify
`order`, those values are not used.

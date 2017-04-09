GC

Your application is presented with a slice of bytes, which presumably
decodes and reifies into some data structure.

With goavro, you create a codec from the provided schema, and then
throw it an io.Reader and tell it to decode the bytes and reify
objects that correspond to the hierarchical data structure represented
by that byte slice.

In the process, you potentially create lots of objects, each
allocating memory, and then maybe your program pulls one or two values
out and then throws away the entire hierarchical data structure.

That's okay: that's what the garbage collector is for, right?  Yes,
except, when your program churns through hundreds of thousands of
these top level data structures each second, just to throw the data
away, get collected.  Repeat.  This behavior taxes even the most
advanced garbage collection engines.

There's got to be a better way.

...

Users probably do not care about the middle data layers, but the leaf
nodes.  In other words, perhaps I know that "some/engineers"
represents an array of engineer records, and I'd like to enumerate
over them, without reifying the records themselves, but be able to
pull out the fields from the records in a loop.

```Go
codec.ForEach("some/engineers", func(engineer) {
    // engineer is not a record, but internally some sort of reference pointer to
    // where this engineer record is stored in the byte slice, and the byte slice is
    // sized to the extent of the engineer record, if possible.
    fmt.Printf("name: %q\n", codec.Get(engineer, "nameField"))
})
```

* expose some object that has methods to dig deeper.

```Go
engineers, err := topLevel.Find(buf, "some/engineers")
```

Here, engineers represents a tuple of the schema at that point, and
the byte slice that is encoded by that schema.

* objects that are collections provide iterators.

codec.Find("some/engineers", func(list) {
    // 
})
```

Namespaces. 

all hierarchies have a global namespace pool. parsing a layer memoizes
the byte slice starts and ends in a cache for future lookups. starts
definitely, and ends when discovered, but not right away or until
needed.

offsets are lazily discovered when needed while trying to find some
portion of the data.

NAME:
Start: [A-Za-z_]
Continue: [A-Za-z0-9_]

FULLNAME:
dot delimited names

So, we could use \[\d+\] to represent array index for annotations.

a codec cache has a map of string names to codecs, and is used to track how
to encode or decode a schema.  it is not mutated after codec created.

an annotations cache has a map of string names to where a particular value is
stored in a byte slice. it may also have end offset if discovered.

an annotation cache is coupled with the byte slice, starts off with starting
offset of 0, and that's it.  it is continually updated with new annotations
when object locations are discovered while traversing byte slice looking for
a particular node.

when codec is bound to byte slice returns some object (name?) which has an
annotations map.

would be nice to give it a Golang data blob, and have encoder encode
that data on the fly based on schema.

    codec.WriteTo(w, map[string]map[string]int{})

this might not work because would be ambiguous in unions that can have
either a map, or a record. or a union which could have one of several
record types, differing only by the qualified record name.  one way is
have map that says:

    {"type": "record", "name": "bar", "value": {"field1": "val1"}}

that would allow client code to pivot on value of "type" key, just
like the codec compiler would.

this is starting to look like the JSON encoding of avro objects...

    {"Foo": {"name": "jane", "age": 42}}

would also be nice to give codec a reader and have it decode into native
Golang data on the fly:

    foo, err := codec.ReadFrom(r)

Admittedly these also reify lots of objects, but interface might be more easy
to use than previous goavro.  Basically, create maps and slices to represent
information in Golang terms.  And use native Golang data types, cast into the
required data type for the schema and write to io.Writer.

The encoding must accept a map that 1:1 corresponds to the JSON for
the JSON encoded version of the data.  In other words, if this library
supported JSON encoding, it would emit something that looks remarkably
like the map that created it.

That's one layer.  Then there's the non-reification version.

Just like `jQuery` uses selectors to return objects that have methods
to access data on leaf nodes, and enumerate various types of
collections on non-leaf nodes, we need to expose a few interfaces that
allow easy node access and traversal.


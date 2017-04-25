package goavro

import "fmt"

// symtab represents a set of Avro full names to the Codec that processes that schema type.
type symtab struct {
	cache map[string]*codec
}

// newSymbolTable returns a new symbol table initialized with codecs for primitive data types.
func newSymbolTable() *symtab {
	return &symtab{cache: map[string]*codec{
		"boolean": &codec{name: &Name{"boolean", nullNamespace}, binaryDecoder: booleanDecoder, binaryEncoder: booleanEncoder},
		"bytes":   &codec{name: &Name{"bytes", nullNamespace}, binaryDecoder: bytesDecoder, binaryEncoder: bytesEncoder},
		"double":  &codec{name: &Name{"double", nullNamespace}, binaryDecoder: doubleDecoder, binaryEncoder: doubleEncoder},
		"float":   &codec{name: &Name{"float", nullNamespace}, binaryDecoder: floatDecoder, binaryEncoder: floatEncoder},
		"int":     &codec{name: &Name{"int", nullNamespace}, binaryDecoder: intDecoder, binaryEncoder: intEncoder},
		"long":    &codec{name: &Name{"long", nullNamespace}, binaryDecoder: longDecoder, binaryEncoder: longEncoder},
		"null":    &codec{name: &Name{"null", nullNamespace}, binaryDecoder: nullDecoder, binaryEncoder: nullEncoder},
		"string":  &codec{name: &Name{"string", nullNamespace}, binaryDecoder: stringDecoder, binaryEncoder: stringEncoder},
	}}
}

// convert a schema data structure to a codec, prefixing with specified namespace
func (st symtab) buildCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	switch schemaType := schema.(type) {
	case map[string]interface{}:
		return st.buildCodecForTypeDescribedByMap(enclosingNamespace, schemaType)
	case string:
		return st.buildCodecForTypeDescribedByString(enclosingNamespace, schemaType, schema)
	case []interface{}:
		return st.buildCodecForTypeDescribedBySlice(enclosingNamespace, schemaType)
	default:
		return nil, fmt.Errorf("cannot build codec: unknown schema type: %T", schema)
	}
}

// Reach into the map, grabbing its "type".  Use that to create the codec.
func (st symtab) buildCodecForTypeDescribedByMap(enclosingNamespace string, schemaMap map[string]interface{}) (*codec, error) {
	// INPUT: {"type":"boolean", ...}
	t, ok := schemaMap["type"]
	if !ok {
		return nil, fmt.Errorf("cannot build codec: missing type: %v", schemaMap)
	}
	switch v := t.(type) {
	case map[string]interface{}:
		return st.buildCodecForTypeDescribedByMap(enclosingNamespace, v)
	case string:
		// Already defined types may be abbreviated with its string name.
		// EXAMPLE: "type":"int"
		// EXAMPLE: "type":"enum"
		// EXAMPLE: "type":"somePreviouslyDefinedCustomTypeString"
		return st.buildCodecForTypeDescribedByString(enclosingNamespace, v, schemaMap)
	case []interface{}:
		return st.buildCodecForTypeDescribedBySlice(enclosingNamespace, v)
	default:
		return nil, fmt.Errorf("cannot build codec: type ought to be either string, map[string]interface{}, or []interface{}; received: %T", t)
	}
}

func (st symtab) buildCodecForTypeDescribedByString(enclosingNamespace string, schemaType string, schema interface{}) (*codec, error) {
	// when codec already exists, return it
	if cd, ok := st.cache[schemaType]; ok {
		return cd, nil
	}
	switch schemaType {
	case "array":
		return st.makeArrayCodec(enclosingNamespace, schema)
	case "map":
		return st.makeMapCodec(enclosingNamespace, schema)
	case "enum":
		return st.makeEnumCodec(enclosingNamespace, schema)
	case "fixed":
		return st.makeFixedCodec(enclosingNamespace, schema)
	case "record":
		return st.makeRecordCodec(enclosingNamespace, schema)
	default:
		return nil, fmt.Errorf("cannot build codec for unknown schema type: %s", schemaType)
	}
}

// notion of enclosing namespace changes when record, enum, or fixed create a new namespace, for child objects.
func (st symtab) registerCodec(c *codec, schemaMap map[string]interface{}, enclosingNamespace string) error {
	n, err := newNameFromSchemaMap(enclosingNamespace, schemaMap)
	if err != nil {
		return err
	}
	st.cache[n.FullName] = c
	c.name = n // reach back into codec and set its name
	return nil
}

func (st symtab) typeNames() []string {
	var keys []string
	for k := range st.cache {
		keys = append(keys, k)
	}
	return keys
}

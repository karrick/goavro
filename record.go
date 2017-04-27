package goavro

import (
	"fmt"
)

func makeRecordCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	// NOTE: To support recursive data types, create the codec and register it using the specified
	// name, and fill in the codec functions later.
	c, err := registerNewCodec(st, schemaMap, enclosingNamespace)
	if err != nil {
		return nil, fmt.Errorf("Record ought to have valid name: %s", err)
	}

	fields, ok := schemaMap["fields"]
	if !ok {
		return nil, fmt.Errorf("Record %q ought to have fields key", c.typeName)
	}
	fieldSchemas, ok := fields.([]interface{})
	if !ok || len(fieldSchemas) == 0 {
		return nil, fmt.Errorf("Record %q fields ought to be non-empty array: %v", c.typeName, fields)
	}

	fieldCodecs := make([]*Codec, len(fieldSchemas))
	fieldNames := make([]string, len(fieldSchemas))
	fieldNameDuplicateCheck := make(map[string]struct{})
	for i, fieldSchema := range fieldSchemas {
		fieldSchemaMap, ok := fieldSchema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Record %q field %d ought to be valid Avro named type; received: %v", c.typeName, i+1, fieldSchema)
		}

		// NOTE: field names are not registered in the symbol table, because field names are not
		// individually addressable codecs.

		// fmt.Printf("%q field: %d; fieldSchemaMap: %v\n", recordName, i+1, fieldSchemaMap)
		// fieldCodec, err := buildCodecForTypeDescribedByMap(st, nullNamespace, fieldSchemaMap)
		fieldCodec, err := buildCodecForTypeDescribedByMap(st, c.typeName.namespace, fieldSchemaMap)
		if err != nil {
			return nil, fmt.Errorf("Record %q field %d ought to be valid Avro named type: %s", c.typeName, i+1, err)
		}
		// However, when creating a full name for the field name, be sure to use record's namespace
		n, err := newNameFromSchemaMap(c.typeName.namespace, fieldSchemaMap)
		if err != nil {
			return nil, fmt.Errorf("Record %q field %d ought to have valid name: %v", c.typeName, i+1, fieldSchemaMap)
		}
		fieldName := n.short()
		if _, ok := fieldNameDuplicateCheck[fieldName]; ok {
			return nil, fmt.Errorf("Record %q field %d ought to have unique name: %q", c.typeName, i+1, fieldName)
		}
		fieldNameDuplicateCheck[fieldName] = struct{}{}
		fieldNames[i] = fieldName

		fieldCodecs[i] = fieldCodec
	}

	c.binaryDecoder = func(buf []byte) (interface{}, []byte, error) {
		recordMap := make(map[string]interface{}, len(fieldCodecs))
		for i, fieldCodec := range fieldCodecs {
			var value interface{}
			var err error
			value, buf, err = fieldCodec.binaryDecoder(buf)
			if err != nil {
				return nil, buf, err
			}
			recordMap[fieldNames[i]] = value
		}
		return recordMap, buf, nil
	}
	c.binaryEncoder = func(buf []byte, datum interface{}) ([]byte, error) {
		valueMap, ok := datum.(map[string]interface{})
		if !ok {
			return buf, fmt.Errorf("Record %q value ought to be map[string]interface{}; received: %T", c.typeName, datum)
		}
		if actual, expected := len(valueMap), len(fieldCodecs); actual != expected {
			return buf, fmt.Errorf("Record %q value field count ought to equal number of schema fields: %d != %d", c.typeName, actual, expected)
		}
		// records encoded in order fields were defined in schema
		for i, fieldCodec := range fieldCodecs {
			fieldName := fieldNames[i]
			fieldValue, ok := valueMap[fieldName]
			if !ok {
				return buf, fmt.Errorf("Record %q value ought to have keys for all schema field names; missing: %q", c.typeName, fieldName)
			}
			var err error
			buf, err = fieldCodec.binaryEncoder(buf, fieldValue)
			if err != nil {
				return buf, fmt.Errorf("Record %q field value does not match its schema: %s", c.typeName, err)
			}
		}
		return buf, nil
	}

	return c, nil
}

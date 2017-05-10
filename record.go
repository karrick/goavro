package goavro

import (
	"fmt"
)

func makeRecordCodec(st map[string]*Codec, enclosingNamespace string, schemaMap map[string]interface{}) (*Codec, error) {
	// NOTE: To support recursive data types, create the codec and register it
	// using the specified name, and fill in the codec functions later.
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

	codecFromFieldName := make(map[string]*Codec)
	codecFromIndex := make([]*Codec, len(fieldSchemas))
	nameFromIndex := make([]string, len(fieldSchemas))

	for i, fieldSchema := range fieldSchemas {
		fieldSchemaMap, ok := fieldSchema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Record %q field %d ought to be valid Avro named type; received: %v", c.typeName, i+1, fieldSchema)
		}

		// NOTE: field names are not registered in the symbol table, because
		// field names are not individually addressable codecs.

		fieldCodec, err := buildCodecForTypeDescribedByMap(st, c.typeName.namespace, fieldSchemaMap)
		if err != nil {
			return nil, fmt.Errorf("Record %q field %d ought to be valid Avro named type: %s", c.typeName, i+1, err)
		}
		// However, when creating a full name for the field name, be sure to use
		// record's namespace
		n, err := newNameFromSchemaMap(c.typeName.namespace, fieldSchemaMap)
		if err != nil {
			return nil, fmt.Errorf("Record %q field %d ought to have valid name: %v", c.typeName, i+1, fieldSchemaMap)
		}
		fieldName := n.short()
		if _, ok := codecFromFieldName[fieldName]; ok {
			return nil, fmt.Errorf("Record %q field %d ought to have unique name: %q", c.typeName, i+1, fieldName)
		}
		nameFromIndex[i] = fieldName
		codecFromIndex[i] = fieldCodec
		codecFromFieldName[fieldName] = fieldCodec
	}

	c.binaryDecoder = func(buf []byte) (interface{}, []byte, error) {
		recordMap := make(map[string]interface{}, len(codecFromIndex))
		for i, fieldCodec := range codecFromIndex {
			var value interface{}
			var err error
			value, buf, err = fieldCodec.binaryDecoder(buf)
			if err != nil {
				return nil, buf, err
			}
			recordMap[nameFromIndex[i]] = value
		}
		return recordMap, buf, nil
	}

	c.binaryEncoder = func(buf []byte, datum interface{}) ([]byte, error) {
		valueMap, ok := datum.(map[string]interface{})
		if !ok {
			return buf, fmt.Errorf("Record %q value ought to be map[string]interface{}; received: %T", c.typeName, datum)
		}

		// records encoded in order fields were defined in schema
		for i, fieldCodec := range codecFromIndex {
			fieldName := nameFromIndex[i]

			// NOTE: If field value was not specified in map, then attempt to
			// encode the nil
			fieldValue, ok := valueMap[fieldName]

			var err error
			buf, err = fieldCodec.binaryEncoder(buf, fieldValue)
			if err != nil {
				if !ok {
					return buf, fmt.Errorf("Record %q field value for %q was not specified", c.typeName, fieldName)
				}
				// field was specified in datum; therefore its value was invalid
				return buf, fmt.Errorf("Record %q field value for %q does not match its schema: %s", c.typeName, fieldName, err)
			}
		}
		return buf, nil
	}

	c.textDecoder = func(buf []byte) (interface{}, []byte, error) {
		var mapValues map[string]interface{}
		var err error
		mapValues, buf, err = genericMapTextDecoder(buf, nil, codecFromFieldName) // defaultCodec == nil
		if err != nil {
			return nil, buf, err
		}
		if actual, expected := len(mapValues), len(codecFromFieldName); actual != expected {
			return nil, buf, fmt.Errorf("cannot decode Record: only found %d of %d fields", actual, expected)
		}
		return mapValues, buf, nil
	}

	c.textEncoder = func(buf []byte, datum interface{}) ([]byte, error) {
		return genericMapTextEncoder(buf, datum, nil, codecFromFieldName) // defaultCodec == nil
	}

	return c, nil
}

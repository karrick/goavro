package goavro

import (
	"errors"
	"fmt"
)

func (st symtab) makeRecordCodec(enclosingNamespace string, schema interface{}) (*codec, error) {
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("cannot create Record codec: expected: map[string]interface{}; received: %T", schema)
	}
	fields, ok := schemaMap["fields"]
	if !ok {
		return nil, fmt.Errorf("cannot create Record codec: ought to have fields key")
	}
	fieldSchemas, ok := fields.([]interface{})
	if !ok || len(fieldSchemas) == 0 {
		return nil, fmt.Errorf("cannot create Record codec: fields ought to be non-empty array")
	}

	recordName, err := newNameFromSchemaMap(enclosingNamespace, schemaMap)
	if err != nil {
		return nil, fmt.Errorf("cannot create Record codec: %s", err)
	}

	fieldCodecs := make([]*codec, len(fieldSchemas))
	fieldNames := make([]string, len(fieldSchemas))
	for i, fieldSchema := range fieldSchemas {
		// fmt.Printf("fieldSchema: %v\n", fieldSchema)
		fieldSchemaMap, ok := fieldSchema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot create Record codec: field schema ought to be Go map[string]interface{}; received: %T", fieldSchema)
		}
		fieldCodec, err := st.buildCodecForTypeDescribedByMap(recordName.Namespace, fieldSchemaMap) // field's enclosing namespace
		if err != nil {
			return nil, fmt.Errorf("cannot create Record codec: cannot create codec for record field: %d; %s", i, err)
		}

		// field's full name, e.g., "com.example.X", ought to be registered with the symbol table;
		// however, field short name ought to be used for encoding or decoding
		st.registerCodec(fieldCodec, fieldSchemaMap, recordName.Namespace)
		fieldNames[i] = fieldCodec.name.short()

		fieldCodecs[i] = fieldCodec
	}

	c := &codec{
		namedType: true,
		binaryDecoder: func(buf []byte) (interface{}, []byte, error) {
			recordMap := make(map[string]interface{}, len(fieldCodecs))
			for i, codec := range fieldCodecs {
				var value interface{}
				var err error
				value, buf, err = codec.binaryDecoder(buf)
				if err != nil {
					return nil, buf, err
				}
				recordMap[fieldNames[i]] = value
			}
			return recordMap, buf, nil
		},
		binaryEncoder: func(buf []byte, datum interface{}) ([]byte, error) {
			valueMap, ok := datum.(map[string]interface{})
			if !ok {
				return buf, fmt.Errorf("cannot encode record: value expected Go map[string]interface{}; received: %T", datum)
			}
			if actual, expected := len(valueMap), len(fieldCodecs); actual != expected {
				return buf, fmt.Errorf("cannot encode record: number of fields does not match schema: %d != %d", actual, expected)
			}
			// records encoded in order fields were defined in schema
			for i, codec := range fieldCodecs {
				fieldName := fieldNames[i]
				fieldValue, ok := valueMap[fieldName]
				if !ok {
					return buf, fmt.Errorf("cannot encode record: field value not found: %q", fieldName)
				}
				var err error
				buf, err = codec.binaryEncoder(buf, fieldValue)
				if err != nil {
					return buf, fmt.Errorf("cannot encode record: field value does not match its schema: %s", err)
				}
			}
			return buf, nil
		},
	}

	if err := st.registerCodec(c, schemaMap, enclosingNamespace); err != nil {
		return nil, fmt.Errorf("cannot create Record codec: %s", err)
	}
	if c.name == nil {
		return nil, errors.New("cannot create Record codec: record requires name")
	}
	return c, nil
}

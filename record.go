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
	fieldNameDuplicateCheck := make(map[string]struct{})
	for i, fieldSchema := range fieldSchemas {
		fieldSchemaMap, ok := fieldSchema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot create Record codec: field schema ought to be Go map[string]interface{}; received: %T", fieldSchema)
		}

		// NOTE: field names are not registered in the symbol table, because field names are not
		// individually addressable codecs.

		fieldCodec, err := st.buildCodecForTypeDescribedByMap(nullNamespace, fieldSchemaMap)
		if err != nil {
			return nil, fmt.Errorf("cannot create Record codec: cannot create codec for record field: %d; %s", i+1, err)
		}
		// However, when creating a full name for the field name, be sure to use record's namespace
		n, err := newNameFromSchemaMap(recordName.Namespace, fieldSchemaMap)
		if err != nil {
			return nil, fmt.Errorf("cannot create Record codec: invalid name for field: %d; %v", i+1, fieldSchemaMap)
		}
		fieldName := n.short()
		if _, ok := fieldNameDuplicateCheck[fieldName]; ok {
			return nil, fmt.Errorf("cannot create Record codec: duplicate field name for field: %d; %s", i+1, fieldName)
		}
		fieldNameDuplicateCheck[fieldName] = struct{}{}
		fieldNames[i] = fieldName

		fieldCodecs[i] = fieldCodec
	}

	c := &codec{
		binaryDecoder: func(buf []byte) (interface{}, []byte, error) {
			recordMap := make(map[string]interface{}, len(fieldCodecs))
			for i, c := range fieldCodecs {
				var value interface{}
				var err error
				value, buf, err = c.binaryDecoder(buf)
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
			for i, c := range fieldCodecs {
				fieldName := fieldNames[i]
				fieldValue, ok := valueMap[fieldName]
				if !ok {
					return buf, fmt.Errorf("cannot encode record: field value not found: %q", fieldName)
				}
				var err error
				buf, err = c.binaryEncoder(buf, fieldValue)
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

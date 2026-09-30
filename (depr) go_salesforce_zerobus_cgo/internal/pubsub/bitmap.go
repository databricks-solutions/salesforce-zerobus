package pubsub

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ProcessBitmap converts Salesforce CDC bitmap hex strings to field name lists
// using the Avro schema to map bit positions to field names.
func ProcessBitmap(schemaJSON string, bitmapFields []interface{}) ([]string, error) {
	if len(bitmapFields) == 0 {
		return nil, nil
	}

	schema, err := parseAvroSchema(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing Avro schema for bitmap: %w", err)
	}

	var fields []string

	for _, bf := range bitmapFields {
		bitmapStr, ok := bf.(string)
		if !ok {
			continue
		}

		if strings.HasPrefix(bitmapStr, "0x") || strings.HasPrefix(bitmapStr, "0X") {
			// Top-level bitmap
			names := fieldNamesFromHex(bitmapStr, schema.Fields)
			fields = append(fields, names...)
		} else if strings.Contains(bitmapStr, "-") {
			// Nested bitmap: "parentPos-childBitmap"
			parts := strings.SplitN(bitmapStr, "-", 2)
			parentPos, err := strconv.Atoi(parts[0])
			if err != nil {
				continue
			}
			childBitmap := parts[1]

			if parentPos < len(schema.Fields) {
				parentField := schema.Fields[parentPos]
				childFields := getNestedFields(parentField.Type)
				childNames := fieldNamesFromHex(childBitmap, childFields)
				for i, name := range childNames {
					childNames[i] = parentField.Name + "." + name
				}
				fields = append(fields, childNames...)
			}
		}
	}

	return fields, nil
}

func fieldNamesFromHex(hex string, fields []avroField) []string {
	hex = strings.TrimPrefix(hex, "0x")
	hex = strings.TrimPrefix(hex, "0X")

	bitset := hexToBitset(hex)
	var names []string
	for i, set := range bitset {
		if set && i < len(fields) {
			names = append(names, fields[i].Name)
		}
	}
	return names
}

// hexToBitset converts a hex string to a slice of bools representing bit positions.
// The bit ordering follows Salesforce's convention: LSB of each byte first, bytes reversed.
func hexToBitset(hex string) []bool {
	var bits []bool
	for i := len(hex) - 1; i >= 0; i-- {
		nibble, err := strconv.ParseUint(string(hex[i]), 16, 8)
		if err != nil {
			bits = append(bits, false, false, false, false)
			continue
		}
		bits = append(bits,
			nibble&1 != 0,
			nibble&2 != 0,
			nibble&4 != 0,
			nibble&8 != 0,
		)
	}
	return bits
}

type avroSchema struct {
	Fields []avroField `json:"fields"`
}

type avroField struct {
	Name string      `json:"name"`
	Type interface{} `json:"type"`
}

func parseAvroSchema(schemaJSON string) (*avroSchema, error) {
	var schema avroSchema
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

// getNestedFields extracts field definitions from a union or record type.
func getNestedFields(fieldType interface{}) []avroField {
	switch t := fieldType.(type) {
	case []interface{}:
		// Union type: find the record within it
		for _, item := range t {
			if fields := getNestedFields(item); fields != nil {
				return fields
			}
		}
	case map[string]interface{}:
		if fields, ok := t["fields"]; ok {
			if fieldSlice, ok := fields.([]interface{}); ok {
				var result []avroField
				for _, f := range fieldSlice {
					if fMap, ok := f.(map[string]interface{}); ok {
						af := avroField{
							Name: fmt.Sprintf("%v", fMap["name"]),
							Type: fMap["type"],
						}
						result = append(result, af)
					}
				}
				return result
			}
		}
	}
	return nil
}

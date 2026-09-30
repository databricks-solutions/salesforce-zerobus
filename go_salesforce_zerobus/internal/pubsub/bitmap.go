package pubsub

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// BitmapSchema maps Salesforce CDC bitmap positions to Avro field names. It is
// parsed once per schema and cached alongside the codec, so decoding a bitmap
// does not re-parse the schema JSON on every event.
//
// See "Event Deserialization Considerations" in the Pub/Sub API guide.
type BitmapSchema struct {
	fields []bitmapField
}

type bitmapField struct {
	name     string
	children []string // field names of a nested record type, if any
}

// ParseBitmapSchema parses the top-level fields of an Avro record schema.
func ParseBitmapSchema(schemaJSON string) (*BitmapSchema, error) {
	var schema struct {
		Fields []struct {
			Name string `json:"name"`
			Type any    `json:"type"`
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return nil, fmt.Errorf("parsing Avro schema for bitmap: %w", err)
	}
	bs := &BitmapSchema{fields: make([]bitmapField, len(schema.Fields))}
	for i, f := range schema.Fields {
		bs.fields[i] = bitmapField{name: f.Name, children: nestedFieldNames(f.Type)}
	}
	return bs, nil
}

// FieldNames converts bitmap strings (e.g. changedFields) to field names.
// A top-level bitmap looks like "0x0A"; a nested bitmap looks like
// "3-0x04", meaning bit 2 of the record field at position 3.
func (s *BitmapSchema) FieldNames(bitmaps []string) []string {
	if s == nil || len(bitmaps) == 0 {
		return nil
	}
	var names []string
	for _, bm := range bitmaps {
		switch {
		case hasHexPrefix(bm):
			for _, pos := range setBits(bm) {
				if pos < len(s.fields) {
					names = append(names, s.fields[pos].name)
				}
			}
		case strings.Contains(bm, "-"):
			parent, child, _ := strings.Cut(bm, "-")
			parentPos, err := strconv.Atoi(parent)
			if err != nil || parentPos < 0 || parentPos >= len(s.fields) {
				continue
			}
			pf := s.fields[parentPos]
			for _, pos := range setBits(child) {
				if pos < len(pf.children) {
					names = append(names, pf.name+"."+pf.children[pos])
				}
			}
		}
	}
	return names
}

func hasHexPrefix(s string) bool {
	return strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X")
}

// setBits returns the set bit positions of a hex bitmap. Salesforce orders bits
// from the least significant bit of the last hex digit, i.e. the reversed
// binary string of the whole value.
func setBits(hex string) []int {
	hex = strings.TrimPrefix(strings.TrimPrefix(hex, "0x"), "0X")
	var positions []int
	pos := 0
	for i := len(hex) - 1; i >= 0; i-- {
		nibble, err := strconv.ParseUint(hex[i:i+1], 16, 8)
		if err != nil {
			nibble = 0
		}
		for b := 0; b < 4; b++ {
			if nibble&(1<<b) != 0 {
				positions = append(positions, pos)
			}
			pos++
		}
	}
	return positions
}

// nestedFieldNames returns the field names of a record type, looking through
// unions such as ["null", {"type":"record",...}].
func nestedFieldNames(t any) []string {
	switch v := t.(type) {
	case []any:
		for _, item := range v {
			if names := nestedFieldNames(item); names != nil {
				return names
			}
		}
	case map[string]any:
		fields, ok := v["fields"].([]any)
		if !ok {
			return nil
		}
		names := make([]string, 0, len(fields))
		for _, f := range fields {
			if fm, ok := f.(map[string]any); ok {
				names = append(names, fmt.Sprint(fm["name"]))
			}
		}
		return names
	}
	return nil
}

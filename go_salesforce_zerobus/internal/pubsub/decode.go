package pubsub

import (
	"fmt"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/event"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/pubsubpb"
)

// DecodeEvent decodes a Pub/Sub ProducerEvent into a CDCEvent. It never
// fails: if the payload cannot be decoded, the returned event carries the raw
// payload, schema, and a DecodeError so it is still written (and the replay
// checkpoint never moves past an event that was not stored).
func DecodeEvent(s *Schema, ev *pubsubpb.ProducerEvent, replayID []byte) *event.CDCEvent {
	e := &event.CDCEvent{
		EventID:       ev.GetId(),
		SchemaID:      ev.GetSchemaId(),
		ReplayID:      replayID,
		PayloadBinary: ev.GetPayload(),
	}
	if s == nil {
		e.DecodeError = "schema unavailable"
		return e
	}
	e.SchemaJSON = s.JSON

	native, _, err := s.Codec.NativeFromBinary(ev.GetPayload())
	if err != nil {
		e.DecodeError = fmt.Sprintf("avro decode: %v", err)
		return e
	}
	record, ok := native.(map[string]any)
	if !ok {
		e.DecodeError = fmt.Sprintf("avro decode: value is %T, not a record", native)
		return e
	}

	if h, ok := unwrapUnion(record["ChangeEventHeader"]).(map[string]any); ok {
		e.ChangeType = stringField(h, "changeType")
		e.EntityName = stringField(h, "entityName")
		e.ChangeOrigin = stringField(h, "changeOrigin")
		e.TransactionKey = stringField(h, "transactionKey")
		e.CommitUser = stringField(h, "commitUser")
		e.RecordIDs = stringSliceField(h, "recordIds")
		e.ChangedFields = s.Bitmap.FieldNames(stringSliceField(h, "changedFields"))
		e.NulledFields = s.Bitmap.FieldNames(stringSliceField(h, "nulledFields"))
		e.DiffFields = s.Bitmap.FieldNames(stringSliceField(h, "diffFields"))
		if v, ok := int64Field(h, "commitNumber"); ok {
			e.CommitNumber = &v
		}
		if v, ok := int64Field(h, "sequenceNumber"); ok {
			n := int32(v)
			e.SequenceNumber = &n
		}
	}

	// Standard JSON: unions are unwrapped ({"Name":"Acme"}, not
	// {"Name":{"string":"Acme"}}), matching the Python service's output.
	if text, err := s.Codec.TextualFromNative(nil, native); err == nil {
		e.RecordDataJSON = string(text)
	} else {
		e.RecordDataJSON = "{}"
		e.DecodeError = fmt.Sprintf("json encode: %v", err)
	}
	return e
}

// unwrapUnion returns the value of a goavro union ({"type": value}).
func unwrapUnion(v any) any {
	if m, ok := v.(map[string]any); ok && len(m) == 1 {
		for _, val := range m {
			return val
		}
	}
	return v
}

func stringField(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	v = unwrapUnion(v)
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func stringSliceField(m map[string]any, key string) []string {
	arr, ok := unwrapUnion(m[key]).([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		switch s := unwrapUnion(item).(type) {
		case string:
			out = append(out, s)
		case nil:
		default:
			out = append(out, fmt.Sprint(s))
		}
	}
	return out
}

func int64Field(m map[string]any, key string) (int64, bool) {
	switch v := unwrapUnion(m[key]).(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	}
	return 0, false
}

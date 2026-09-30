// Package event defines the decoded CDC event and its conversion to the
// SalesforceEvent protobuf row written to Delta through Zerobus.
package event

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/eventpb"
)

// CDCEvent is a decoded Salesforce Change Data Capture event.
type CDCEvent struct {
	EventID        string
	SchemaID       string
	ReplayID       []byte
	ChangeType     string
	EntityName     string
	ChangeOrigin   string
	RecordIDs      []string
	ChangedFields  []string
	NulledFields   []string
	DiffFields     []string
	RecordDataJSON string
	PayloadBinary  []byte
	SchemaJSON     string
	CommitNumber   *int64
	SequenceNumber *int32
	TransactionKey string
	CommitUser     string
	// DecodeError is non-empty when the payload could not be decoded; the row
	// is still written with the raw payload so no event is silently lost.
	DecodeError string
}

// Source identifies where an event came from.
type Source struct {
	OrgID     string
	TenantKey string
	Topic     string
}

// ToProto converts e to the Delta row format. now is the ingest timestamp.
func ToProto(e *CDCEvent, src Source, now time.Time) *eventpb.SalesforceEvent {
	ms := now.UnixMilli()
	row := &eventpb.SalesforceEvent{
		EventId:            proto.String(e.EventID),
		SchemaId:           proto.String(e.SchemaID),
		ReplayId:           proto.String(hex.EncodeToString(e.ReplayID)),
		Timestamp:          proto.Int64(ms),
		ChangeType:         optString(e.ChangeType),
		EntityName:         optString(e.EntityName),
		ChangeOrigin:       optString(e.ChangeOrigin),
		RecordIds:          e.RecordIDs,
		ChangedFields:      e.ChangedFields,
		NulledFields:       e.NulledFields,
		DiffFields:         e.DiffFields,
		RecordDataJson:     optString(e.RecordDataJSON),
		PayloadBinary:      e.PayloadBinary,
		SchemaJson:         optString(e.SchemaJSON),
		OrgId:              proto.String(src.OrgID),
		ProcessedTimestamp: proto.Int64(ms),
		Topic:              proto.String(src.Topic),
		TenantKey:          proto.String(src.TenantKey),
		DecodeError:        optString(e.DecodeError),
		CommitNumber:       e.CommitNumber,
		SequenceNumber:     e.SequenceNumber,
		TransactionKey:     optString(e.TransactionKey),
		CommitUser:         optString(e.CommitUser),
	}
	return row
}

// Marshal serializes the row, dropping the largest optional columns if the
// encoded row would exceed maxBytes. It reports whether the row was
// truncated. Truncation order: schema_json, record_data_json, payload_binary.
// Every truncation is recorded in decode_error.
func Marshal(row *eventpb.SalesforceEvent, maxBytes int) ([]byte, bool, error) {
	data, err := proto.Marshal(row)
	if err != nil {
		return nil, false, fmt.Errorf("marshaling SalesforceEvent: %w", err)
	}
	if maxBytes <= 0 || len(data) <= maxBytes {
		return data, false, nil
	}
	var dropped []string
	steps := []struct {
		name  string
		clear func()
	}{
		{"schema_json", func() { row.SchemaJson = nil }},
		{"record_data_json", func() { row.RecordDataJson = nil }},
		{"payload_binary", func() { row.PayloadBinary = nil }},
	}
	for _, step := range steps {
		step.clear()
		dropped = append(dropped, step.name)
		msg := "truncated: dropped " + strings.Join(dropped, ",")
		if prev := row.GetDecodeError(); prev != "" && !strings.HasPrefix(prev, "truncated:") {
			msg = prev + "; " + msg
		}
		row.DecodeError = proto.String(msg)
		data, err = proto.Marshal(row)
		if err != nil {
			return nil, false, fmt.Errorf("marshaling SalesforceEvent: %w", err)
		}
		if len(data) <= maxBytes {
			return data, true, nil
		}
	}
	return nil, true, fmt.Errorf("row for event %s is %d bytes after truncation (limit %d)", row.GetEventId(), len(data), maxBytes)
}

func optString(s string) *string {
	if s == "" {
		return nil
	}
	return proto.String(s)
}

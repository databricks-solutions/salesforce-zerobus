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

// Format selects the payload columns written for each event.
type Format string

const (
	// FormatVariant writes the decoded event to the record VARIANT column.
	// The raw Avro payload and its schema are written only when decoding
	// fails, so an undecodable event can still be recovered.
	FormatVariant Format = "variant"
	// FormatAvro writes record_data_json, payload_binary, and schema_json on
	// every row: the original layout, read by from_avro pipelines.
	FormatAvro Format = "avro"
	// FormatBoth writes both layouts, for migrating a pipeline.
	FormatBoth Format = "both"
)

// ParseFormat parses a payload format name.
func ParseFormat(s string) (Format, error) {
	switch f := Format(strings.ToLower(strings.TrimSpace(s))); f {
	case FormatVariant, FormatAvro, FormatBoth:
		return f, nil
	}
	return "", fmt.Errorf("payload format must be variant, avro, or both, got %q", s)
}

// Includes reports whether the row schema (the Zerobus descriptor and so the
// Delta columns) has field under f. The avro layout omits record, so tables
// created before the VARIANT column keep working unchanged.
func (f Format) Includes(field string) bool { return f != FormatAvro || field != "record" }

// Source identifies where an event came from.
type Source struct {
	OrgID     string
	TenantKey string
	Topic     string
}

// ToProto converts e to the Delta row format for f (default variant). now is
// the ingest timestamp.
func ToProto(e *CDCEvent, src Source, f Format, now time.Time) *eventpb.SalesforceEvent {
	if f == "" {
		f = FormatVariant
	}
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
	decoded := e.DecodeError == ""
	if f != FormatAvro && decoded {
		row.Record = optString(e.RecordDataJSON)
	}
	if f != FormatVariant {
		row.RecordDataJson = optString(e.RecordDataJSON)
	}
	if f != FormatVariant || !decoded {
		row.PayloadBinary = e.PayloadBinary
		row.SchemaJson = optString(e.SchemaJSON)
	}
	return row
}

// Marshal serializes the row, dropping the largest optional columns if the
// encoded row would exceed maxBytes. It reports whether the row was
// truncated. Truncation order: schema_json, record_data_json, payload_binary,
// record; columns that are already empty are skipped. Every truncation is
// recorded in decode_error.
func Marshal(row *eventpb.SalesforceEvent, maxBytes int) ([]byte, bool, error) {
	data, err := proto.Marshal(row)
	if err != nil {
		return nil, false, fmt.Errorf("marshaling SalesforceEvent: %w", err)
	}
	if maxBytes <= 0 || len(data) <= maxBytes {
		return data, false, nil
	}
	var dropped []string
	prev := row.GetDecodeError()
	steps := []struct {
		name  string
		set   bool
		clear func()
	}{
		{"schema_json", row.SchemaJson != nil, func() { row.SchemaJson = nil }},
		{"record_data_json", row.RecordDataJson != nil, func() { row.RecordDataJson = nil }},
		{"payload_binary", row.PayloadBinary != nil, func() { row.PayloadBinary = nil }},
		{"record", row.Record != nil, func() { row.Record = nil }},
	}
	for _, step := range steps {
		if !step.set {
			continue
		}
		step.clear()
		dropped = append(dropped, step.name)
		msg := "truncated: dropped " + strings.Join(dropped, ",")
		if prev != "" {
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

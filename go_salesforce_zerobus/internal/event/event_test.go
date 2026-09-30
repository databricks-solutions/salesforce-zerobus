package event

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/eventpb"
)

func sampleEvent() *CDCEvent {
	cn := int64(7)
	return &CDCEvent{
		EventID: "e1", SchemaID: "s1", ReplayID: []byte{0x00, 0x2a},
		ChangeType: "UPDATE", EntityName: "Account", RecordIDs: []string{"001"},
		ChangedFields: []string{"Name"}, RecordDataJSON: `{"Name":"Acme"}`,
		PayloadBinary: []byte{1, 2, 3}, SchemaJSON: `{"type":"record"}`, CommitNumber: &cn,
	}
}

func TestToProto(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	row := ToProto(sampleEvent(), Source{OrgID: "00D", TenantKey: "acme", Topic: "/data/AccountChangeEvent"}, now)
	if row.GetReplayId() != "002a" {
		t.Errorf("replay_id = %q, want hex 002a", row.GetReplayId())
	}
	if row.GetTimestamp() != now.UnixMilli() || row.GetProcessedTimestamp() != now.UnixMilli() {
		t.Errorf("timestamps not set to ingest time")
	}
	if row.GetOrgId() != "00D" || row.GetTenantKey() != "acme" || row.GetTopic() != "/data/AccountChangeEvent" {
		t.Errorf("source fields not set: %v", row)
	}
	if row.GetCommitNumber() != 7 || row.SequenceNumber != nil {
		t.Errorf("header fields wrong: commit=%v seq=%v", row.CommitNumber, row.SequenceNumber)
	}
	if row.DecodeError != nil || row.ChangeOrigin != nil {
		t.Errorf("empty strings should be null columns")
	}
}

func TestMarshalTruncates(t *testing.T) {
	e := sampleEvent()
	e.SchemaJSON = strings.Repeat("s", 5000)
	e.RecordDataJSON = strings.Repeat("r", 5000)
	row := ToProto(e, Source{OrgID: "00D"}, time.Now())

	data, truncated, err := Marshal(proto.Clone(row).(*eventpb.SalesforceEvent), 1<<20)
	if err != nil || truncated || len(data) == 0 {
		t.Fatalf("small row: truncated=%v err=%v", truncated, err)
	}

	data, truncated, err = Marshal(row, 6000)
	if err != nil || !truncated {
		t.Fatalf("expected truncation, got truncated=%v err=%v", truncated, err)
	}
	var got eventpb.SalesforceEvent
	if err := proto.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaJson != nil || got.GetRecordDataJson() == "" {
		t.Errorf("expected only schema_json dropped")
	}
	if got.GetDecodeError() != "truncated: dropped schema_json" {
		t.Errorf("decode_error = %q", got.GetDecodeError())
	}

	if _, _, err := Marshal(ToProto(e, Source{}, time.Now()), 10); err == nil {
		t.Fatal("expected error when row cannot fit")
	}
}

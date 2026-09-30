package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/cdc"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/pubsubpb"
)

func TestDecodeAccountEvent(t *testing.T) {
	schema, err := CompileSchema("s1", cdc.AccountSchemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := cdc.EncodeAccount(cdc.AccountEvent{
		RecordID: "001xx", ChangeType: "UPDATE", Name: "Acme", City: "SF",
		CommitNumber: 99, SequenceNumber: 2, TransactionKey: "tx1",
		ChangedFields: []string{"0x0A", "3-0x02"}, // Name, BillingAddress; BillingAddress.City
	})
	if err != nil {
		t.Fatal(err)
	}
	e := DecodeEvent(schema, &pubsubpb.ProducerEvent{Id: "ev1", SchemaId: "s1", Payload: payload}, []byte{1})
	if e.DecodeError != "" {
		t.Fatalf("unexpected decode error: %s", e.DecodeError)
	}
	if e.ChangeType != "UPDATE" || e.EntityName != "Account" || !reflect.DeepEqual(e.RecordIDs, []string{"001xx"}) {
		t.Errorf("header = %+v", e)
	}
	if e.CommitNumber == nil || *e.CommitNumber != 99 || e.SequenceNumber == nil || *e.SequenceNumber != 2 {
		t.Errorf("commit/sequence not decoded: %v %v", e.CommitNumber, e.SequenceNumber)
	}
	if e.TransactionKey != "tx1" || e.CommitUser != "005000000000001" {
		t.Errorf("transaction fields: %q %q", e.TransactionKey, e.CommitUser)
	}
	want := []string{"Name", "BillingAddress", "BillingAddress.City"}
	if !reflect.DeepEqual(e.ChangedFields, want) {
		t.Errorf("ChangedFields = %v, want %v", e.ChangedFields, want)
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(e.RecordDataJSON), &rec); err != nil {
		t.Fatalf("record json: %v", err)
	}
	if rec["Name"] != "Acme" {
		t.Errorf("record_data_json should have unwrapped unions, got Name=%v", rec["Name"])
	}
	if addr, _ := rec["BillingAddress"].(map[string]any); addr["City"] != "SF" {
		t.Errorf("nested union not unwrapped: %v", rec["BillingAddress"])
	}
}

func TestDecodeBadPayloadKeepsRawRow(t *testing.T) {
	schema, _ := CompileSchema("s1", cdc.AccountSchemaJSON)
	e := DecodeEvent(schema, &pubsubpb.ProducerEvent{Id: "ev1", SchemaId: "s1", Payload: []byte{0xff, 0xff}}, []byte{1})
	if e.DecodeError == "" || !reflect.DeepEqual(e.PayloadBinary, []byte{0xff, 0xff}) || e.SchemaJSON == "" {
		t.Fatalf("expected raw row with decode error, got %+v", e)
	}
}

func TestSchemaCacheSingleflightAndEviction(t *testing.T) {
	c := NewSchemaCache(int64(len(cdc.AccountSchemaJSON)) * 4 * 2) // room for 2 entries
	var fetches atomic.Int32
	fetch := func(ctx context.Context, id string) (string, error) {
		fetches.Add(1)
		return cdc.AccountSchemaJSON, nil
	}
	for i := 0; i < 3; i++ {
		if _, err := c.Get(context.Background(), "orgA", "s1", fetch); err != nil {
			t.Fatal(err)
		}
	}
	if fetches.Load() != 1 {
		t.Fatalf("fetches = %d, want 1", fetches.Load())
	}
	c.Get(context.Background(), "orgB", "s1", fetch) // same schema ID, different org: separate entry
	c.Get(context.Background(), "orgC", "s1", fetch) // evicts orgA
	if c.Len() != 2 || fetches.Load() != 3 {
		t.Fatalf("len=%d fetches=%d", c.Len(), fetches.Load())
	}
	c.Get(context.Background(), "orgA", "s1", fetch)
	if fetches.Load() != 4 {
		t.Fatalf("orgA should have been evicted; fetches=%d", fetches.Load())
	}

	boom := errors.New("boom")
	if _, err := c.Get(context.Background(), "orgZ", "s9", func(context.Context, string) (string, error) { return "", boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

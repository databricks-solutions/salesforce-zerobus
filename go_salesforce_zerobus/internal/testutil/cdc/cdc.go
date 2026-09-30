// Package cdc provides a realistic Salesforce Change Data Capture Avro schema
// and an encoder for test fixtures, the fake Pub/Sub server, and the load
// generator.
package cdc

import (
	"crypto/sha256"
	"encoding/base64"

	"github.com/linkedin/goavro/v2"
)

// AccountSchemaJSON mirrors the shape of a Salesforce AccountChangeEvent
// schema (ChangeEventHeader first, then nullable fields, including a nested
// compound Address field).
const AccountSchemaJSON = `{
  "type": "record", "name": "AccountChangeEvent", "namespace": "com.sforce.eventbus",
  "fields": [
    {"name": "ChangeEventHeader", "type": {"type": "record", "name": "ChangeEventHeader", "fields": [
      {"name": "entityName", "type": "string"},
      {"name": "recordIds", "type": {"type": "array", "items": "string"}},
      {"name": "changeType", "type": {"type": "enum", "name": "ChangeType", "namespace": "com.sforce.eventbus",
        "symbols": ["CREATE", "UPDATE", "DELETE", "UNDELETE", "GAP_CREATE", "GAP_UPDATE", "GAP_DELETE", "GAP_UNDELETE", "GAP_OVERFLOW"]}},
      {"name": "changeOrigin", "type": "string"},
      {"name": "transactionKey", "type": "string"},
      {"name": "sequenceNumber", "type": "int"},
      {"name": "commitTimestamp", "type": "long"},
      {"name": "commitNumber", "type": "long"},
      {"name": "commitUser", "type": "string"},
      {"name": "nulledFields", "type": {"type": "array", "items": "string"}},
      {"name": "diffFields", "type": {"type": "array", "items": "string"}},
      {"name": "changedFields", "type": {"type": "array", "items": "string"}}
    ]}},
    {"name": "Name", "type": ["null", "string"], "default": null},
    {"name": "Type", "type": ["null", "string"], "default": null},
    {"name": "BillingAddress", "type": ["null", {"type": "record", "name": "Address", "fields": [
      {"name": "Street", "type": ["null", "string"], "default": null},
      {"name": "City", "type": ["null", "string"], "default": null},
      {"name": "State", "type": ["null", "string"], "default": null}
    ]}], "default": null},
    {"name": "AnnualRevenue", "type": ["null", "double"], "default": null},
    {"name": "LastModifiedDate", "type": ["null", "long"], "default": null}
  ]
}`

// SchemaID returns a stable schema ID for schemaJSON, in the style of
// Salesforce's base64 schema fingerprints.
func SchemaID(schemaJSON string) string {
	sum := sha256.Sum256([]byte(schemaJSON))
	return base64.RawURLEncoding.EncodeToString(sum[:16])
}

// AccountEvent describes one fixture event.
type AccountEvent struct {
	RecordID       string
	ChangeType     string // CREATE, UPDATE, ...
	Name           string
	City           string
	CommitNumber   int64
	SequenceNumber int32
	TransactionKey string
	ChangedFields  []string // bitmap strings, e.g. "0x02", "3-0x02"
}

var accountCodec = mustCodec(AccountSchemaJSON)

func mustCodec(schema string) *goavro.Codec {
	c, err := goavro.NewCodec(schema)
	if err != nil {
		panic(err)
	}
	return c
}

// EncodeAccount returns the Avro binary payload for e.
func EncodeAccount(e AccountEvent) ([]byte, error) {
	changed := make([]any, len(e.ChangedFields))
	for i, f := range e.ChangedFields {
		changed[i] = f
	}
	header := map[string]any{
		"entityName":      "Account",
		"recordIds":       []any{e.RecordID},
		"changeType":      e.ChangeType,
		"changeOrigin":    "com/salesforce/api/rest/62.0",
		"transactionKey":  e.TransactionKey,
		"sequenceNumber":  e.SequenceNumber,
		"commitTimestamp": int64(1_700_000_000_000),
		"commitNumber":    e.CommitNumber,
		"commitUser":      "005000000000001",
		"nulledFields":    []any{},
		"diffFields":      []any{},
		"changedFields":   changed,
	}
	rec := map[string]any{
		"ChangeEventHeader": header,
		"Name":              nullable("string", e.Name),
		"Type":              nil,
		"BillingAddress":    nil,
		"AnnualRevenue":     nil,
		"LastModifiedDate":  goavro.Union("long", int64(1_700_000_000_000)),
	}
	if e.City != "" {
		rec["BillingAddress"] = goavro.Union("com.sforce.eventbus.Address", map[string]any{
			"Street": nil, "City": goavro.Union("string", e.City), "State": nil,
		})
	}
	return accountCodec.BinaryFromNative(nil, rec)
}

func nullable(typ, v string) any {
	if v == "" {
		return nil
	}
	return goavro.Union(typ, v)
}

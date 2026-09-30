# DEPRECATED

This folder contains the original single-tenant Go port of `salesforce_zerobus`,
built on the CGO/Rust-backed Zerobus SDK (`github.com/databricks/zerobus-sdk/go`).

It is **deprecated** and kept only for reference and for existing deployments
while they migrate. It receives no new features.

The long-term service is [`../go_salesforce_zerobus/`](../go_salesforce_zerobus/):

- pure Go (no CGO, static binary) on the Zerobus pure-Go SDK
- multi-tenant: thousands of Salesforce orgs per deployment, hash-sharded replicas
- durable, ack-driven checkpoints in Lakebase Postgres
- single-org env-var mode for drop-in migration from this service

See `../go_salesforce_zerobus/README.md` → "Migrating from go_salesforce_zerobus_cgo".

# Design

## Components

| Package | Responsibility |
|---|---|
| `cmd/zerobus` | CLI: `run`, `validate`, `migrate`, `tenants`, `checkpoint`, `bootstrap lakebase`, `healthcheck` |
| `internal/app` | Wiring, startup order, graceful shutdown, status reporting |
| `internal/config` | Service settings (flags/env), service defaults, single-tenant legacy env mode |
| `internal/tenant` | Tenant model, validation (shared by registry rows and YAML), YAML import format |
| `internal/registry` | Lakebase tenant registry: tables, triggers, version polling, `apply`, status write-back |
| `internal/shard` | `Assigner` interface; static jump-consistent-hash assigner |
| `internal/secrets` | Secret reference resolution (`uc-secret` for Unity Catalog secrets, `env`, `file`, `#json-field`), TTL cache |
| `internal/ucsecrets` | Grants on the tenant secrets schema (`bootstrap uc-secrets`) |
| `internal/sfauth` | OAuth client credentials and SOAP login; per-tenant `TokenSource` (cache, singleflight, proactive refresh) |
| `internal/pubsub` | Pooled gRPC connections, `Subscription` (fetch/recv loops), Avro schema cache, CDC decoding, error classes |
| `internal/event` | CDC event → `SalesforceEvent` protobuf row, size guard |
| `internal/sink/zerobus` | Per-table stream pools, slot writers, ack tracking; SDK factory and in-memory factory |
| `internal/checkpoint` | Watermarks, committer; Lakebase, Delta-derived and memory stores |
| `internal/tablemgr` | Delta table creation and additive migration from the protobuf descriptor |
| `internal/supervisor` | Reconciliation and one isolated runner per subscription |
| `internal/obs` | Prometheus metrics, admin HTTP, logging |
| `internal/testutil` | Fake Pub/Sub server, fake OAuth/SOAP server, CDC fixtures, Postgres test helper |

## Data path

1. The **registry** (`sfzb.tenants`, `sfzb.subscriptions`) is read in one repeatable-read transaction.
   - Rows are validated one by one; invalid tenants are skipped and reported.
   - Statement-level triggers bump `sfzb.registry_version`, so polling reads a single row until something changes.
2. The **assigner** keeps tenants where `jump(fnv64a(tenant_key), SHARD_COUNT) == SHARD_INDEX`, or whose `shard_pin` equals the index.
3. The **supervisor** reconciles the new snapshot against the running subscriptions:
   - It stops removed subscriptions and ones whose effective config hash changed.
   - It loads checkpoints for new subscriptions in one query, and starts a runner for each, with a small random stagger on the first start.
4. A **runner** gets a session from its tenant's shared `TokenSource`, acquires a Pub/Sub client from the shared `ConnPool`, and runs a `Subscription` until it errors. It then retries on the policy for that error class.
5. A **subscription** runs two goroutines:
   - **fetch** sends `FetchRequest`s when credit allows;
   - **recv** receives events, drops redeliveries (dedup LRU), decodes Avro, builds the proto row and `Submit`s it to its pinned sink writer.
6. A **sink slot** (one per stream) batches up to 500 records, 4MiB or 20ms into `IngestRecordsOffsetContext`. Each batch gets one dense offset, which is appended to the slot's **ack tracker**.
7. `OnAck(offset)` drains the tracker in offset order and calls `Watermark.Advance(gen, lastReplayID, n)` for each subscription in the batch.
8. The **committer** upserts every dirty watermark to `sfzb.checkpoints` every 5s, in one statement per 1,000 rows.

## Flow control

A subscription's Salesforce credit is:

```
credit = max_unacked − unacked − pending_requested
```

- `unacked` counts records submitted to the sink but not yet acked by Zerobus. It is incremented *before* `Submit`, so an ack can never be counted first.
- `pending_requested` is resynced from `FetchResponse.pending_num_requested`.
- A `FetchRequest` is sent only when nothing is pending. It asks for `min(batch_size, credit)` events.
- When the sink is behind (credit 0) the stream would go quiet. Salesforce closes a stream with nothing requested for 60s, so the fetch loop sends `num_requested=1` after 50s as a keepalive.
- A slow Zerobus therefore slows Salesforce instead of growing memory. The load test checks the bound (`unacked + pending ≤ max_unacked + 1`).

Salesforce sends keepalive responses on idle streams. When nothing is in flight, their `latest_replay_id` becomes the new position (`AdvanceIdle`). This keeps quiet orgs from resuming from a replay ID that has aged out of the ~72h retention.

## Checkpoint invariant

A stored replay ID is never ahead of the last replay ID Zerobus durably acked for that subscription. Four mechanisms guarantee it:

- **Pinning.** A subscription always writes to the same stream slot (`jump(hash(subKey), slots)`). Zerobus acks a stream's offsets in order, so the last acked batch implies all earlier ones.
- **Ordered application.** Tracker updates are applied under the tracker lock, so a subscription's progress is applied in offset order even when acks race appends. Acks for batches not yet appended are remembered in `ackedThrough`.
- **Generations.** Each restart that may have lost in-flight rows bumps the watermark's generation, and acks carrying an older generation are ignored. Without this, a late ack from a dead stream could move the position past records that were never written.
- **Resume point by error class.**
  - Salesforce-side errors resume from the last *submitted* replay ID in the same generation. Rows already handed to a healthy stream will still be acked, and the SDK re-sends them after a reconnect.
  - Sink failures (`sink_reset`, `sink_unavailable`) bump the generation and resume from the last *acked* replay ID.

A crash loses at most one commit interval of progress, so delivery is at-least-once. On graceful shutdown, `Stream.Close()` flushes every stream. A successful flush means everything appended is durable, so the tracker applies all of it directly, without waiting for asynchronous ack callbacks. Only then does the committer write the final checkpoints.

## Pools and limits

HTTP/2 servers cap concurrent streams per connection, and gRPC-go queues streams past the cap silently. So both sides pack streams onto connections under conservative caps:

| Side | Pool | Cap per connection |
|---|---|---|
| Salesforce | `ConnPool` shared across all orgs (auth is per-RPC metadata); least-loaded placement | `SFZB_SF_SUBS_PER_CONN`, default 100 |
| Zerobus | `SDKFactory` hands out SDK instances; streams on one SDK share its connection and OAuth token cache (tokens are per table) | `SFZB_ZB_STREAMS_PER_SDK`, default 50 |

Zerobus stream slots per table default to one per 250 subscriptions, capped at 16. The count is fixed when the pool is created, so pinning stays stable.

Watch `sfzb_grpc_stream_open_seconds{target}` for signs that a cap is too high.

Startup herd control:
- Token-bucket limiters cover logins, Subscribe calls, GetSchema calls and Databricks Secrets reads.
- Concurrent logins for one tenant collapse into one call (singleflight).
- Schema fetches for the same `(org, schema_id)` collapse too.
- A small random stagger spreads the first starts.

## Failure handling

| Class | Examples | Action |
|---|---|---|
| `transient` | `UNAVAILABLE`, `INTERNAL`, EOF, schema fetch failure, panic | Resume from last submitted; 1s→60s backoff |
| `auth` | `UNAUTHENTICATED` | Invalidate the session, retry immediately once, then back off |
| `permanent_auth` | OAuth 400/401/403, SOAP `INVALID_LOGIN` | State `failed`, retry every 15 min |
| `config` | `PERMISSION_DENIED`, `NOT_FOUND`, non-replay `INVALID_ARGUMENT`, `org_id` mismatch | State `failed`, retry every 15 min |
| `replay_expired` | `INVALID_ARGUMENT` mentioning the replay ID | Use `on_replay_expired`; error log + metric |
| `quota` | `RESOURCE_EXHAUSTED` | 30s→10min backoff |
| `sink_reset` / `sink_unavailable` | Zerobus stream failed or reconnecting | New generation, resume from acked, 2s→2min backoff |

- A Zerobus slot that fails resets only the subscriptions pinned to it, and it reopens with backoff. The SDK already retries recoverable failures itself and re-sends unacked records.
- The process exits only if startup preconditions fail: invalid config, or Lakebase unreachable after about 2 minutes. It never exits because of one tenant.

## Credentials

Tenant credentials never live in the service's tables. The registry stores references, and the resolver fetches values when needed:

| Reference | Backend | Notes |
|---|---|---|
| `uc-secret://<catalog>.<schema>.<secret>[#field]` | Unity Catalog secrets (`SecretsUc.GetSecret` with `include_value`, returning `effective_value`) | Governed by `READ SECRET`; every read is audited in `system.access.audit` |
| `env://VAR`, `file:///path` | Container platform | For platform-injected secrets |

- **Caching:** values are cached per base reference for `SFZB_SECRETS_TTL`. Concurrent lookups collapse into one read, and reads are rate limited, so thousands of tenants starting at once don't flood the secrets API.
- **Resolution time:** secrets are resolved at login, not at startup, so a rotated secret takes effect on the next login after the cache expires.
- **Limits:** UC secrets default to 100 per schema and 1,000 per metastore. These are soft limits that the account team can raise. `#field` lets one secret hold all of a tenant's credentials, which keeps the count at one per tenant.

## Token lifetimes

Every short-lived credential is refreshed before it expires, and every expiry is compared on the **wall clock**. Go's monotonic clock does not advance while a laptop sleeps or a container VM is suspended, so a monotonic comparison can make an expired token look valid after the machine wakes up.

| Credential | Lifetime | Refresh |
|---|---|---|
| Databricks service principal token (SDK calls: Lakebase credentials, UC secrets, tables, SQL) | ~1h | Our own M2M token source (`internal/dbauth`) with wall-clock expiry; the SDK refreshes it ahead of expiry |
| Lakebase database credential | `SFZB_LAKEBASE_TOKEN_TTL` (≤ 1h) | Background loop at 75% of lifetime, with retries and backoff. The current credential keeps serving while a refresh fails. Connections are recycled at 75% of the TTL. |
| Zerobus table token | ~1h | Cached per table until 5 min before expiry; dropped when the server rejects it |
| Salesforce session | tenant `token_ttl` | Re-login before the TTL; dropped on `UNAUTHENTICATED` |

## Zerobus authentication

Streams are opened with `CreateStreamWithProvider` and the service's own token provider (`internal/sink/zerobus/oauth.go`), not the SDK's built-in OAuth.
- purego v0.1.0 caps the token response at 4 KiB. Tokens that embed `authorization_details` can be larger (4,167 bytes was observed on a live workspace), and every mint then fails with `parse token response: unexpected EOF`.
- The provider sends the identical request (same audience and `authorization_details`), so the token grants the same privileges.
- Tokens are cached per table until 5 minutes before expiry, and concurrent mints collapse into one.
- Remove this once the SDK raises the limit.

## Schema evolution

- `SalesforceEvent` is proto2. Fields 1–16 are frozen for compatibility with the Python service's tables; new fields are additive.
- `tablemgr` maps the descriptor to Delta types and runs `ALTER TABLE ADD COLUMNS` for missing columns before a table's first stream opens (Zerobus rejects descriptors with unknown columns).
- Concurrent migrations from several replicas converge: on error, the table is compared again before failing.

## Extension points

| Interface | Built in | Possible additions |
|---|---|---|
| `tenant.Source` | Lakebase registry, static | Admin API |
| `shard.Assigner` | Static jump hash | Lease-based (e.g. Lakebase advisory locks) for automatic rebalancing and one autoscaled service |
| `checkpoint.Store` | Lakebase, Delta-derived, memory, fallback composite | Salesforce ManagedSubscribe per tenant (server-side `CommitReplay`), once GA |
| `zbsink.Factory` | Zerobus SDK, in-memory | Other sinks |

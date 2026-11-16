# AfterZero HTTP API v1

Base URL is the deployed API Gateway URL. JSON request/response bodies. All `/api` endpoints except `POST /api/demo` require `Authorization: Bearer TOKEN`. Owner credentials are local deployment secrets; integration credentials restrict scope and producer writes. No credentials go in URL query parameters.

## Scopes

`POST /api/scopes` (owner only)

```json
{"scope_id":"customer-A","dataset":"imports","partition":{"customer":"A","business_date":"2026-11-13"},"required_producers":["uploader","validator"],"lifetime_seconds":86400}
```

`GET /api/scopes` returns the first 50 scope views. `GET /api/scopes/{scope_id}` returns one live view with receipts, source heads, completeness, committed events, pending count and version. Scope IDs are unique within an authenticated tenant.

## Source changes

`POST /api/scopes/{scope_id}/events` applies a source event synchronously.

```json
{"event_id":"upload-2","producer":"uploader","revision":2,"kind":"append","rows":[{"id":"late-42","validation_status":"invalid"}],"complete":true}
```

`POST /api/scopes/{scope_id}/manifests` writes the same normalized event into a private S3 object. Returns HTTP 202 with object key. S3 notifications asynchronously invoke the ingestion Lambda. HTTP 202 means stored, not yet applied: inspect the scope's source head to observe completion.

Kinds: `append`, `seal`, `reopen`, `changed`. Only `append` accepts projected rows. `complete` defaults to true for a seal and false otherwise; reopen always withdraws completion. Every new revision replaces that producer's completeness flag. A scope remains conditional on the source's declared coverage.

Identical event ID/content returns `duplicate: true` without a new version or workflow. Reusing an ID with different normalized content, or assigning a different event to an occupied revision, returns HTTP 409. Source gaps are buffered, not skipped.

## Observations

`POST /api/scopes/{scope_id}/observations`

```json
{"receipt_id":"report-A-v1","artifact":"daily-report-A","observed_count":0,"basis":{"uploader":1,"validator":1},"predicate":{"all":[{"field":"validation_status","op":"eq","value":"invalid"}]},"valid_for_seconds":86400}
```

This records a producer-attested initial zero result. Every required producer needs an explicit basis revision. Future/uncovered basis revisions are rejected. Stale basis revisions trigger evaluation of all later stored changes before status is returned.

`POST /api/scopes/{scope_id}/evaluate` accepts `{"predicate": ...}` and evaluates the bounded stored append-only projections, returning observed count, evaluated row count, sampled failures and source basis. It refuses evaluation across gaps or coarse changes.

`POST /api/scopes/{scope_id}/check` accepts `receipt_id`, `artifact`, `predicate`, and optional `valid_for_seconds`. It evaluates and registers in one conditional write; caller-supplied count/basis are not used. Nonzero results return 409. This proves the result for the stored projections, not any omitted source columns/rows.

## Predicate format

`{"all":[{"field":"name","op":"eq","value":"value"}]}`

Operators: `eq` (typed scalar equality), `in` (list of typed scalars), `gte` and `lt` (integer ranges). Every term must match. Scalars are string, integer, boolean or explicit null; missing fields never match. Date partitions are scope labels. Use integer epoch seconds for time-range predicates. Unsupported operators/types return 422.

## Integration credentials

`POST /api/credentials` (owner only)

```json
{"scopes":["customer-A"],"producers":["uploader"],"can_observe":false,"lifetime_seconds":86400}
```

Returns the secret token once, its expiration interval and permission grant. It can read its scope and write only as a permitted producer. It cannot create scopes, mint credentials, access the owner inbox, or register observations unless explicitly permitted. Scope lifetime still bounds writes.

## Consumer inbox and evidence

- `GET /api/inbox`: up to 100 durable workflow deliveries, including receipt snapshots, SHA256 and evidence object key. Sort is scope/version, not global chronology. Only owner/demo credentials may read it.
- `GET /api/evidence?key=URL_ENCODED_KEY`: retrieve an archived snapshot belonging to the authenticated tenant.
- `POST /api/ack` with `{"id":"INBOX#scope#000003"}`: persist a delivery acknowledgement. This is not evidence of downstream repair.

Consumers should deduplicate by inbox ID and apply only increasing versions per scope. Older workflow executions can finish after newer ones. A receipt's current applicability always comes from the live scope endpoint, with expiry evaluated at read time.

## Errors and guarantees

401 invalid/expired credential; 403 insufficient permission; 404 inaccessible/missing resource; 409 conflicting identity, source basis, concurrency, failed zero check or scope limit; 413 bounded payload/storage exceeded; 422 unsupported/malformed input; 429 public provisioning cap/API throttling.

Retry transient network/5xx/throttle failures with backoff and the same event/receipt identifiers. Reconcile revision conflicts. Do not turn failed or timed-out source checks into zero-result observations.

Public `POST /api/demo` creates an isolated synthetic workspace and returns a one-hour capability token. Public `GET /health` returns deployment identity, not a deep dependency health guarantee.

# AGENTS.md — FastKV Server

What must never break if you change this code.
Consumer-facing docs: REFERENCE.md, runtime OpenAPI at `/docs` route.

## Red Flags (Read Before Editing)

- Never change response envelopes without updating every handler that uses them
- Never add a new pagination style — one exists, use it
- Never push user filters into CQL without checking partition keys first
- Never knowingly return partial data without signaling `truncated` or logging a warning
- Never bypass `validate_*` in KV handlers

## Module Ownership

- **handlers.rs**
  Owns: HTTP layer, param validation, response envelope construction
  Must NOT: issue CQL, import scylladb internals, construct `Statement`

- **social_handlers.rs**
  Owns: SocialDB compatibility layer, key pattern parsing, tree assembly
  Must NOT: define new envelope types, add pagination styles

- **scylladb.rs**
  Owns: all DB access, prepared statements, scan caps, stream iteration, `collect_page()` helper
  Must NOT: know about HTTP, import actix_web, validate query params
  Key pattern: `collect_page()` is a free function that handles overfetch+1 and scan-cap modes. All paginated methods return `(Vec<T>, bool, usize)` or `(Vec<T>, bool, bool, usize)` where the final `usize` is `dropped_rows`.

- **models.rs**
  Owns: all request/response structs, constants, `ApiError`, serde config
  Must NOT: contain business logic or DB access

- **queries.rs**
  Owns: dynamic CQL for prefix queries (the only place dynamic CQL is allowed)
  Must NOT: execute queries — it builds `Statement`s, scylladb.rs executes them

- **tree.rs**
  Owns: `build_tree()` — slash-delimited keys to nested JSON
  Must NOT: access DB or HTTP types

**Dependency direction:** handlers → models + scylladb + tree. Never the reverse. Handlers never import each other.

## Standard Handler Pattern

Every KV handler follows this sequence. Do not skip steps. Do not reorder.

1. Validate all params (`validate_account_id`, `validate_key`, `validate_limit`, cursor/offset conflict)
2. Log the request (`tracing::info!` with `target: PROJECT_ID`)
3. `require_db()` → get `RwLockReadGuard`
4. Call exactly one `scylladb` method
5. Build `PaginationMeta` from results
6. Return via `respond_paginated()` or `DataResponse`

Social handlers deviate: they construct `QueryParams`/`WritersParams` directly and call `scylladb` methods in loops (one per key pattern). They validate outer params themselves.

### Canonical KV Handler Skeleton

Struct fields use internal names with `#[serde(rename)]` to API params:
`predecessor_id` → `accountId`, `current_account_id` → `contractId`.
Cursor fields vary by endpoint: `after_key` (validated as key), `after_account` / `after_source` (validated as account ID).

```rust
pub async fn my_handler(
    query: web::Query<MyParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    // predecessor_id is #[serde(rename = "accountId")]
    validate_account_id(&query.predecessor_id, "accountId")?;
    // current_account_id is #[serde(rename = "contractId")]
    validate_account_id(&query.current_account_id, "contractId")?;
    validate_limit(query.limit)?;
    // Cursor field varies: after_key | after_account | after_source
    if let Some(ref cursor) = query.after_key {
        validate_key(cursor, "after_key", MAX_KEY_LENGTH)?;
        if query.offset > 0 {
            return Err(ApiError::InvalidParameter(
                "cannot use both 'after_key' cursor and 'offset'".to_string(),
            ));
        }
    } else {
        validate_offset(query.offset)?;
    }

    tracing::info!(target: PROJECT_ID, /* fields */, "GET /v1/kv/my-endpoint");

    let db = require_db(&app_state).await?;
    let (entries, has_more, dropped) = db.my_query(&query).await?;

    let next_cursor = entries.last().map(|e| e.key.clone());
    let meta = PaginationMeta { has_more, truncated: false, next_cursor, dropped_rows: dropped_to_option(dropped) };
    let fields = parse_field_set(&query.fields);
    let decode = should_decode(&query.value_format)?;
    Ok(respond_paginated(entries, meta, &fields, decode))
}
```

## Pagination Invariants

These are load-bearing. Breaking any one breaks client pagination.

1. **Overfetch by 1.** Query `limit + 1` rows. Got `limit + 1` back → `has_more = true`, then truncate to `limit`.
2. **Always emit `next_cursor`.** Set from the last item in the result, even when `has_more == false`. Clients use `next_cursor` as the resume token.
3. **`truncated` only from scan caps.** `truncated: true` only when a scan/dedup cap was hit (10k rows for history/timeline, 100k unique accounts for accounts-by-contract). Writers streams without a cap — `truncated` is always false there. Never set `truncated` from normal limit+1 pagination.
4. **`has_more` truthfulness.** Authoritative for cursor+limit endpoints. Best-effort when `truncated == true`. If `truncated` is true, treat `has_more` as unreliable; use `next_cursor` to resume.
5. **Cursor/offset mutual exclusion.** Both `after_*` and `offset > 0` → reject with HTTP 400.
6. **No pagination fields outside `meta`.** Never add `has_more`, `next_cursor`, or `truncated` to the top level.
7. **`dropped_rows` from deser errors.** `meta.dropped_rows` reports rows skipped due to deserialization failures. Omitted when zero (`Option<u32>`, `skip_serializing_if`). For social endpoints, use `X-Dropped-Rows` header instead (no `meta` envelope). Never expose error details — just the count.

**Client stop rule:**
Stop paginating when `meta.has_more == false && meta.truncated != true`.
If `meta.truncated == true`, results are best-effort; clients may continue
using `next_cursor` but completion is unknown.

## Response Envelopes

| Kind | Shape | Used by |
|---|---|---|
| Paginated list | `PaginatedResponse<T>` → `{ data: T[], meta }` | All KV list endpoints |
| Singleton | `DataResponse<T>` → `{ data: T }` | get, batch, diff, edges/count |
| Infra | Flat JSON, no envelope | /health, /v1/status |
| Social get/keys | Raw nested JSON (SocialDB compat) | /social/get, /social/keys |

Do not invent ad-hoc `serde_json::json!({...})` shapes for new endpoints. Use `PaginatedResponse<T>` or `DataResponse<T>`.

## Safety Rules

1. **All user-supplied identifiers must be validated** before any DB call — `validate_account_id()`, `validate_key()`, `validate_prefix()`.
2. **Never interpolate user input into CQL.** Table names are interpolated but validated at startup via `validate_identifier()`. User values go through bind parameters (`?`) only.
3. **Do not panic on DB disconnect.** `require_db()` returns `Err(DatabaseUnavailable)`. Never `.unwrap()` on DB access.
4. **Do not add `deny_unknown_fields`** to request structs — breaks forward compatibility.
5. **Error sanitization.** `From<anyhow::Error> for ApiError` logs full context, returns generic message. Never expose table names, IPs, or schema in client-facing errors.
6. **New configurable table names must call `validate_identifier()`** in `ScyllaDb::new()`.
7. **DB is `Arc<RwLock<Option<ScyllaDb>>>`** — server starts and runs without a DB connection. All handlers must call `require_db()` and return `DatabaseUnavailable` if `None`. Never hold the `RwLock` guard across `.await` — this deadlocks the server.

## Prepared Statements

- All new CQL must be prepared in `ScyllaDb::new()`. Single exception: prefix-bound queries in `queries.rs` (bounds vary per request).
- Only `queries.rs` may build dynamic CQL. This is the only place.
- Default consistency: `LocalOne`. Exceptions require justification (see `accounts_by_contract` for `LocalQuorum`).
- All statements get 10s request timeout via `set_request_timeout`.

## Hot Endpoints (Do Not Remove Safeguards)

- `/v1/kv/query` without `key_prefix` — full partition scan. **Prefer `key_prefix` to narrow.**
- `/v1/kv/timeline` — full partition scan + in-memory filter/sort, capped at 10k rows. **Narrow with `from_block`/`to_block`.**
- `/v1/kv/accounts` without `key` — full partition scan + HashSet dedup, capped at 100k unique. **Prefer `key` filter.**
- `/v1/kv/accounts` without `contractId` (`scan=1`) — reads `all_accounts` table (no dedup needed, TOKEN-based cursor). Throttled 1 req/sec/IP, limit clamped to 1,000. **Admin/dev only.**
- `/v1/kv/writers` — streams entire reverse table partition (unbounded, no scan cap). **Use cursor pagination with tight `limit`.**
- `/v1/kv/edges/count` — `COUNT(*)` scans entire partition. **No mitigation; avoid in hot loops.**

## Hard Limits (Do Not Change Casually)

| Constant | Value | Why it matters |
|---|---|---|
| `MAX_HISTORY_SCAN` | 10,000 | Row cap for history/timeline. Changing affects `truncated` truthfulness and query cost. |
| `MAX_DEDUP_SCAN` | 100,000 | Unique-value cap for `query_accounts_by_contract` dedup HashSet. Memory-bound. |
| `MAX_SOCIAL_RESULTS` | 1,000 | Per-pattern cap in social handlers. Controls `X-Results-Truncated`. |
| `MAX_OFFSET` | 100,000 | Hard ceiling on offset pagination. |
| `MAX_BATCH_KEYS` | 100 | Concurrent batch lookups (buffered 10 at a time). |
| `MAX_SCAN_LIMIT` | 1,000 | Max `limit` for `/v1/kv/accounts` without `contractId` (`scan=1`). |

Full constant list in models.rs:1–20.

## Testing Contract

- `cargo test` must pass (38 unit tests)
- `cargo clippy` must pass
- No ScyllaDB required — unit tests cover serde, validation, tree building, prefix computation
- Do not add integration tests without discussion (requires live DB)
- Tests live in `#[cfg(test)] mod tests` at the bottom of each module

## Anti-Patterns (Do NOT)

- **Do not add new pagination styles.** One style: `PaginationMeta` with `has_more`/`truncated`/`next_cursor`/`dropped_rows`.
- **Do not re-add `decode` param.** It was removed. Use `value_format=json|raw` only.
- **Do not push block filters into CQL for `/kv/timeline`.** The partition layout (`predecessor_id, current_account_id`) doesn't support it. In-memory filtering is intentional.
- **Do not add `offset` to `/kv/history`.** Pagination is via `from_block`/`to_block` narrowing. Adding offset would need scan cap interaction redesign.
- **Do not make `build_tree()` error on conflicts.** Leaf at "a/b" blocks nesting "a/b/c" — it skips silently. This is intentional.
- **Do not remove scan caps** from history/timeline/accounts. They prevent unbounded partition scans in prod.
- **Do not mix `rename` and `alias` conventions.** KV params use `#[serde(rename = "accountId")]` (one canonical form). Social params use `#[serde(alias = "accountId")]` (accept both for SocialDB compat).

## Pointers

- **REFERENCE.md** — Endpoint params, TS interfaces, env vars, cost ratings, prepared statement table
- **https://fastdata.up.railway.app/docs** — OpenAPI spec via Scalar UI (auto-generated from utoipa annotations in main.rs)
- **models.rs:1–18** — All constants
- **scylladb.rs `collect_page()`** — Reusable paginated stream helper (overfetch + scan-cap modes). 7 unit tests.
- **scylladb.rs:118–352** — ScyllaDb struct + all prepared statement initialization
- **main.rs** — `X-Indexer-Block` header middleware (cached `AtomicU64`, refreshed 5s); don't remove

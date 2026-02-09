# FastKV Server — Technical Reference

> Rust/Actix-web 4.5.1 read-only API for NEAR blockchain KV data stored in ScyllaDB (scylla 1.4).
> Deployed at `https://near.garden` — OpenAPI docs at `/docs` (Scalar UI).

## Endpoint Reference

19 endpoints: 10 KV + 7 Social + 2 System.

### Cost Legend

| Rating        | Meaning                                                           |
| ------------- | ----------------------------------------------------------------- |
| **Cheap**     | Single-partition PK lookup, bounded results                       |
| **Moderate**  | CK-range scan or PK + in-memory dedup with cap                    |
| **Risky**     | Full partition scan, in-memory filtering/sort, or unbounded dedup |
| **Expensive** | Full-partition aggregate (`COUNT(*)`)                             |

### System Endpoints

| Endpoint     | Method | Handler          | Cost  | Notes                                             |
| ------------ | ------ | ---------------- | ----- | ------------------------------------------------- |
| `/health`    | GET    | `health_check`   | Cheap | Returns `ok` / `degraded` (503 if DB unavailable) |
| `/v1/status` | GET    | `status_handler` | Cheap | `meta` table PK lookup for `indexer_block`        |

### KV Endpoints

| Endpoint             | Method | Handler               | Table                          | Cost           | CQL Pattern                                                                                                                                                                                  |
| -------------------- | ------ | --------------------- | ------------------------------ | -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/v1/kv/get`         | GET    | `get_kv_handler`      | `s_kv_last`                    | Cheap          | `WHERE predecessor_id=? AND current_account_id=? AND key=?`                                                                                                                                  |
| `/v1/kv/batch`       | POST   | `batch_kv_handler`    | `s_kv_last`                    | Cheap          | N parallel PK lookups (max 100, 10 concurrent)                                                                                                                                               |
| `/v1/kv/query`       | GET    | `query_kv_handler`    | `s_kv_last`                    | Moderate       | `WHERE ... AND key >= ? AND key < ?` (prefix). **Risky** without `key_prefix` (full partition)                                                                                               |
| `/v1/kv/history`     | GET    | `history_kv_handler`  | `s_kv`                         | Cheap          | `WHERE ... AND key=? AND block_height >= ? AND block_height <= ?` (CQL pushdown)                                                                                                             |
| `/v1/kv/writers`     | GET    | `writers_handler`     | `kv_reverse`                   | Moderate       | `WHERE current_account_id=? AND key=?` — streams partition (no dedup needed)                                                                                                                 |
| `/v1/kv/accounts`    | GET    | `accounts_handler`    | `kv_accounts` / `all_accounts` | Cheap/Risky    | Cheap with `key` param (PK+CK). **Risky** without `key` (full partition + 100k dedup). Without `contractId` (`scan=1`): reads `all_accounts` table with TOKEN cursor, throttled 1 req/sec/IP |
| `/v1/kv/diff`        | GET    | `diff_kv_handler`     | `s_kv`                         | Moderate       | 2 parallel PK+CK lookups at exact block heights                                                                                                                                              |
| `/v1/kv/timeline`    | GET    | `timeline_kv_handler` | `s_kv`                         | Risky          | `WHERE predecessor_id=? AND current_account_id=?` — full partition, in-memory block filter + sort, 10k row cap                                                                               |
| `/v1/kv/edges`       | GET    | `edges_handler`       | `kv_edges`                     | Moderate/Risky | Moderate with `after_source` cursor (`source > ?`). Risky without cursor (full partition + offset)                                                                                           |
| `/v1/kv/edges/count` | GET    | `edges_count_handler` | `kv_edges`                     | Expensive      | `SELECT COUNT(*) WHERE edge_type=? AND target=?` — scans entire partition                                                                                                                    |

**Response headers (all endpoints):**

- `X-Indexer-Block: <height>` — latest indexer block height, cached every 5s from `meta` table, added by middleware
- `X-Dropped-Rows: <n>` — number of rows skipped due to deserialization errors (only present when > 0; social endpoints only — KV endpoints use `meta.dropped_rows` instead)

### Social Endpoints

All social endpoints default to `social.near` contract (env: `SOCIAL_CONTRACT`); override with `contract_id` param.

| Endpoint                  | Method | Handler                       | Underlying Query                                                   | Cost     |
| ------------------------- | ------ | ----------------------------- | ------------------------------------------------------------------ | -------- |
| `/v1/social/get`          | POST   | `social_get_handler`          | Varies by key pattern (PK, prefix, or reverse view)                | Varies   |
| `/v1/social/keys`         | POST   | `social_keys_handler`         | Same as `/get` but returns structure/block heights                 | Varies   |
| `/v1/social/index`        | GET    | `social_index_handler`        | Reverse view (`mv_kv_cur_key`) for `index/{action}/{key}`          | Moderate |
| `/v1/social/profile`      | GET    | `social_profile_handler`      | Prefix query on `profile/*`                                        | Cheap    |
| `/v1/social/followers`    | GET    | `social_followers_handler`    | `accounts_by_key` on reverse view for `graph/follow/{accountId}`   | Moderate |
| `/v1/social/following`    | GET    | `social_following_handler`    | Prefix query on `s_kv_last` for `graph/follow/*`                   | Moderate |
| `/v1/social/feed/account` | GET    | `social_account_feed_handler` | History query (`get_kv_history`) on `post/main` (+ `post/comment`) | Moderate |

---

## Endpoint Details

### GET /health

No parameters.

```jsonc
// 200 (database field omitted when healthy)
{ "status": "ok" }
// 503
{ "status": "degraded", "database": "unavailable" }
```

### GET /v1/status

No parameters.

```jsonc
{ "indexer_block": 139000000, "timestamp": "2026-02-07T12:00:00Z" }
```

### GET /v1/kv/get

| Param          | Type   | Required | Notes                                       |
| -------------- | ------ | -------- | ------------------------------------------- |
| `accountId`    | string | yes      | Writer account (predecessor), max 256 chars |
| `contractId`   | string | yes      | Contract account, max 256 chars             |
| `key`          | string | yes      | KV key, max 10,000 chars                    |
| `fields`       | string | no       | Comma-separated field filter                |
| `value_format` | string | no       | `"raw"` (default) or `"json"` (decoded)     |

Returns `DataResponse<KvEntry | null>`.

### GET /v1/kv/query

| Param          | Type   | Required | Default | Notes                                                                                           |
| -------------- | ------ | -------- | ------- | ----------------------------------------------------------------------------------------------- |
| `accountId`    | string | yes      |         | Writer account                                                                                  |
| `contractId`   | string | yes      |         | Contract account                                                                                |
| `key_prefix`   | string | no       |         | Key prefix filter, max 1,000 chars. **Omitting scans entire partition.**                        |
| `exclude_null` | bool   | no       | false   | Filter out null values                                                                          |
| `limit`        | int    | no       | 100     | Range 1–1000                                                                                    |
| `offset`       | int    | no       | 0       | Max 100,000. Applied in-memory after fetch.                                                     |
| `fields`       | string | no       |         | Comma-separated field filter                                                                    |
| `format`       | string | no       |         | `"tree"` for nested JSON (`TreeResponse`)                                                       |
| `value_format` | string | no       | `"raw"` | `"raw"` or `"json"` (decoded)                                                                   |
| `after_key`    | string | no       |         | Cursor: return entries with key after this value (exclusive). Cannot combine with `offset > 0`. |

Returns `PaginatedResponse<KvEntry>` or `TreeResponse` (if `format=tree`).

### GET /v1/kv/history

| Param          | Type   | Required | Default  | Notes                                         |
| -------------- | ------ | -------- | -------- | --------------------------------------------- |
| `accountId`    | string | yes      |          | Writer account                                |
| `contractId`   | string | yes      |          | Contract account                              |
| `key`          | string | yes      |          | KV key                                        |
| `limit`        | int    | no       | 100      | Range 1–1000                                  |
| `order`        | string | no       | `"desc"` | `"asc"` or `"desc"`                           |
| `from_block`   | int    | no       |          | Min block height (CQL pushdown, must be >= 0) |
| `to_block`     | int    | no       |          | Max block height (CQL pushdown, must be >= 0) |
| `fields`       | string | no       |          | Comma-separated field filter                  |
| `value_format` | string | no       | `"raw"`  | `"raw"` or `"json"` (decoded)                 |

Returns `PaginatedResponse<KvEntry>`. `meta.truncated: true` if scan hit 10,000 rows.
Paginate using `from_block`/`to_block` range narrowing + `limit`.

### GET /v1/kv/writers

| Param           | Type   | Required | Default | Notes                                                                                    |
| --------------- | ------ | -------- | ------- | ---------------------------------------------------------------------------------------- |
| `contractId`    | string | yes      |         | Contract account                                                                         |
| `key`           | string | yes      |         | KV key, max 10,000 chars                                                                 |
| `accountId`     | string | no       |         | Filter to specific writer                                                                |
| `exclude_null`  | bool   | no       | false   | Filter out null values                                                                   |
| `limit`         | int    | no       | 100     | Range 1–1000                                                                             |
| `offset`        | int    | no       | 0       | Max 100,000. Applied in-memory.                                                          |
| `fields`        | string | no       |         | Comma-separated field filter                                                             |
| `value_format`  | string | no       | `"raw"` | `"raw"` or `"json"` (decoded)                                                            |
| `after_account` | string | no       |         | Cursor: return writers after this account (exclusive). Cannot combine with `offset > 0`. |

Returns `PaginatedResponse<KvEntry>`. Reads from `kv_reverse` table where rows are naturally unique per `predecessor_id` (no dedup needed). `meta.truncated` is always `false`.

### POST /v1/kv/batch

Request body:

```jsonc
{
  "accountId": "alice.near",
  "contractId": "social.near",
  "keys": ["key1", "key2"], // max 100 items, each ≤1024 chars
}
```

Returns `DataResponse<BatchResultItem[]>`.

### GET /v1/kv/diff

| Param            | Type   | Required | Notes                         |
| ---------------- | ------ | -------- | ----------------------------- |
| `accountId`      | string | yes      | Writer account                |
| `contractId`     | string | yes      | Contract account              |
| `key`            | string | yes      | KV key                        |
| `block_height_a` | int    | yes      | First block height            |
| `block_height_b` | int    | yes      | Second block height           |
| `fields`         | string | no       | Comma-separated field filter  |
| `value_format`   | string | no       | `"raw"` or `"json"` (decoded) |

Returns `DataResponse<DiffResponse>`.

### GET /v1/kv/timeline

| Param          | Type   | Required | Default  | Notes                                            |
| -------------- | ------ | -------- | -------- | ------------------------------------------------ |
| `accountId`    | string | yes      |          | Writer account                                   |
| `contractId`   | string | yes      |          | Contract account                                 |
| `limit`        | int    | no       | 100      | Range 1–1000                                     |
| `offset`       | int    | no       | 0        | Max 100,000                                      |
| `order`        | string | no       | `"desc"` | `"asc"` or `"desc"`                              |
| `from_block`   | int    | no       |          | Min block height (**in-memory filter, not CQL**) |
| `to_block`     | int    | no       |          | Max block height (**in-memory filter, not CQL**) |
| `fields`       | string | no       |          | Comma-separated field filter                     |
| `value_format` | string | no       | `"raw"`  | `"raw"` or `"json"` (decoded)                    |

Returns `PaginatedResponse<KvEntry>`. `meta.truncated: true` if scan hit 10,000 rows.

### GET /v1/kv/accounts

| Param           | Type   | Required | Default | Notes                                                                                                                   |
| --------------- | ------ | -------- | ------- | ----------------------------------------------------------------------------------------------------------------------- |
| `contractId`    | string | no       |         | Contract account, max 256 chars. When omitted, requires `scan=1`.                                                       |
| `scan`          | int    | no       |         | Set to `1` to opt-in to full table scan when `contractId` is omitted.                                                   |
| `key`           | string | no       |         | Key filter. **Recommended for large contracts** — omitting triggers full partition scan + dedup. Requires `contractId`. |
| `limit`         | int    | no       | 100     | Range 1–1000. Clamped to 1,000 max when `contractId` is omitted.                                                        |
| `offset`        | int    | no       | 0       | Max 100,000. Not available when `contractId` is omitted.                                                                |
| `after_account` | string | no       |         | Cursor: return accounts after this value (exclusive). Cannot combine with `offset > 0`.                                 |

Returns `PaginatedResponse<String>` (list of account IDs). `meta.truncated: true` if dedup scan hit 100,000.

> **Scan mode** (`contractId` omitted, `scan=1`): queries the dedicated `all_accounts` table (one row per unique account). Pagination is **token-ordered** (Murmur3 hash order), not alphabetical — results appear in a stable but non-lexicographic order. Pass the last returned account ID as `after_account` to resume. Throttled to 1 req/sec per IP (429 if exceeded). Intended for admin/dev use.
>
> **Required table:** `all_accounts` (`predecessor_id text PRIMARY KEY`). Override name via `ALL_ACCOUNTS_TABLE_NAME` env var.

### GET /v1/kv/edges

| Param          | Type   | Required | Default | Notes                                                                                 |
| -------------- | ------ | -------- | ------- | ------------------------------------------------------------------------------------- |
| `edge_type`    | string | yes      |         | Edge type, max 256 chars                                                              |
| `target`       | string | yes      |         | Target account, max 256 chars                                                         |
| `limit`        | int    | no       | 100     | Range 1–1000                                                                          |
| `offset`       | int    | no       | 0       | Max 100,000. Cannot combine with `after_source`.                                      |
| `after_source` | string | no       |         | Cursor: return sources alphabetically after this value (CQL `source > ?`). Exclusive. |

Returns `PaginatedResponse<EdgeSourceEntry>`.

### GET /v1/kv/edges/count

| Param       | Type   | Required | Notes          |
| ----------- | ------ | -------- | -------------- |
| `edge_type` | string | yes      | Edge type      |
| `target`    | string | yes      | Target account |

Returns `DataResponse<EdgesCountResponse>`.

### POST /v1/social/get

Request body:

```jsonc
{
  "keys": ["alice.near/profile/**"], // max 100 patterns
  "contract_id": "social.near", // optional, default: SOCIAL_CONTRACT env
  "options": {
    "with_block_height": false, // include block heights in response
    "return_deleted": false, // include deleted (null) entries
  },
}
```

Returns nested JSON structure. Sets `X-Results-Truncated: true` header if any pattern hit 1,000-result limit.

**Key pattern types:**

- `alice.near/profile/name` — exact key lookup
- `alice.near/profile/**` — recursive wildcard (all keys under prefix)
- `alice.near/profile/*` — single-level wildcard (one depth only)
- `alice.near` — bare account (all keys, recursive)
- `*/widget/name` — wildcard account (all writers for key, uses reverse view)

### POST /v1/social/keys

Same request body as `/social/get` but with different options:

```jsonc
{
  "keys": ["alice.near/profile/*"],
  "options": {
    "return_type": "True", // "True" → values are `true`, "BlockHeight" → values are block heights
    "return_deleted": false,
    "values_only": false, // filter out parent directory keys (O(n) algorithm)
  },
}
```

Returns nested JSON structure. Sets `X-Results-Truncated: true` header if truncated.

### GET /v1/social/index

| Param         | Type      | Required | Default  | Notes                                                                          |
| ------------- | --------- | -------- | -------- | ------------------------------------------------------------------------------ |
| `action`      | string    | yes      |          | Action type (e.g., `"post"`, `"like"`)                                         |
| `key`         | string    | yes      |          | Index key                                                                      |
| `order`       | string    | no       | `"desc"` | `"asc"` or `"desc"`                                                            |
| `limit`       | int       | no       | 100      | Range 1–1000                                                                   |
| `from`        | int (u64) | no       |          | Exclusive cursor (desc: skips entries >= `from`; asc: skips entries <= `from`) |
| `account_id`  | string    | no       |          | Filter to account. Also accepts `accountId`.                                   |
| `contract_id` | string    | no       |          | Override default contract                                                      |

Returns `{ "entries": [IndexEntry, ...] }`.

Uses a 30-second stream timeout. Aborts after 10 deserialization errors (`MAX_STREAM_ERRORS`). Sets `X-Dropped-Rows: <n>` header when deserialization errors occur.

### GET /v1/social/profile

| Param         | Type   | Required | Notes                     |
| ------------- | ------ | -------- | ------------------------- |
| `account_id`  | string | yes      | Also accepts `accountId`  |
| `contract_id` | string | no       | Override default contract |

Returns nested JSON tree of profile data (not wrapped in `PaginatedResponse`).

### GET /v1/social/followers

| Param           | Type   | Required | Default | Notes                                                                                   |
| --------------- | ------ | -------- | ------- | --------------------------------------------------------------------------------------- |
| `account_id`    | string | yes      |         | Also accepts `accountId`                                                                |
| `limit`         | int    | no       | 100     | Range 1–1000                                                                            |
| `offset`        | int    | no       | 0       | Max 100,000                                                                             |
| `contract_id`   | string | no       |         | Override default contract                                                               |
| `after_account` | string | no       |         | Cursor: return accounts after this value (exclusive). Cannot combine with `offset > 0`. |

Returns `SocialFollowResponse` (`{ accounts, count, meta }`).

### GET /v1/social/following

Same params and response shape as `/followers` (includes `after_account` cursor).

### GET /v1/social/feed/account

| Param             | Type      | Required | Default  | Notes                             |
| ----------------- | --------- | -------- | -------- | --------------------------------- |
| `account_id`      | string    | yes      |          | Also accepts `accountId`          |
| `order`           | string    | no       | `"desc"` | `"asc"` or `"desc"`               |
| `limit`           | int       | no       | 100      | Range 1–1000                      |
| `from`            | int (u64) | no       |          | Block height cursor               |
| `include_replies` | bool      | no       | false    | Also fetch `post/comment` entries |
| `contract_id`     | string    | no       |          | Override default contract         |

Returns `{ "posts": [IndexEntry, ...] }`. Uses history query with CQL block-height pushdown (not timeline). When `include_replies=true`, makes two parallel history queries and merges.

---

## Pagination Contract

All paginated endpoints return `PaginatedResponse<T>` with a `meta` object:

```jsonc
{
  "data": [ ... ],
  "meta": {
    "has_more": true,
    "truncated": true,       // omitted when false
    "next_cursor": "last_key", // omitted when no items returned
    "dropped_rows": 2        // omitted when zero — rows skipped due to deserialization errors
  }
}
```

**`meta.has_more`** — Authoritative for cursor+limit endpoints (query, writers, edges, followers, following) which use the limit+1 overfetch pattern. Best-effort for scan-limited endpoints (history, timeline, accounts) where a scan cap may prevent full enumeration.

**`meta.next_cursor`** — Always set when items are returned, regardless of `has_more`. Use as the resume point for the next page via the corresponding `after_*` parameter.

**`meta.truncated`** — True only when a scan/dedup cap was hit: 10,000 rows for history/timeline, 100,000 unique values for accounts. Omitted when false (`default: false` in OpenAPI schema). When true, `has_more` may be inaccurate — treat completion as unknown.

**`meta.dropped_rows`** — Number of rows skipped due to deserialization errors. Omitted when zero. Nonzero means the results are complete for the requested page but some rows in the underlying data could not be read. This is a data-quality signal, not a pagination issue — clients do not need to retry. For social endpoints (which lack `meta`), the same information is conveyed via the `X-Dropped-Rows` response header.

**Cursor/offset exclusivity** — All endpoints reject `after_*` cursor combined with `offset > 0` (HTTP 400).

**Client rule** — Stop paginating when `meta.has_more == false` and `meta.truncated != true`. If `truncated` is true, the client may continue via `next_cursor` but should treat the dataset as potentially incomplete.

**History/timeline pagination** — These endpoints have no `after_*` cursor. Paginate by narrowing the block range:

- Descending: set `to_block` to `last_returned_block_height - 1` for the next page
- Ascending: set `from_block` to `last_returned_block_height + 1` for the next page

---

## TypeScript Interfaces

```typescript
// ── Response Envelope ───────────────────────────────────────────

interface PaginationMeta {
  has_more: boolean;
  truncated?: boolean; // omitted when false (default: false)
  next_cursor?: string; // omitted when no items returned
  dropped_rows?: number; // omitted when zero — rows skipped due to deserialization errors
}

interface PaginatedResponse<T> {
  data: T[];
  meta: PaginationMeta;
}

interface DataResponse<T> {
  data: T;
}

// ── Domain Types ────────────────────────────────────────────────

interface KvEntry {
  accountId: string;
  contractId: string;
  key: string;
  value: string;
  block_height: number;
  block_timestamp: number;
  receipt_id: string;
  tx_hash: string;
  is_deleted?: boolean; // omitted when false
}

interface HealthResponse {
  status: string;
  database?: string;
}

interface StatusResponse {
  indexer_block?: number;
  timestamp: string;
}

interface TreeResponse {
  tree: Record<string, any>;
}

interface DiffResponse {
  a?: KvEntry;
  b?: KvEntry;
}

interface BatchResultItem {
  key: string;
  value?: string;
  found: boolean;
  error?: string;
}

interface EdgeSourceEntry {
  source: string;
  block_height: number;
}

interface EdgesCountResponse {
  edge_type: string;
  target: string;
  count: number;
}

interface IndexEntry {
  accountId: string;
  blockHeight: number;
  value?: any;
}

interface IndexResponse {
  entries: IndexEntry[];
}

interface SocialFollowResponse {
  accounts: string[];
  count: number;
  meta: PaginationMeta;
}

interface SocialFeedResponse {
  posts: IndexEntry[];
}

// ── Request / Query Param Types ─────────────────────────────────

interface GetParams {
  accountId: string;
  contractId: string;
  key: string;
  fields?: string;
  value_format?: "raw" | "json";
}

interface QueryParams {
  accountId: string;
  contractId: string;
  key_prefix?: string;
  exclude_null?: boolean;
  limit?: number; // default 100, max 1000
  offset?: number; // default 0, max 100_000
  fields?: string;
  format?: "tree";
  value_format?: "raw" | "json";
  after_key?: string; // cursor, cannot combine with offset > 0
}

interface HistoryParams {
  accountId: string;
  contractId: string;
  key: string;
  limit?: number; // default 100, max 1000
  order?: "asc" | "desc"; // default "desc"
  from_block?: number;
  to_block?: number;
  fields?: string;
  value_format?: "raw" | "json";
}

interface WritersParams {
  contractId: string;
  key: string;
  accountId?: string;
  exclude_null?: boolean;
  limit?: number;
  offset?: number;
  fields?: string;
  value_format?: "raw" | "json";
  after_account?: string; // cursor, cannot combine with offset > 0
}

interface AccountsQueryParams {
  contractId?: string; // optional; requires scan=1 when omitted
  scan?: number; // 1 to opt-in to full table scan when contractId omitted
  key?: string; // requires contractId
  limit?: number; // clamped to 1,000 when contractId omitted
  offset?: number; // not available when contractId omitted
  after_account?: string; // cursor, cannot combine with offset > 0
}

interface DiffParams {
  accountId: string;
  contractId: string;
  key: string;
  block_height_a: number;
  block_height_b: number;
  fields?: string;
  value_format?: "raw" | "json";
}

interface TimelineParams {
  accountId: string;
  contractId: string;
  limit?: number;
  offset?: number;
  order?: "asc" | "desc";
  from_block?: number;
  to_block?: number;
  fields?: string;
  value_format?: "raw" | "json";
}

interface EdgesParams {
  edge_type: string;
  target: string;
  limit?: number;
  offset?: number;
  after_source?: string; // cursor, cannot combine with offset > 0
}

interface EdgesCountParams {
  edge_type: string;
  target: string;
}

interface BatchQuery {
  accountId: string;
  contractId: string;
  keys: string[]; // max 100 items, each ≤1024 chars
}

interface SocialGetBody {
  keys: string[]; // max 100 patterns
  contract_id?: string; // also accepts contractId
  options?: {
    with_block_height?: boolean;
    return_deleted?: boolean;
  };
}

interface SocialKeysBody {
  keys: string[];
  contract_id?: string; // also accepts contractId
  options?: {
    return_type?: "True" | "BlockHeight";
    return_deleted?: boolean;
    values_only?: boolean;
  };
}

interface SocialIndexParams {
  action: string;
  key: string;
  order?: "asc" | "desc";
  limit?: number;
  from?: number; // block height cursor
  account_id?: string; // also accepts accountId
  contract_id?: string; // also accepts contractId
}

interface SocialProfileParams {
  account_id: string; // also accepts accountId
  contract_id?: string; // also accepts contractId
}

interface SocialFollowParams {
  account_id: string; // also accepts accountId
  limit?: number;
  offset?: number;
  contract_id?: string; // also accepts contractId
  after_account?: string; // cursor, cannot combine with offset > 0
}

interface SocialAccountFeedParams {
  account_id: string; // also accepts accountId
  order?: "asc" | "desc";
  limit?: number;
  from?: number;
  include_replies?: boolean;
  contract_id?: string; // also accepts contractId
}
```

---

## Environment Variables

### Required

| Variable          | Description                                                  |
| ----------------- | ------------------------------------------------------------ |
| `CHAIN_ID`        | NEAR chain ID (validated via `fastnear_primitives::ChainId`) |
| `SCYLLA_URL`      | ScyllaDB node URL                                            |
| `SCYLLA_USERNAME` | Database username                                            |
| `SCYLLA_PASSWORD` | Database password                                            |

### Optional

| Variable                     | Default               | Description                                                                  |
| ---------------------------- | --------------------- | ---------------------------------------------------------------------------- |
| `KEYSPACE`                   | `fastdata_{CHAIN_ID}` | ScyllaDB keyspace (alphanumeric + underscore only)                           |
| `TABLE_NAME`                 | `s_kv_last`           | Current-state table                                                          |
| `HISTORY_TABLE_NAME`         | `s_kv`                | History table (with block_height clustering)                                 |
| `REVERSE_VIEW_NAME`          | `mv_kv_cur_key`       | Materialized view for reverse lookups                                        |
| `KV_ACCOUNTS_TABLE_NAME`     | `kv_accounts`         | Contract-to-writer mappings                                                  |
| `ALL_ACCOUNTS_TABLE_NAME`    | `all_accounts`        | Unique accounts table (`predecessor_id text PRIMARY KEY`). Used by `scan=1`. |
| `KV_EDGES_TABLE_NAME`        | `kv_edges`            | Reverse edge lookup table                                                    |
| `KV_REVERSE_TABLE_NAME`      | `kv_reverse`          | Reverse lookup by (contract, key) → writers                                  |
| `PORT`                       | `3001`                | Server listen port                                                           |
| `DB_RECONNECT_INTERVAL_SECS` | `5`                   | Background reconnection interval (5–300s, exponential backoff)               |
| `SOCIAL_CONTRACT`            | `social.near`         | Default contract for social API endpoints                                    |
| `SCYLLA_SSL_CA`              | —                     | Path to CA certificate PEM (enables TLS)                                     |
| `SCYLLA_SSL_CERT`            | —                     | Path to client certificate (mTLS)                                            |
| `SCYLLA_SSL_KEY`             | —                     | Path to client key (mTLS)                                                    |

---

## Table Schemas (ScyllaDB)

```
s_kv_last       PRIMARY KEY ((predecessor_id), current_account_id, key)
                → value, block_height, block_timestamp, receipt_id, tx_hash
                Current-state table. One row per (writer, contract, key).

s_kv            PRIMARY KEY ((predecessor_id), current_account_id, key, block_height, order_id)
                → value, block_timestamp, receipt_id, tx_hash, signer_id, shard_id, receipt_index, action_index
                History table. One row per write event.

mv_kv_cur_key   Materialized view on s_kv_last
                PRIMARY KEY ((current_account_id, key), predecessor_id)
                Reverse lookup: find all writers for a given (contract, key).

kv_accounts     PRIMARY KEY ((current_account_id), key, predecessor_id)
                Contract-to-writer mapping. Populated asynchronously (reads use LocalQuorum).

all_accounts    PRIMARY KEY (predecessor_id)
                One row per unique account. Used by scan=1 on /v1/kv/accounts. Populated by indexer.

kv_edges        PRIMARY KEY ((edge_type, target), source)
                → block_height
                Reverse edge lookup: find all sources for an (edge_type, target).

kv_reverse      PRIMARY KEY ((current_account_id, key), predecessor_id)
                → value, block_height, block_timestamp, receipt_id, tx_hash
                Reverse lookup: find all writers for a (contract, key). Used by /kv/writers,
                /social/followers (via social handlers).
```

---

## Internal Constants

| Constant                | Value   | Location    | Purpose                                          |
| ----------------------- | ------- | ----------- | ------------------------------------------------ |
| `MAX_OFFSET`            | 100,000 | `models.rs` | Max `offset` param for any endpoint              |
| `MAX_ACCOUNT_ID_LENGTH` | 256     | `models.rs` | Max chars for account/contract IDs               |
| `MAX_KEY_LENGTH`        | 10,000  | `models.rs` | Max chars for KV keys                            |
| `MAX_PREFIX_LENGTH`     | 1,000   | `models.rs` | Max chars for key_prefix param                   |
| `MAX_BATCH_KEYS`        | 100     | `models.rs` | Max keys in batch request                        |
| `MAX_BATCH_KEY_LENGTH`  | 1,024   | `models.rs` | Max chars per key in batch                       |
| `MAX_SOCIAL_RESULTS`    | 1,000   | `models.rs` | Per-pattern result cap for social endpoints      |
| `MAX_SOCIAL_KEYS`       | 100     | `models.rs` | Max patterns per social request                  |
| `MAX_STREAM_ERRORS`     | 10      | `models.rs` | Deserialization error cap before aborting stream |
| `MAX_HISTORY_SCAN`      | 10,000  | `models.rs` | Row cap for history/timeline scans               |
| `MAX_DEDUP_SCAN`        | 100,000 | `models.rs` | Unique-value cap for dedup scans                 |
| `MAX_EDGE_TYPE_LENGTH`  | 256     | `models.rs` | Max chars for edge_type param                    |

---

## Prepared Statements

20 statements prepared at startup. All use `LocalOne` consistency and 10s timeout unless noted.

| Name                       | Table           | CQL Summary                                          | Used By                                          |
| -------------------------- | --------------- | ---------------------------------------------------- | ------------------------------------------------ |
| `get_kv`                   | `s_kv_last`     | PK lookup (3-col)                                    | `/kv/get`                                        |
| `get_kv_last`              | `s_kv_last`     | Value-only PK lookup                                 | `/kv/batch`                                      |
| `query_kv_no_prefix`       | `s_kv_last`     | Full partition (2-col PK)                            | `/kv/query` (no prefix)                          |
| `query_kv_cursor`          | `s_kv_last`     | `key > ?` (cursor, no prefix)                        | `/kv/query` (cursor, no prefix)                  |
| `prefix_query`             | `s_kv_last`     | `key >= ? AND key < ?`                               | `/kv/query` (prefix, no cursor)                  |
| `prefix_cursor_query`      | `s_kv_last`     | `key > ? AND key < ?`                                | `/kv/query` (prefix + cursor)                    |
| `reverse_kv`               | `mv_kv_cur_key` | PK + ORDER BY DESC                                   | social index, social get/keys (wildcard account) |
| `reverse_list`             | `kv_reverse`    | Full partition (2-col PK)                            | `/kv/writers` (no cursor)                        |
| `reverse_list_cursor`      | `kv_reverse`    | PK + `predecessor_id > ?`                            | `/kv/writers` (with cursor)                      |
| `get_kv_history`           | `s_kv`          | PK + `block_height >= ? AND <= ?`                    | `/kv/history`, `/social/feed/account`            |
| `get_kv_at_block`          | `s_kv`          | PK + exact block                                     | `/kv/diff`                                       |
| `get_kv_timeline`          | `s_kv`          | Full partition (2-col PK)                            | `/kv/timeline`                                   |
| `accounts_by_contract`     | `kv_accounts`   | Full partition (**LocalQuorum**)                     | `/kv/accounts` (no key)                          |
| `accounts_by_contract_key` | `kv_accounts`   | PK+CK lookup (**LocalQuorum**)                       | `/kv/accounts` (with key)                        |
| `accounts_all`             | `all_accounts`  | Full table scan (**LocalQuorum**)                    | `/kv/accounts` (scan=1, no cursor)               |
| `accounts_all_cursor`      | `all_accounts`  | `TOKEN(predecessor_id) > TOKEN(?)` (**LocalQuorum**) | `/kv/accounts` (scan=1, with cursor)             |
| `edges_list`               | `kv_edges`      | Full partition                                       | `/kv/edges` (no cursor)                          |
| `edges_list_cursor`        | `kv_edges`      | PK + `source > ?`                                    | `/kv/edges` (with cursor)                        |
| `edges_count`              | `kv_edges`      | `COUNT(*)` full partition                            | `/kv/edges/count`                                |
| `meta_query`               | `meta`          | Single-row PK lookup                                 | `/v1/status`                                     |

---

## Open Issues

### Bugs / Missing Features

1. **`/v1/kv/history` has no `offset` parameter.** `HistoryParams` lacks the field entirely. Users must paginate via `from_block`/`to_block` block-range narrowing. This is undocumented and may surprise API consumers.

2. **`/v1/kv/timeline` does not push `from_block`/`to_block` to CQL.** The `get_kv_timeline` prepared statement has no block-height WHERE clause — filtering happens in-memory after a full partition scan. For active accounts, this scans up to 10,000 rows regardless of the block range requested.

### Performance Risks

| Endpoint                                    | Risk                                            | Trigger                       | Mitigation                                 |
| ------------------------------------------- | ----------------------------------------------- | ----------------------------- | ------------------------------------------ |
| `/v1/kv/query`                              | Full partition scan                             | Missing `key_prefix`          | Always provide `key_prefix`                |
| `/v1/kv/timeline`                           | Full partition + in-memory filter/sort          | Any call                      | Capped at 10,000 rows (`truncated: true`)  |
| `/v1/kv/accounts` (contract)                | Full partition + 100k dedup                     | Missing `key` param           | Always provide `key`                       |
| `/v1/kv/accounts` (scan)                    | Full table TOKEN scan                           | `scan=1` without `contractId` | Throttled 1 req/sec per IP, max 1000 rows  |
| `/v1/kv/edges`                              | Full partition + offset                         | Missing `after_source` cursor | Use cursor-based pagination                |
| `/v1/kv/edges/count`                        | Full partition `COUNT(*)`                       | Any call                      | No mitigation; consider caching            |
| `/v1/kv/writers`                            | Full partition stream                           | Popular keys (many writers)   | Use cursor pagination with tight `limit`   |
| `/v1/social/feed/account`                   | Two history queries when `include_replies=true` | `include_replies=true`        | Still bounded by CQL block-height pushdown |
| `/v1/social/get` (wildcard account `*/key`) | Reverse view full scan                          | Wildcard account pattern      | Limit patterns per request (max 100)       |

### Verified Working

- **Serde renames**: `accountId`/`contractId` in both request params and response JSON
- **`PaginatedResponse<T>`**: `truncated` field omitted when false (`skip_serializing_if`)
- **`X-Results-Truncated` header**: Set by `/social/get` and `/social/keys`, exposed via CORS
- **`X-Indexer-Block` header**: Added to every response by middleware, cached from `meta` table every 5s, exposed via CORS
- **`meta.dropped_rows`**: Omitted when zero, present as integer when deserialization errors occur on KV endpoints
- **`X-Dropped-Rows` header**: Set on social endpoints when deserialization errors occur (same semantics, header form)
- **ORDER BY DESC dedup**: First occurrence kept = newest entry (accounts-by-contract)
- **`MAX_STREAM_ERRORS = 10`**: Defined in `models.rs:15`, used in `social_handlers.rs:165`
- **Social handler validation parity**: `validate_offset()` applied to followers/following
- **`validate_identifier()`**: Prevents CQL injection on all 7 table names + keyspace
- **Error sanitization**: Generic client messages, full context in server logs
- **DB resilience**: Optional connection with exponential backoff reconnection (5–300s)
- **Prefix queries prepared at startup**: `prefix_query` and `prefix_cursor_query` are prepared statements (no per-request parsing overhead)

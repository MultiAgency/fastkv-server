# FastKV Server

API for querying NEAR blockchain FastData key-value stores (`__fastdata_kv`) stored in ScyllaDB.

**Live:** https://near.garden

## Supported Use Cases

- **`__fastdata_kv`** - Simple key-value storage with the full key as `(predecessor_id, current_account_id, key)`
- **SocialDB** - Tree-structured KV store for social graphs (follows, profiles, posts, etc.)
- Both use the same schema and query patterns, just with hierarchical keys for SocialDB (e.g., `graph/follow/alice.near`)

## Features

- **Latest Values** - Query most recent key-value data from `s_kv_last` table
- **Full History** - Access complete value history from `s_kv` table
- **Field Selection** - Request only specific fields to reduce bandwidth (e.g., `fields=key,value`)
- **Prefix Queries** - Efficient tree traversal for SocialDB-style hierarchical data
- **Reverse Lookups** - Find all accounts with a specific key
- **Diff** - Compare a key's value at two different block heights
- **Timeline** - All writes by one account across all keys

## Endpoints

### Health Check

```
GET /health
```

Verifies database connectivity and returns the current status.

**Responses:**

- **200 OK** - Database is reachable:

  ```json
  { "status": "ok" }
  ```

- **503 Service Unavailable** - Database connection failed:
  ```json
  { "status": "database_unavailable" }
  ```

### API Documentation

Interactive OpenAPI 3.0 documentation available at [https://near.garden/docs](https://near.garden/docs)

### Get Single Entry

```
GET /v1/kv/get?predecessor_id={account}&current_account_id={contract}&key={key}
```

| Parameter            | Description                                                            |
| -------------------- | ---------------------------------------------------------------------- |
| `predecessor_id`     | The account that wrote the data                                        |
| `current_account_id` | The contract called (e.g., `social.near`)                              |
| `key`                | The specific key with which to retrieve its respective value           |
| `fields`             | Optional: Comma-separated list of fields to return (e.g., `key,value`) |

Returns a single entry object if found, or `null` if not found.

**Field Selection Example:**

```bash
curl "https://near.garden/v1/kv/get?predecessor_id=james.near&current_account_id=social.near&key=profile/name&fields=key,value"
```

**Response with field selection:**

```json
{
  "key": "profile/name",
  "value": "\"James\""
}
```

**Available fields:** `predecessor_id`, `current_account_id`, `key`, `value`, `block_height`, `block_timestamp`, `receipt_id`, `tx_hash`

### Get History

```
GET /v1/kv/history?predecessor_id={account}&current_account_id={contract}&key={key}&limit={n}&order={asc|desc}
```

Retrieves the full history of values for a specific key from the `s_kv` table.

| Parameter            | Description                                                                     |
| -------------------- | ------------------------------------------------------------------------------- |
| `predecessor_id`     | The account that wrote the data                                                 |
| `current_account_id` | The contract called (e.g., `social.near`)                                       |
| `key`                | The specific key to retrieve history for                                        |
| `limit`              | Max results to return (default: `100`, range: `1-1000`)                         |
| `order`              | Sort order by block height: `asc` or `desc` (default: `desc`)                   |
| `from_block`         | Optional: Filter entries with block_height >= this value                        |
| `to_block`           | Optional: Filter entries with block_height <= this value                        |
| `fields`             | Optional: Comma-separated list of fields to return (e.g., `value,block_height`) |

Returns an array of entries showing how the value changed over time, with the most recent value first (when `order=desc`).

**Example - Get profile name history:**

```bash
curl "https://near.garden/v1/kv/history?predecessor_id=james.near&current_account_id=social.near&key=profile/name&limit=10"
```

**Response:**

```json
{
  "entries": [
    {
      "predecessor_id": "james.near",
      "current_account_id": "social.near",
      "key": "profile/name",
      "value": "\"James v3\"",
      "block_height": 183500000,
      "block_timestamp": 1769900000000000000,
      "receipt_id": "ghi...",
      "tx_hash": "rst..."
    },
    {
      "predecessor_id": "james.near",
      "current_account_id": "social.near",
      "key": "profile/name",
      "value": "\"James v2\"",
      "block_height": 183400000,
      "block_timestamp": 1769800000000000000,
      "receipt_id": "abc...",
      "tx_hash": "xyz..."
    },
    {
      "predecessor_id": "james.near",
      "current_account_id": "social.near",
      "key": "profile/name",
      "value": "\"James\"",
      "block_height": 183300000,
      "block_timestamp": 1769700000000000000,
      "receipt_id": "def...",
      "tx_hash": "uvw..."
    }
  ]
}
```

**Use Cases:**

- Track how a profile evolved over time
- Audit trail for social graph changes
- Time-series analysis of on-chain data

### All Endpoints

Full interactive documentation for all 16 endpoints (8 KV + health + 7 Social) is available at [/docs](https://near.garden/docs). The most common endpoints are documented below.

### Query Multiple Entries

```
GET /v1/kv/query?predecessor_id={account}&current_account_id={contract}&key_prefix={prefix}&exclude_null={bool}
```

| Parameter            | Description                                                                                     |
| -------------------- | ----------------------------------------------------------------------------------------------- |
| `predecessor_id`     | The account that wrote the data                                                                 |
| `current_account_id` | The contract called                                                                             |
| `key_prefix`         | Optional prefix filter (e.g., `graph/follow/`). Cannot be empty string - omit if not filtering. |
| `exclude_null`       | If `true`, excludes entries where value is JSON `null` (default: `false`)                       |
| `limit`              | Max results after filtering (default: `100`, range: `1-1000`)                                   |
| `offset`             | Skip N results after filtering (default: `0`)                                                   |
| `fields`             | Optional: Comma-separated list of fields to return (e.g., `key,value`)                          |

**Note:** Pagination is applied after filtering null values, so the actual number of rows scanned from the database may be higher than the limit.

**Example - Get all follows (SocialDB tree structure):**

```bash
curl "https://near.garden/v1/kv/query?predecessor_id=james.near&current_account_id=social.near&key_prefix=graph/follow/&exclude_null=true"
```

This returns all keys under the `graph/follow/` tree:

- `graph/follow/alice.near`
- `graph/follow/bob.near`
- etc.

**Example - Get entire profile tree:**

```bash
curl "https://near.garden/v1/kv/query?predecessor_id=alice.near&current_account_id=social.near&key_prefix=profile/&exclude_null=true"
```

Returns all profile keys:

- `profile/name`
- `profile/image`
- `profile/description`
- etc.

### Reverse Lookup

```
GET /v1/kv/reverse?current_account_id={contract}&key={key}&exclude_null={bool}
```

Find all accounts that have a specific key (e.g., "who follows root.near?").

Results are automatically deduplicated by `predecessor_id`, keeping only the most recent entry for each account. The materialized view returns entries ordered by recency, and the first occurrence of each `predecessor_id` is kept.

| Parameter            | Description                                                               |
| -------------------- | ------------------------------------------------------------------------- |
| `current_account_id` | The contract called                                                       |
| `key`                | The exact key to look up                                                  |
| `exclude_null`       | If `true`, excludes entries where value is JSON `null` (default: `false`) |
| `limit`              | Max results after filtering (default: `100`, range: `1-1000`)             |
| `offset`             | Skip N results after filtering (default: `0`)                             |
| `fields`             | Optional: Comma-separated list of fields to return                        |

**Note:** Pagination is applied after deduplication and filtering, so the actual number of rows scanned may be higher than the limit.

**Example - Get followers:**

```bash
curl "https://near.garden/v1/kv/reverse?current_account_id=social.near&key=graph/follow/james.near&exclude_null=true"
```

## Response Format

**Query/Reverse endpoints:**

```json
{
  "entries": [
    {
      "predecessor_id": "james.near",
      "current_account_id": "social.near",
      "key": "graph/follow/root.near",
      "value": "\"\"",
      "block_height": 183302718,
      "block_timestamp": 1769731630602682599,
      "receipt_id": "abc123...",
      "tx_hash": "xyz789..."
    }
  ]
}
```

**Get endpoint (single entry):**

```json
{
  "predecessor_id": "james.near",
  "current_account_id": "social.near",
  "key": "graph/follow/root.near",
  "value": "\"\"",
  "block_height": 183302718,
  "block_timestamp": 1769731630602682599,
  "receipt_id": "abc123...",
  "tx_hash": "xyz789..."
}
```

Returns `null` if entry not found.

**Error responses:**

```json
{
  "error": "Invalid parameter: limit must be between 1 and 1000"
}
```

```json
{
  "error": "Invalid parameter: predecessor_id cannot be empty"
}
```

```json
{
  "error": "Invalid parameter: key_prefix cannot be empty string (omit parameter if not filtering)"
}
```

## Response Fields

Each entry object contains the following fields:

| Field                | Type   | Description                                              |
| -------------------- | ------ | -------------------------------------------------------- |
| `predecessor_id`     | string | The NEAR account that wrote this key-value data          |
| `current_account_id` | string | The contract that was called (e.g., `social.near`)       |
| `key`                | string | The key path (e.g., `graph/follow/root.near`, `profile`) |
| `value`              | string | JSON-serialized value (see Data Format section)          |
| `block_height`       | number | NEAR blockchain block height when this write occurred    |
| `block_timestamp`    | number | Nanoseconds since Unix epoch (divide by 1e9 for seconds) |
| `receipt_id`         | string | NEAR protocol receipt ID for this transaction            |
| `tx_hash`            | string | NEAR protocol transaction hash                           |

## Data Format

Values are stored as JSON-serialized strings. The indexer converts all values to JSON strings before storing them in the database:

- JSON string `"hello"` → stored as `"\"hello\""`
- JSON null → stored as `"null"`
- JSON number `42` → stored as `"42"`
- JSON boolean `true` → stored as `"true"`
- Empty JSON string `""` → stored as `"\"\""`

**Example response values:**

```json
{
  "value": "\"\"", // Empty string (deleted/cleared value)
  "value": "null", // Null (deleted key)
  "value": "{\"name\":\"Alice\"}" // JSON object
}
```

The `exclude_null` parameter filters entries where the value field equals the string `"null"` (JSON-serialized null values).

## Tech Stack

- **Rust** (stable) + Actix-web 4.5
- **ScyllaDB** - High-performance distributed database
- **Scylla Driver** v1.4 with full serialization support
- **Rustls 0.23** - TLS/SSL with AWS LC backend
- **Actix-CORS** - Cross-origin resource sharing
- **Tracing** - Structured logging and observability
- **fastnear-primitives** - NEAR protocol types

## CORS Policy

The API is configured with the following CORS settings:

- **Allowed origins:** Any (`*`)
- **Allowed methods:** `GET`, `POST`
- **Allowed headers:** `Content-Type`, `Authorization`, `Accept`
- **Preflight cache:** 1 hour (3600 seconds)

## Response Compression

Responses are automatically compressed using gzip, deflate, or brotli based on the client's `Accept-Encoding` header. This significantly reduces bandwidth usage, especially for large query results with many entries.

## Development

### Prerequisites

- Rust stable (1.70+)
- ScyllaDB instance

### Environment Variables

**Required:**

```bash
CHAIN_ID=mainnet                        # Chain ID (used for keyspace: fastdata_{chain_id})
SCYLLA_URL=your-scylla-host:9042        # ScyllaDB connection endpoint
SCYLLA_USERNAME=scylla                  # ScyllaDB username
SCYLLA_PASSWORD=your-password           # ScyllaDB password
PORT=3001                               # Server port (optional, defaults to 3001)
```

**Optional (TLS/SSL):**

```bash
SCYLLA_SSL_CA=/path/to/ca.pem           # CA certificate (enables TLS)
SCYLLA_SSL_CERT=/path/to/client.pem     # Client certificate (for mTLS)
SCYLLA_SSL_KEY=/path/to/client-key.pem  # Client private key (for mTLS)
```

When `SCYLLA_SSL_CA` is set, the server connects to ScyllaDB using TLS. If both `SCYLLA_SSL_CERT` and `SCYLLA_SSL_KEY` are also provided, mutual TLS (mTLS) authentication is enabled.

**Optional (Advanced Configuration):**

```bash
RUST_LOG=info                           # Logging level (default: "scylladb=info,fastkv-server=info")
                                        # Examples: "debug", "warn", "fastkv-server=debug"
KEYSPACE=custom_keyspace                # Override keyspace (default: fastdata_{CHAIN_ID})
TABLE_NAME=custom_table                 # Override latest values table (default: s_kv_last)
HISTORY_TABLE_NAME=custom_history       # Override history table (default: s_kv)
REVERSE_VIEW_NAME=custom_mv             # Override reverse lookup view (default: mv_kv_cur_key)
```

**Note:** The server uses `dotenv` to automatically load environment variables from a `.env` file in the project root for local development.

### Startup Requirements

The server performs validation checks at startup and will exit immediately if:

- Required environment variables (`CHAIN_ID`, `SCYLLA_URL`, `SCYLLA_USERNAME`, `SCYLLA_PASSWORD`) are missing
- `CHAIN_ID` is not a valid chain identifier
- Cannot connect to ScyllaDB at the specified URL
- TLS certificates (if configured) cannot be read or are invalid
- The specified keyspace does not exist

Check server logs for specific error messages if the server fails to start.

### Run Locally

```bash
# Create a .env file with your configuration (see Environment Variables above)
cargo run
```

The server binds to `0.0.0.0` (all network interfaces) on the specified PORT, making it accessible from external connections. This is required for Railway deployment and production environments.

### Build for Production

```bash
cargo build --release
```

## Deployment

The service includes a Dockerfile for Railway/Docker deployment:

```dockerfile
FROM rust:latest AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/fastkv-server /usr/local/bin/
CMD ["fastkv-server"]
```

## Database Schema

The server connects to the keyspace `fastdata_{CHAIN_ID}` (e.g., `fastdata_mainnet`).

### Latest Values Table

`s_kv_last` - Stores only the most recent value for each key:

```sql
CREATE TABLE s_kv_last (
    predecessor_id text,
    current_account_id text,
    key text,
    value text,
    block_height bigint,
    block_timestamp bigint,
    receipt_id text,
    tx_hash text,
    PRIMARY KEY ((predecessor_id), current_account_id, key)
);
```

### History Table

`s_kv` - Stores complete history of all values:

```sql
CREATE TABLE s_kv (
    predecessor_id text,
    current_account_id text,
    key text,
    block_height bigint,
    order_id bigint,
    value text,
    block_timestamp bigint,
    receipt_id text,
    tx_hash text,
    signer_id text,
    shard_id int,
    receipt_index int,
    action_index int,
    PRIMARY KEY ((predecessor_id), current_account_id, key, block_height, order_id)
);
```

### Reverse Lookup View

`mv_kv_cur_key` - Materialized view for reverse lookups (finding all accounts with a specific key).

### Query Configuration

All database queries use ScyllaDB consistency level `LocalOne`, which prioritizes low latency over strong consistency across data centers. This provides:

- **Fast reads:** Queries only need to reach one replica in the local datacenter
- **Lower latency:** Minimal network overhead
- **Trade-off:** May not reflect the very latest writes from other datacenters

For most use cases, this provides an optimal balance of performance and consistency.

**Query Timeout:** All queries have a 10-second timeout. Queries that exceed this limit will fail with a database error.

## Related Repositories

- [fastnear-social](https://github.com/MultiAgency/fastnear-social) - Evolving frontend
- [fastfs-server](https://github.com/fastnear/fastfs-server) - API for file system queries
- [fastdata-indexer](https://github.com/fastnear/fastdata-indexer) - Blockchain data pipeline

## License

MIT

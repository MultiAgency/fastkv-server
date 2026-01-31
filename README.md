# FastKV Server

API for querying FastData key-value data stored in ScyllaDB.

**Live:** https://fastdata.up.railway.app

## Endpoints

### Health Check

```
GET /health
```

Returns a static health status. The server tests database connectivity on startup but this endpoint does not verify live database connection.

Response:

```json
{ "status": "ok" }
```

### Get Single Entry

```
GET /v1/kv/get?predecessor_id={account}&current_account_id={contract}&key={key}
```

| Parameter            | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| `predecessor_id`     | The account that wrote the data                              |
| `current_account_id` | The contract called (e.g., `social.near`)                    |
| `key`                | The specific key with which to retrieve its respective value |

Returns a single entry object if found, or `null` if not found.

### Query Multiple Entries

```
GET /v1/kv/query?predecessor_id={account}&current_account_id={contract}&key_prefix={prefix}&exclude_null={bool}
```

| Parameter            | Description                                    |
| -------------------- | ---------------------------------------------- |
| `predecessor_id`     | The account that wrote the data                |
| `current_account_id` | The contract called                            |
| `key_prefix`         | Optional prefix filter (e.g., `graph/follow/`). Cannot be empty string - omit if not filtering. |
| `exclude_null`       | If `true`, excludes entries where value is JSON `null` (default: `false`) |
| `limit`              | Max results after filtering (default: `100`, range: `1-1000`) |
| `offset`             | Skip N results after filtering (default: `0`)  |

**Note:** Pagination is applied after filtering null values, so the actual number of rows scanned from the database may be higher than the limit.

**Example - Get all follows:**

```bash
curl "https://fastdata.up.railway.app/v1/kv/query?predecessor_id=james.near&current_account_id=social.near&key_prefix=graph/follow/&exclude_null=true"
```

### Reverse Lookup

```
GET /v1/kv/reverse?current_account_id={contract}&key={key}&exclude_null={bool}
```

Find all accounts that have a specific key (e.g., "who follows root.near?").

Results are automatically deduplicated by `predecessor_id`, keeping only the most recent entry for each account. The materialized view returns entries ordered by recency, and the first occurrence of each `predecessor_id` is kept.

| Parameter            | Description                                       |
| -------------------- | ------------------------------------------------- |
| `current_account_id` | The contract called                               |
| `key`                | The exact key to look up                          |
| `exclude_null`       | If `true`, excludes entries where value is JSON `null` (default: `false`) |
| `limit`              | Max results after filtering (default: `100`, range: `1-1000`) |
| `offset`             | Skip N results after filtering (default: `0`)     |

**Note:** Pagination is applied after deduplication and filtering, so the actual number of rows scanned may be higher than the limit.

**Example - Get followers:**

```bash
curl "https://fastdata.up.railway.app/v1/kv/reverse?current_account_id=social.near&key=graph/follow/james.near&exclude_null=true"
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

| Field                  | Type   | Description                                                                 |
| ---------------------- | ------ | --------------------------------------------------------------------------- |
| `predecessor_id`       | string | The NEAR account that wrote this key-value data                             |
| `current_account_id`   | string | The contract that was called (e.g., `social.near`)                          |
| `key`                  | string | The key path (e.g., `graph/follow/root.near`, `profile`)                   |
| `value`                | string | JSON-serialized value (see Data Format section)                            |
| `block_height`         | number | NEAR blockchain block height when this write occurred                       |
| `block_timestamp`      | number | Nanoseconds since Unix epoch (divide by 1e9 for seconds)                   |
| `receipt_id`           | string | NEAR protocol receipt ID for this transaction                               |
| `tx_hash`              | string | NEAR protocol transaction hash                                              |

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
  "value": "\"\"",        // Empty string (deleted/cleared value)
  "value": "null",        // Null (deleted key)
  "value": "{\"name\":\"Alice\"}"  // JSON object
}
```

The `exclude_null` parameter filters entries where the value field equals the string `"null"` (JSON-serialized null values).

## Tech Stack

- **Rust** (nightly) + Actix-web 4.5
- **ScyllaDB** - High-performance distributed database
- **Scylla Driver** v1.4 with full serialization support
- **Rustls 0.23** - TLS/SSL with AWS LC backend
- **Actix-CORS** - Cross-origin resource sharing
- **Tracing** - Structured logging and observability
- **fastnear-primitives** - NEAR protocol types

## CORS Policy

The API is configured with the following CORS settings:

- **Allowed origins:** Any (`*`)
- **Allowed methods:** `GET` only
- **Allowed headers:** `Content-Type`, `Authorization`, `Accept`
- **Preflight cache:** 1 hour (3600 seconds)

**Note:** Only GET requests are allowed. POST, PUT, DELETE, and other methods will be rejected with a CORS error.

## Development

### Prerequisites

- Rust (nightly recommended)
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

**Note:** The server uses `dotenv` to automatically load environment variables from a `.env` file in the project root for local development.

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
FROM rustlang/rust:nightly AS builder
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

Queries the `s_kv_last` table:

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

And the `mv_kv_cur_key` materialized view for reverse lookups.

### Query Configuration

All database queries use ScyllaDB consistency level `LocalOne`, which prioritizes low latency over strong consistency across data centers. This provides:

- **Fast reads:** Queries only need to reach one replica in the local datacenter
- **Lower latency:** Minimal network overhead
- **Trade-off:** May not reflect the very latest writes from other datacenters

For most use cases, this provides an optimal balance of performance and consistency.

## Related Repositories

- [fastnear-social](https://github.com/MultiAgency/fastnear-social) - Evolving frontend
- [fastfs-server](https://github.com/fastnear/fastfs-server) - API for file system queries
- [fastdata-indexer](https://github.com/fastnear/fastdata-indexer) - Blockchain data pipeline

## License

MIT

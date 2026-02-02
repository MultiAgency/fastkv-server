# TODO

## Future Endpoints (require schema/infrastructure changes)

### /v1/kv/recent — Global activity feed
Global recent writes across all accounts, ordered by block_height desc.
- Requires new time-bucketed table + indexer changes
- Schema: `CREATE TABLE kv_recent (time_bucket int, block_height bigint, order_id bigint, predecessor_id text, current_account_id text, key text, value text, ..., PRIMARY KEY (time_bucket, block_height, order_id)) WITH CLUSTERING ORDER BY (block_height DESC, order_id DESC)`
- Partitioned by time bucket (hourly/daily) to keep partitions bounded
- Indexer must write to this table on every `__fastdata_kv` call
- ~40 lines for handler, significant indexer work

### /v1/kv/count — Count matching entries
Same as /kv/query or /kv/reverse but returns `{ count: N }` instead of entries. Feasible with current schema but ScyllaDB COUNT scans all matching rows. Low priority — clients can paginate and count.

### /v1/kv/subscribe — Real-time event stream (SSE/WebSocket)
Subscribe to key patterns, get pushes as indexer writes new data. Requires message broker infrastructure (Redis Pub/Sub, NATS, or Kafka) between indexer and server. ~200-400 lines + infrastructure. Should be a separate project.

use actix_web::{error::ResponseError, http::StatusCode, HttpResponse};
use scylla::DeserializeRow;
use serde::{Deserialize, Serialize};
use std::fmt;

// Shared validation constants
pub const MAX_OFFSET: usize = 100_000;
pub const MAX_PREFIX_LENGTH: usize = 1000;
pub const MAX_ACCOUNT_ID_LENGTH: usize = 256;
pub const MAX_KEY_LENGTH: usize = 10000;
pub const MAX_BATCH_KEYS: usize = 100;
pub const MAX_BATCH_KEY_LENGTH: usize = 1024;
pub const MAX_SOCIAL_RESULTS: usize = 1000;
pub const MAX_SOCIAL_KEYS: usize = 100;
pub const MAX_STREAM_ERRORS: usize = 10;
pub const MAX_HISTORY_SCAN: usize = 10_000;
pub const MAX_DEDUP_SCAN: usize = 100_000;
pub const MAX_EDGE_TYPE_LENGTH: usize = 256;
pub const MAX_SCAN_LIMIT: usize = 1000;
pub const PROJECT_ID: &str = "fastkv-server";

// Raw row from ScyllaDB s_kv_last (matches table schema exactly)
#[derive(DeserializeRow, Debug, Clone)]
pub struct KvRow {
    pub predecessor_id: String,
    pub current_account_id: String,
    pub key: String,
    pub value: String,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub receipt_id: String,
    pub tx_hash: String,
}

// Raw row from ScyllaDB s_kv (history table with additional fields)
#[derive(DeserializeRow, Debug, Clone)]
pub struct KvHistoryRow {
    pub predecessor_id: String,
    pub current_account_id: String,
    pub key: String,
    pub block_height: i64,
    pub order_id: i64,
    pub value: String,
    pub block_timestamp: i64,
    pub receipt_id: String,
    pub tx_hash: String,
    pub signer_id: String,
    pub shard_id: i32,
    pub receipt_index: i32,
    pub action_index: i32,
}

// Lightweight row for contract-based account queries (predecessor_id only)
#[derive(DeserializeRow, Debug, Clone)]
pub struct ContractAccountRow {
    pub predecessor_id: String,
}

// API response
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct KvEntry {
    #[serde(rename = "accountId")]
    pub predecessor_id: String,
    #[serde(rename = "contractId")]
    pub current_account_id: String,
    pub key: String,
    pub value: String,
    pub block_height: u64,
    pub block_timestamp: u64,
    pub receipt_id: String,
    pub tx_hash: String,
    /// True when the entry represents a deletion (value is the literal string "null").
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub is_deleted: bool,
}

impl KvEntry {
    /// Convert to JSON with only requested fields. Pass a pre-built HashSet to avoid
    /// rebuilding it per entry when called in a loop.
    pub fn to_json_with_fields(
        &self,
        fields: &Option<std::collections::HashSet<String>>,
    ) -> serde_json::Value {
        if let Some(field_set) = fields {
            let mut map = serde_json::Map::new();

            if field_set.contains("accountId") {
                map.insert(
                    "accountId".to_string(),
                    serde_json::json!(&self.predecessor_id),
                );
            }
            if field_set.contains("contractId") {
                map.insert(
                    "contractId".to_string(),
                    serde_json::json!(&self.current_account_id),
                );
            }
            if field_set.contains("key") {
                map.insert("key".to_string(), serde_json::json!(&self.key));
            }
            if field_set.contains("value") {
                map.insert("value".to_string(), serde_json::json!(&self.value));
            }
            if field_set.contains("block_height") {
                map.insert(
                    "block_height".to_string(),
                    serde_json::json!(self.block_height),
                );
            }
            if field_set.contains("block_timestamp") {
                map.insert(
                    "block_timestamp".to_string(),
                    serde_json::json!(self.block_timestamp),
                );
            }
            if field_set.contains("receipt_id") {
                map.insert(
                    "receipt_id".to_string(),
                    serde_json::json!(&self.receipt_id),
                );
            }
            if field_set.contains("tx_hash") {
                map.insert("tx_hash".to_string(), serde_json::json!(&self.tx_hash));
            }
            if field_set.contains("is_deleted") && self.is_deleted {
                map.insert("is_deleted".to_string(), serde_json::json!(true));
            }

            serde_json::Value::Object(map)
        } else {
            // No field filtering - return all fields
            serde_json::to_value(self).unwrap_or_else(|e| {
                tracing::error!(target: "fastkv-server", error = %e, "Failed to serialize KvEntry");
                serde_json::json!({"error": "serialization_failed"})
            })
        }
    }
}

/// Convert a ScyllaDB bigint (i64) to u64, clamping negatives to 0.
/// ScyllaDB stores block heights/timestamps as bigint (i64) but they are
/// logically unsigned. Negative values indicate upstream data issues.
pub fn bigint_to_u64(val: i64) -> u64 {
    val.max(0) as u64
}

impl From<KvRow> for KvEntry {
    fn from(row: KvRow) -> Self {
        let is_deleted = row.value == "null";
        Self {
            predecessor_id: row.predecessor_id,
            current_account_id: row.current_account_id,
            key: row.key,
            value: row.value,
            block_height: bigint_to_u64(row.block_height),
            block_timestamp: bigint_to_u64(row.block_timestamp),
            receipt_id: row.receipt_id,
            tx_hash: row.tx_hash,
            is_deleted,
        }
    }
}

impl From<KvHistoryRow> for KvEntry {
    fn from(row: KvHistoryRow) -> Self {
        let is_deleted = row.value == "null";
        Self {
            predecessor_id: row.predecessor_id,
            current_account_id: row.current_account_id,
            key: row.key,
            value: row.value,
            block_height: bigint_to_u64(row.block_height),
            block_timestamp: bigint_to_u64(row.block_timestamp),
            receipt_id: row.receipt_id,
            tx_hash: row.tx_hash,
            is_deleted,
        }
    }
}

// Pagination metadata returned in all paginated responses
#[derive(Serialize, utoipa::ToSchema)]
pub struct PaginationMeta {
    pub has_more: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    #[schema(default = false)]
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Number of rows skipped due to deserialization errors. Omitted when zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dropped_rows: Option<u32>,
}

// Standardized paginated response for all list endpoints
#[derive(Serialize, utoipa::ToSchema)]
pub struct PaginatedResponse<T: Serialize + utoipa::ToSchema> {
    pub data: Vec<T>,
    pub meta: PaginationMeta,
}

// Standardized single-item response wrapper
#[derive(Serialize, utoipa::ToSchema)]
pub struct DataResponse<T: Serialize + utoipa::ToSchema> {
    pub data: T,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct TreeResponse {
    pub tree: serde_json::Value,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct HealthResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
}

// Query parameter structs
#[derive(Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
pub struct GetParams {
    #[serde(rename = "accountId")]
    pub predecessor_id: String,
    #[serde(rename = "contractId")]
    pub current_account_id: String,
    pub key: String,
    #[serde(default)]
    pub fields: Option<String>, // Comma-separated field names
    /// Value format: "raw" (default) or "json" (decoded).
    #[serde(default)]
    pub value_format: Option<String>,
}

/// Parse a comma-separated fields string into a set of field names.
pub fn parse_field_set(fields: &Option<String>) -> Option<std::collections::HashSet<String>> {
    fields.as_ref().map(|f| {
        f.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    })
}

/// Convert a dropped-row count to `Option<u32>`, returning `None` for zero.
pub(crate) fn dropped_to_option(n: usize) -> Option<u32> {
    if n > 0 {
        Some(n.min(u32::MAX as usize) as u32)
    } else {
        None
    }
}

/// Resolve whether to decode values based on `value_format`.
pub fn should_decode(value_format: &Option<String>) -> Result<bool, ApiError> {
    match value_format.as_deref() {
        Some("json") => Ok(true),
        Some("raw") | None => Ok(false),
        Some(other) => Err(ApiError::InvalidParameter(format!(
            "value_format must be 'json' or 'raw', got '{}'",
            other
        ))),
    }
}

pub fn validate_limit(limit: usize) -> Result<(), ApiError> {
    if limit == 0 || limit > 1000 {
        return Err(ApiError::InvalidParameter(
            "limit must be between 1 and 1000".to_string(),
        ));
    }
    Ok(())
}

#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct QueryParams {
    #[serde(rename = "accountId")]
    pub predecessor_id: String,
    #[serde(rename = "contractId")]
    pub current_account_id: String,
    #[serde(default)]
    pub key_prefix: Option<String>,
    #[serde(default)]
    pub exclude_null: Option<bool>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub fields: Option<String>, // Comma-separated field names
    /// Response format. Use `"tree"` for nested JSON; omit for paginated list.
    #[serde(default)]
    pub format: Option<String>,
    /// Value format: "raw" (default) or "json" (decoded).
    #[serde(default)]
    pub value_format: Option<String>,
    /// Cursor: return entries with key alphabetically after this value (exclusive).
    /// Cannot be combined with offset > 0.
    #[serde(default)]
    pub after_key: Option<String>,
}

// GET /v1/kv/writers — replaces /v1/kv/reverse and /v1/kv/by-key
#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct WritersParams {
    #[serde(rename = "contractId")]
    pub current_account_id: String,
    pub key: String,
    /// Optional: filter to a specific writer account
    #[serde(rename = "accountId")]
    #[serde(default)]
    pub predecessor_id: Option<String>,
    #[serde(default)]
    pub exclude_null: Option<bool>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub fields: Option<String>,
    /// Value format: "raw" (default) or "json" (decoded).
    #[serde(default)]
    pub value_format: Option<String>,
    /// Cursor: return writers with account ID alphabetically after this value (exclusive).
    /// Cannot be combined with offset > 0.
    #[serde(default)]
    pub after_account: Option<String>,
}

fn default_limit() -> usize {
    100
}

// History query parameters
#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct HistoryParams {
    #[serde(rename = "accountId")]
    pub predecessor_id: String,
    #[serde(rename = "contractId")]
    pub current_account_id: String,
    pub key: String,
    #[serde(default = "default_history_limit")]
    pub limit: usize,
    #[serde(default = "default_order_desc")]
    pub order: String, // "asc" or "desc"
    #[serde(default)]
    pub from_block: Option<i64>,
    #[serde(default)]
    pub to_block: Option<i64>,
    #[serde(default)]
    pub fields: Option<String>, // Comma-separated field names
    /// Value format: "raw" (default) or "json" (decoded).
    #[serde(default)]
    pub value_format: Option<String>,
}

fn default_history_limit() -> usize {
    100
}

fn default_order_desc() -> String {
    "desc".to_string()
}

// Internal accounts query parameters (used by social handlers, not exposed in API)
#[derive(Deserialize, Clone)]
pub struct AccountsParams {
    #[serde(rename = "contractId")]
    pub current_account_id: String,
    pub key: String,
    #[serde(default)]
    pub exclude_null: Option<bool>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    /// Cursor: return accounts alphabetically after this value (exclusive).
    #[serde(default)]
    pub after_account: Option<String>,
}

// Accounts-by-contract query parameters
#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct AccountsQueryParams {
    /// Contract account. When omitted, requires scan=1 for a full table scan.
    #[serde(rename = "contractId", default)]
    pub contract_id: Option<String>,
    /// Opt-in for expensive full table scan (required when contractId is omitted).
    #[serde(default)]
    pub scan: Option<u8>,
    /// Optional key filter. Recommended for large contracts to avoid expensive full-partition scans.
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    /// Cursor: return accounts after this value (exclusive).
    /// Token-ordered (Murmur3) in scan mode; lexicographic with contractId.
    /// Cannot be combined with offset > 0.
    /// Note: responses always emit next_cursor (even when has_more is false);
    /// use it for resumption, especially when truncated=true.
    #[serde(default)]
    pub after_account: Option<String>,
}

// Diff query parameters
#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct DiffParams {
    #[serde(rename = "accountId")]
    pub predecessor_id: String,
    #[serde(rename = "contractId")]
    pub current_account_id: String,
    pub key: String,
    pub block_height_a: i64,
    pub block_height_b: i64,
    #[serde(default)]
    pub fields: Option<String>,
    /// Value format: "raw" (default) or "json" (decoded).
    #[serde(default)]
    pub value_format: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct DiffResponse {
    pub a: Option<KvEntry>,
    pub b: Option<KvEntry>,
}

// Timeline query parameters
#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct TimelineParams {
    #[serde(rename = "accountId")]
    pub predecessor_id: String,
    #[serde(rename = "contractId")]
    pub current_account_id: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_order_desc")]
    pub order: String,
    #[serde(default)]
    pub from_block: Option<i64>,
    #[serde(default)]
    pub to_block: Option<i64>,
    #[serde(default)]
    pub fields: Option<String>,
    /// Value format: "raw" (default) or "json" (decoded).
    #[serde(default)]
    pub value_format: Option<String>,
}

// Batch query structs
#[derive(Deserialize, utoipa::ToSchema)]
pub struct BatchQuery {
    #[serde(rename = "accountId")]
    pub predecessor_id: String,
    #[serde(rename = "contractId")]
    pub current_account_id: String,
    pub keys: Vec<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct BatchResultItem {
    pub key: String,
    pub value: Option<String>,
    pub found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ===== Social API types =====

// POST /v1/social/get request body
#[derive(Deserialize, utoipa::ToSchema)]
pub struct SocialGetBody {
    pub keys: Vec<String>,
    #[serde(default)]
    #[serde(alias = "contractId")]
    pub contract_id: Option<String>,
    #[serde(default)]
    pub options: Option<SocialGetOptions>,
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct SocialGetOptions {
    #[serde(default)]
    pub with_block_height: Option<bool>,
    #[serde(default)]
    pub return_deleted: Option<bool>,
}

// POST /v1/social/keys request body
#[derive(Deserialize, utoipa::ToSchema)]
pub struct SocialKeysBody {
    pub keys: Vec<String>,
    #[serde(default)]
    #[serde(alias = "contractId")]
    pub contract_id: Option<String>,
    #[serde(default)]
    pub options: Option<SocialKeysOptions>,
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct SocialKeysOptions {
    #[serde(default)]
    pub return_type: Option<String>, // "True" | "BlockHeight"
    #[serde(default)]
    pub return_deleted: Option<bool>,
    #[serde(default)]
    pub values_only: Option<bool>,
}

// GET /v1/social/index query params
#[derive(Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
pub struct SocialIndexParams {
    pub action: String,
    pub key: String,
    #[serde(default = "default_order_desc")]
    pub order: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub from: Option<u64>, // block_height cursor
    #[serde(default)]
    #[serde(alias = "accountId")]
    pub account_id: Option<String>,
    #[serde(default)]
    #[serde(alias = "contractId")]
    pub contract_id: Option<String>,
}

// GET /v1/social/profile query params
#[derive(Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
pub struct SocialProfileParams {
    #[serde(alias = "accountId")]
    pub account_id: String,
    #[serde(default)]
    #[serde(alias = "contractId")]
    pub contract_id: Option<String>,
}

// GET /v1/social/followers and /v1/social/following query params
#[derive(Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
pub struct SocialFollowParams {
    #[serde(alias = "accountId")]
    pub account_id: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    #[serde(alias = "contractId")]
    pub contract_id: Option<String>,
    /// Cursor: return accounts alphabetically after this value (exclusive).
    /// Cannot be combined with offset > 0.
    #[serde(default)]
    pub after_account: Option<String>,
}

// GET /v1/social/feed/account query params
#[derive(Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
pub struct SocialAccountFeedParams {
    #[serde(alias = "accountId")]
    pub account_id: String,
    #[serde(default = "default_order_desc")]
    pub order: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub from: Option<u64>,
    #[serde(default)]
    pub include_replies: Option<bool>,
    #[serde(default)]
    #[serde(alias = "contractId")]
    pub contract_id: Option<String>,
}

// Social API response types
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct IndexEntry {
    #[serde(rename = "accountId")]
    pub account_id: String,
    #[serde(rename = "blockHeight")]
    pub block_height: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct IndexResponse {
    pub entries: Vec<IndexEntry>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct SocialFollowResponse {
    pub accounts: Vec<String>,
    pub count: usize,
    pub meta: PaginationMeta,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct SocialFeedResponse {
    pub posts: Vec<IndexEntry>,
}

// Error handling
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(tag = "type", content = "message")]
pub enum ApiError {
    InvalidParameter(String),
    DatabaseError(String),
    DatabaseUnavailable,
    TooManyRequests(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ApiError::InvalidParameter(msg) => write!(f, "Invalid parameter: {}", msg),
            ApiError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            ApiError::DatabaseUnavailable => write!(f, "Database unavailable"),
            ApiError::TooManyRequests(msg) => write!(f, "{}", msg),
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        let status = match self {
            ApiError::InvalidParameter(_) => StatusCode::BAD_REQUEST,
            ApiError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::DatabaseUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::TooManyRequests(_) => StatusCode::TOO_MANY_REQUESTS,
        };

        HttpResponse::build(status).json(serde_json::json!({
            "error": self.to_string()
        }))
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        // Log full error internally for debugging, but return generic message to client
        // to prevent information disclosure (paths, IPs, schema details)
        tracing::error!(
            target: "fastkv-server",
            error = %err,
            "Database error occurred"
        );
        ApiError::DatabaseError("An internal database error occurred".to_string())
    }
}

// ===== Edges API types =====

// Raw row from ScyllaDB kv_edges table
#[derive(DeserializeRow, Debug, Clone)]
pub struct EdgeRow {
    pub source: String,
    pub block_height: i64,
}

// GET /v1/kv/edges query params
#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct EdgesParams {
    pub edge_type: String,
    pub target: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    /// Cursor: return sources alphabetically after this value (exclusive).
    /// Cannot be combined with offset > 0.
    #[serde(default)]
    pub after_source: Option<String>,
}

// GET /v1/kv/edges/count query params
#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct EdgesCountParams {
    pub edge_type: String,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct EdgeSourceEntry {
    pub source: String,
    pub block_height: u64,
}

// StatusResponse for /v1/status
#[derive(Serialize, utoipa::ToSchema)]
pub struct StatusResponse {
    pub indexer_block: Option<u64>,
    pub timestamp: String,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct EdgesCountResponse {
    pub edge_type: String,
    pub target: String,
    pub count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_entry_from_row() {
        let row = KvRow {
            predecessor_id: "alice.near".to_string(),
            current_account_id: "social.near".to_string(),
            key: "profile".to_string(),
            value: "test".to_string(),
            block_height: 123456789,
            block_timestamp: 1234567890123456789,
            receipt_id: "abc123".to_string(),
            tx_hash: "def456".to_string(),
        };

        let entry: KvEntry = row.into();
        assert_eq!(entry.predecessor_id, "alice.near");
        assert_eq!(entry.current_account_id, "social.near");
        assert_eq!(entry.key, "profile");
        assert_eq!(entry.block_height, 123456789);
        assert_eq!(entry.block_timestamp, 1234567890123456789);
        assert_eq!(entry.receipt_id, "abc123");
        assert_eq!(entry.tx_hash, "def456");
        assert!(!entry.is_deleted);
    }

    #[test]
    fn test_kv_entry_is_deleted() {
        let row = KvRow {
            predecessor_id: "alice.near".to_string(),
            current_account_id: "social.near".to_string(),
            key: "profile".to_string(),
            value: "null".to_string(),
            block_height: 100,
            block_timestamp: 200,
            receipt_id: "r".to_string(),
            tx_hash: "t".to_string(),
        };

        let entry: KvEntry = row.into();
        assert!(entry.is_deleted);

        // Verify is_deleted is serialized when true
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["is_deleted"], true);
    }

    #[test]
    fn test_kv_entry_is_deleted_omitted_when_false() {
        let row = KvRow {
            predecessor_id: "alice.near".to_string(),
            current_account_id: "social.near".to_string(),
            key: "profile".to_string(),
            value: "\"hello\"".to_string(),
            block_height: 100,
            block_timestamp: 200,
            receipt_id: "r".to_string(),
            tx_hash: "t".to_string(),
        };

        let entry: KvEntry = row.into();
        assert!(!entry.is_deleted);

        // Verify is_deleted is omitted when false
        let json = serde_json::to_value(&entry).unwrap();
        assert!(json.get("is_deleted").is_none());
    }

    #[test]
    fn test_should_decode() {
        assert!(should_decode(&Some("json".to_string())).unwrap());
        assert!(!should_decode(&Some("raw".to_string())).unwrap());
        assert!(!should_decode(&None).unwrap());
        // Invalid value_format
        assert!(should_decode(&Some("invalid".to_string())).is_err());
    }

    #[test]
    fn test_default_limit() {
        assert_eq!(default_limit(), 100);
    }

    #[test]
    fn test_bigint_to_u64_negative() {
        assert_eq!(bigint_to_u64(-1), 0);
        assert_eq!(bigint_to_u64(i64::MIN), 0);
        assert_eq!(bigint_to_u64(0), 0);
        assert_eq!(bigint_to_u64(42), 42);
    }

    #[test]
    fn test_pagination_meta_serialization() {
        let meta = PaginationMeta {
            has_more: true,
            truncated: false,
            next_cursor: Some("abc".to_string()),
            dropped_rows: None,
        };
        let json = serde_json::to_value(&meta).unwrap();
        assert_eq!(json["has_more"], true);
        assert!(json.get("truncated").is_none()); // skipped when false
        assert_eq!(json["next_cursor"], "abc");
        assert!(json.get("dropped_rows").is_none()); // skipped when None

        let meta_no_cursor = PaginationMeta {
            has_more: false,
            truncated: true,
            next_cursor: None,
            dropped_rows: None,
        };
        let json = serde_json::to_value(&meta_no_cursor).unwrap();
        assert_eq!(json["truncated"], true);
        assert!(json.get("next_cursor").is_none()); // skipped when None
    }

    #[test]
    fn test_pagination_meta_cursor_without_has_more() {
        let meta = PaginationMeta {
            has_more: false,
            truncated: false,
            next_cursor: Some("last_key".to_string()),
            dropped_rows: None,
        };
        let json = serde_json::to_value(&meta).unwrap();
        assert_eq!(json["has_more"], false);
        assert!(json.get("truncated").is_none());
        assert_eq!(json["next_cursor"], "last_key");
    }

    #[test]
    fn test_pagination_meta_dropped_rows_present_when_some() {
        let meta = PaginationMeta {
            has_more: true,
            truncated: false,
            next_cursor: None,
            dropped_rows: Some(3),
        };
        let json = serde_json::to_value(&meta).unwrap();
        assert_eq!(json["dropped_rows"], 3);
    }
}

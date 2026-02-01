use actix_web::{error::ResponseError, http::StatusCode, HttpResponse};
use scylla::DeserializeRow;
use serde::{Deserialize, Serialize};
use std::fmt;

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

// API response - keeps NEAR/FastData field names for compatibility
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct KvEntry {
    pub predecessor_id: String,
    pub current_account_id: String,
    pub key: String,
    pub value: String,
    pub block_height: u64,
    pub block_timestamp: u64,
    pub receipt_id: String,
    pub tx_hash: String,
}

impl KvEntry {
    // Convert to JSON with only requested fields
    pub fn to_json_with_fields(&self, fields: &Option<Vec<String>>) -> serde_json::Value {
        if let Some(field_list) = fields {
            let field_set: std::collections::HashSet<_> = field_list.iter().map(|s| s.as_str()).collect();
            let mut map = serde_json::Map::new();

            if field_set.contains("predecessor_id") {
                map.insert("predecessor_id".to_string(), serde_json::json!(&self.predecessor_id));
            }
            if field_set.contains("current_account_id") {
                map.insert("current_account_id".to_string(), serde_json::json!(&self.current_account_id));
            }
            if field_set.contains("key") {
                map.insert("key".to_string(), serde_json::json!(&self.key));
            }
            if field_set.contains("value") {
                map.insert("value".to_string(), serde_json::json!(&self.value));
            }
            if field_set.contains("block_height") {
                map.insert("block_height".to_string(), serde_json::json!(self.block_height));
            }
            if field_set.contains("block_timestamp") {
                map.insert("block_timestamp".to_string(), serde_json::json!(self.block_timestamp));
            }
            if field_set.contains("receipt_id") {
                map.insert("receipt_id".to_string(), serde_json::json!(&self.receipt_id));
            }
            if field_set.contains("tx_hash") {
                map.insert("tx_hash".to_string(), serde_json::json!(&self.tx_hash));
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

impl From<KvRow> for KvEntry {
    fn from(row: KvRow) -> Self {
        Self {
            predecessor_id: row.predecessor_id,
            current_account_id: row.current_account_id,
            key: row.key,
            value: row.value,
            block_height: row.block_height.max(0) as u64,
            block_timestamp: row.block_timestamp.max(0) as u64,
            receipt_id: row.receipt_id,
            tx_hash: row.tx_hash,
        }
    }
}

impl From<KvHistoryRow> for KvEntry {
    fn from(row: KvHistoryRow) -> Self {
        Self {
            predecessor_id: row.predecessor_id,
            current_account_id: row.current_account_id,
            key: row.key,
            value: row.value,
            block_height: row.block_height.max(0) as u64,
            block_timestamp: row.block_timestamp.max(0) as u64,
            receipt_id: row.receipt_id,
            tx_hash: row.tx_hash,
        }
    }
}

// Response structs
#[derive(Serialize, utoipa::ToSchema)]
pub struct QueryResponse {
    pub entries: Vec<KvEntry>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct HealthResponse {
    pub status: String,
}

// Query parameter structs - aligned with official FastData protocol
#[derive(Deserialize, utoipa::ToSchema, utoipa::IntoParams)]
pub struct GetParams {
    pub predecessor_id: String,
    pub current_account_id: String,
    pub key: String,
    #[serde(default)]
    pub fields: Option<String>, // Comma-separated field names
}

impl GetParams {
    pub fn parse_fields(&self) -> Option<Vec<String>> {
        self.fields.as_ref().map(|f| {
            f.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
    }
}

#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct QueryParams {
    pub predecessor_id: String,
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
}

impl QueryParams {
    pub fn parse_fields(&self) -> Option<Vec<String>> {
        self.fields.as_ref().map(|f| {
            f.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
    }
}

#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct ReverseParams {
    pub current_account_id: String,
    pub key: String,
    #[serde(default)]
    pub exclude_null: Option<bool>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub fields: Option<String>, // Comma-separated field names
}

impl ReverseParams {
    pub fn parse_fields(&self) -> Option<Vec<String>> {
        self.fields.as_ref().map(|f| {
            f.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
    }
}

fn default_limit() -> usize {
    100
}

// History query parameters
#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct HistoryParams {
    pub predecessor_id: String,
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
}

impl HistoryParams {
    pub fn parse_fields(&self) -> Option<Vec<String>> {
        self.fields.as_ref().map(|f| {
            f.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
    }
}

fn default_history_limit() -> usize {
    100
}

fn default_order_desc() -> String {
    "desc".to_string()
}

// Count query parameters
#[derive(Deserialize, Clone, utoipa::ToSchema, utoipa::IntoParams)]
pub struct CountParams {
    pub predecessor_id: String,
    pub current_account_id: String,
    #[serde(default)]
    pub key_prefix: Option<String>,
}

// Count response
#[derive(Serialize, utoipa::ToSchema)]
pub struct CountResponse {
    pub count: usize,
    pub estimated: bool,
}

// Batch query structs
#[derive(Deserialize, utoipa::ToSchema)]
pub struct BatchQuery {
    pub predecessor: String,
    pub current_account: String,
    pub keys: Vec<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct BatchResultItem {
    pub key: String,
    pub value: Option<String>,
    pub found: bool,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct BatchResponse {
    pub results: Vec<BatchResultItem>,
}

// Error handling
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(tag = "type", content = "message")]
pub enum ApiError {
    InvalidParameter(String),
    DatabaseError(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ApiError::InvalidParameter(msg) => write!(f, "Invalid parameter: {}", msg),
            ApiError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
        }
    }
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        let status = match self {
            ApiError::InvalidParameter(_) => StatusCode::BAD_REQUEST,
            ApiError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        HttpResponse::build(status).json(serde_json::json!({
            "error": self.to_string()
        }))
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        tracing::error!(
            target: "fastkv-server",
            error = %err,
            "Database error occurred"
        );
        ApiError::DatabaseError("Database error occurred".to_string())
    }
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
    }

    #[test]
    fn test_default_limit() {
        assert_eq!(default_limit(), 100);
    }
}

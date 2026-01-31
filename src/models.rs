use actix_web::{error::ResponseError, http::StatusCode, HttpResponse};
use scylla::DeserializeRow;
use serde::{Deserialize, Serialize};
use std::fmt;

// Raw row from ScyllaDB (matches table schema exactly)
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

// Parsed row for API responses
#[derive(Debug, Clone, Serialize)]
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

// Response structs
#[derive(Serialize)]
pub struct QueryResponse {
    pub entries: Vec<KvEntry>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

// Query parameter structs
#[derive(Deserialize)]
pub struct GetParams {
    pub predecessor_id: String,
    pub current_account_id: String,
    pub key: String,
}

#[derive(Deserialize, Clone)]
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
}

#[derive(Deserialize, Clone)]
pub struct ReverseParams {
    pub current_account_id: String,
    pub key: String,
    #[serde(default)]
    pub exclude_null: Option<bool>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

fn default_limit() -> usize {
    100
}

// Error handling
#[derive(Debug)]
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
        ApiError::DatabaseError(err.to_string())
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
        assert_eq!(entry.block_height, 123456789);
        assert_eq!(entry.block_timestamp, 1234567890123456789);
    }

    #[test]
    fn test_default_limit() {
        assert_eq!(default_limit(), 100);
    }
}

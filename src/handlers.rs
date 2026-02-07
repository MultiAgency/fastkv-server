use actix_web::{get, post, web, HttpResponse};
use crate::models::*;
use crate::scylladb::ScyllaDb;
use crate::tree::build_tree;
use crate::AppState;
use tokio::sync::RwLockReadGuard;

use std::collections::HashSet;

pub(crate) async fn require_db(state: &AppState) -> Result<RwLockReadGuard<'_, Option<ScyllaDb>>, ApiError> {
    let guard = state.scylladb.read().await;
    if guard.is_none() {
        return Err(ApiError::DatabaseUnavailable);
    }
    Ok(guard)
}

fn respond_with_entries(entries: Vec<KvEntry>, fields: &Option<HashSet<String>>) -> HttpResponse {
    if fields.is_some() {
        let filtered: Vec<_> = entries
            .into_iter()
            .map(|e| e.to_json_with_fields(fields))
            .collect();
        HttpResponse::Ok().json(serde_json::json!({ "entries": filtered }))
    } else {
        HttpResponse::Ok().json(QueryResponse { entries })
    }
}

pub(crate) fn validate_account_id(value: &str, name: &str) -> Result<(), ApiError> {
    if value.is_empty() {
        return Err(ApiError::InvalidParameter(format!("{} cannot be empty", name)));
    }
    if value.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("{} cannot exceed {} characters", name, MAX_ACCOUNT_ID_LENGTH),
        ));
    }
    Ok(())
}

pub(crate) fn validate_key(value: &str, name: &str, max_len: usize) -> Result<(), ApiError> {
    if value.is_empty() {
        return Err(ApiError::InvalidParameter(format!("{} cannot be empty", name)));
    }
    if value.len() > max_len {
        return Err(ApiError::InvalidParameter(
            format!("{} cannot exceed {} characters", name, max_len),
        ));
    }
    Ok(())
}

pub(crate) fn validate_offset(offset: usize) -> Result<(), ApiError> {
    if offset > MAX_OFFSET {
        return Err(ApiError::InvalidParameter(
            format!("offset cannot exceed {}", MAX_OFFSET),
        ));
    }
    Ok(())
}

fn validate_prefix(prefix: &Option<String>) -> Result<(), ApiError> {
    if let Some(ref p) = prefix {
        if p.is_empty() {
            return Err(ApiError::InvalidParameter(
                "key_prefix cannot be empty string (omit parameter if not filtering)".to_string(),
            ));
        }
        if p.len() > MAX_PREFIX_LENGTH {
            return Err(ApiError::InvalidParameter(
                format!("key_prefix cannot exceed {} characters", MAX_PREFIX_LENGTH),
            ));
        }
    }
    Ok(())
}

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Service is healthy", body = HealthResponse),
        (status = 503, description = "Database unavailable", body = HealthResponse)
    ),
    tag = "health"
)]
#[get("/health")]
pub async fn health_check(app_state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    let guard = app_state.scylladb.read().await;
    match guard.as_ref() {
        Some(db) => match db.health_check().await {
            Ok(_) => Ok(HttpResponse::Ok().json(HealthResponse {
                status: "ok".to_string(),
                database: None,
            })),
            Err(e) => {
                tracing::warn!(target: PROJECT_ID, error = %e, "Health check failed");
                Ok(HttpResponse::ServiceUnavailable().json(HealthResponse {
                    status: "degraded".to_string(),
                    database: Some("unavailable".to_string()),
                }))
            }
        },
        None => Ok(HttpResponse::ServiceUnavailable().json(HealthResponse {
            status: "degraded".to_string(),
            database: Some("unavailable".to_string()),
        })),
    }
}

/// Get a single KV entry by exact key
#[utoipa::path(
    get,
    path = "/v1/kv/get",
    params(GetParams),
    responses(
        (status = 200, description = "Entry found or null if not found", body = KvEntry),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/get")]
pub async fn get_kv_handler(
    query: web::Query<GetParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.predecessor_id, "predecessor_id")?;
    validate_account_id(&query.current_account_id, "current_account_id")?;
    validate_key(&query.key, "key", MAX_KEY_LENGTH)?;

    tracing::info!(
        target: PROJECT_ID,
        predecessor_id = %query.predecessor_id,
        current_account_id = %query.current_account_id,
        key = %query.key,
        "GET /v1/kv/get"
    );

    let db = require_db(&app_state).await?;
    let entry = db.as_ref().ok_or(ApiError::DatabaseUnavailable)?
        .get_kv(&query.predecessor_id, &query.current_account_id, &query.key)
        .await?;

    // Apply field selection and return
    let fields = parse_field_set(&query.fields);
    match entry {
        Some(entry) => Ok(HttpResponse::Ok().json(entry.to_json_with_fields(&fields))),
        None => Ok(HttpResponse::Ok().json(serde_json::Value::Null)),
    }
}

/// Query KV entries with optional prefix filtering and pagination
#[utoipa::path(
    get,
    path = "/v1/kv/query",
    params(QueryParams),
    responses(
        (status = 200, description = "List of matching entries", body = QueryResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/query")]
pub async fn query_kv_handler(
    query: web::Query<QueryParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.predecessor_id, "predecessor_id")?;
    validate_account_id(&query.current_account_id, "current_account_id")?;
    validate_limit(query.limit)?;
    validate_offset(query.offset)?;
    validate_prefix(&query.key_prefix)?;

    if let Some(ref fmt) = query.format {
        if fmt != "tree" {
            return Err(ApiError::InvalidParameter(
                "format must be 'tree' or omitted".to_string(),
            ));
        }
    }

    tracing::info!(
        target: PROJECT_ID,
        predecessor_id = %query.predecessor_id,
        current_account_id = %query.current_account_id,
        key_prefix = ?query.key_prefix,
        limit = query.limit,
        offset = query.offset,
        "GET /v1/kv/query"
    );

    let db = require_db(&app_state).await?;
    let entries = db.as_ref().ok_or(ApiError::DatabaseUnavailable)?
        .query_kv_with_pagination(&query)
        .await?;

    // Tree format: transform flat entries into nested JSON
    if query.format.as_deref() == Some("tree") {
        let items: Vec<(String, String)> = entries
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();
        let tree = build_tree(&items);
        return Ok(HttpResponse::Ok().json(TreeResponse { tree }));
    }

    let fields = parse_field_set(&query.fields);
    Ok(respond_with_entries(entries, &fields))
}

/// Get historical versions of a KV entry
#[utoipa::path(
    get,
    path = "/v1/kv/history",
    params(HistoryParams),
    responses(
        (status = 200, description = "List of historical entries", body = QueryResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/history")]
pub async fn history_kv_handler(
    query: web::Query<HistoryParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.predecessor_id, "predecessor_id")?;
    validate_account_id(&query.current_account_id, "current_account_id")?;
    validate_key(&query.key, "key", MAX_KEY_LENGTH)?;
    validate_limit(query.limit)?;

    let order_lower = query.order.to_lowercase();
    if order_lower != "asc" && order_lower != "desc" {
        return Err(ApiError::InvalidParameter(
            "order must be 'asc' or 'desc'".to_string(),
        ));
    }

    if let (Some(from), Some(to)) = (query.from_block, query.to_block) {
        if from > to {
            return Err(ApiError::InvalidParameter(
                "from_block must be less than or equal to to_block".to_string(),
            ));
        }
    }

    tracing::info!(
        target: PROJECT_ID,
        predecessor_id = %query.predecessor_id,
        current_account_id = %query.current_account_id,
        key = %query.key,
        limit = query.limit,
        order = %query.order,
        from_block = ?query.from_block,
        to_block = ?query.to_block,
        "GET /v1/kv/history"
    );

    let db = require_db(&app_state).await?;
    let entries = db.as_ref().ok_or(ApiError::DatabaseUnavailable)?
        .get_kv_history(&query)
        .await?;

    let fields = parse_field_set(&query.fields);
    Ok(respond_with_entries(entries, &fields))
}

/// Reverse lookup: find all predecessor_ids that wrote to a given key
#[utoipa::path(
    get,
    path = "/v1/kv/reverse",
    params(ReverseParams),
    responses(
        (status = 200, description = "List of entries from different predecessors", body = QueryResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/reverse")]
pub async fn reverse_kv_handler(
    query: web::Query<ReverseParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.current_account_id, "current_account_id")?;
    validate_key(&query.key, "key", MAX_KEY_LENGTH)?;
    validate_limit(query.limit)?;
    validate_offset(query.offset)?;

    tracing::info!(
        target: PROJECT_ID,
        current_account_id = %query.current_account_id,
        key = %query.key,
        limit = query.limit,
        offset = query.offset,
        "GET /v1/kv/reverse"
    );

    let db = require_db(&app_state).await?;
    let entries = db.as_ref().ok_or(ApiError::DatabaseUnavailable)?
        .reverse_kv_with_dedup(&query)
        .await?;

    let fields = parse_field_set(&query.fields);
    Ok(respond_with_entries(entries, &fields))
}

/// Lookup by exact key: find all predecessors who wrote to a specific key for a given account
#[utoipa::path(
    get,
    path = "/v1/kv/by-key",
    params(ByKeyParams),
    responses(
        (status = 200, description = "List of entries matching the key", body = QueryResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/by-key")]
pub async fn by_key_handler(
    query: web::Query<ByKeyParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_key(&query.key, "key", MAX_KEY_LENGTH)?;
    validate_account_id(&query.current_account_id, "current_account_id")?;
    validate_limit(query.limit)?;
    validate_offset(query.offset)?;

    tracing::info!(
        target: PROJECT_ID,
        key = %query.key,
        current_account_id = %query.current_account_id,
        limit = query.limit,
        offset = query.offset,
        "GET /v1/kv/by-key"
    );

    let db = require_db(&app_state).await?;
    let entries = db.as_ref().ok_or(ApiError::DatabaseUnavailable)?
        .query_by_key(&query)
        .await?;

    let fields = parse_field_set(&query.fields);
    Ok(respond_with_entries(entries, &fields))
}

/// List unique writer accounts for a contract.
///
/// Returns deduplicated predecessor accounts that have written to the given contract.
/// Providing a `key` filter is recommended — omitting it scans the entire contract partition
/// which can be expensive for large contracts. Results are capped at 100,000 unique accounts;
/// if this limit is reached the response will include `"truncated": true`.
/// The `count` field reflects the number of accounts in the current page, not the total.
#[utoipa::path(
    get,
    path = "/v1/kv/accounts",
    params(AccountsQueryParams),
    responses(
        (status = 200, description = "List of writer accounts", body = AccountsResponse),
        (status = 400, description = "Invalid parameters", body = ApiError),
        (status = 503, description = "Database unavailable", body = ApiError),
    ),
    tag = "kv"
)]
#[get("/v1/kv/accounts")]
pub async fn accounts_handler(
    query: web::Query<AccountsQueryParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.contract_id, "contract_id")?;
    validate_limit(query.limit)?;
    validate_offset(query.offset)?;
    if let Some(ref key) = query.key {
        validate_key(key, "key", MAX_KEY_LENGTH)?;
    }

    tracing::info!(
        target: PROJECT_ID,
        contract_id = %query.contract_id,
        key = ?query.key,
        limit = query.limit,
        offset = query.offset,
        "GET /v1/kv/accounts"
    );

    let db = require_db(&app_state).await?;
    let (accounts, truncated) = db.as_ref().ok_or(ApiError::DatabaseUnavailable)?
        .query_accounts_by_contract(
            &query.contract_id,
            query.key.as_deref(),
            query.limit,
            query.offset,
        )
        .await?;

    let count = accounts.len();
    Ok(HttpResponse::Ok().json(AccountsResponse { accounts, count, truncated }))
}

/// Compare a key's value at two different block heights
#[utoipa::path(
    get,
    path = "/v1/kv/diff",
    params(DiffParams),
    responses(
        (status = 200, description = "Values at both block heights", body = DiffResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/diff")]
pub async fn diff_kv_handler(
    query: web::Query<DiffParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.predecessor_id, "predecessor_id")?;
    validate_account_id(&query.current_account_id, "current_account_id")?;
    validate_key(&query.key, "key", MAX_KEY_LENGTH)?;

    tracing::info!(
        target: PROJECT_ID,
        predecessor_id = %query.predecessor_id,
        current_account_id = %query.current_account_id,
        key = %query.key,
        block_height_a = query.block_height_a,
        block_height_b = query.block_height_b,
        "GET /v1/kv/diff"
    );

    let db = require_db(&app_state).await?;
    let scylladb = db.as_ref().ok_or(ApiError::DatabaseUnavailable)?;
    let (a, b) = futures::future::try_join(
        scylladb.get_kv_at_block(
            &query.predecessor_id, &query.current_account_id, &query.key, query.block_height_a,
        ),
        scylladb.get_kv_at_block(
            &query.predecessor_id, &query.current_account_id, &query.key, query.block_height_b,
        ),
    ).await?;

    let fields = parse_field_set(&query.fields);
    if fields.is_some() {
        Ok(HttpResponse::Ok().json(serde_json::json!({
            "a": a.as_ref().map(|e| e.to_json_with_fields(&fields)),
            "b": b.as_ref().map(|e| e.to_json_with_fields(&fields)),
        })))
    } else {
        Ok(HttpResponse::Ok().json(DiffResponse { a, b }))
    }
}

/// All writes by one account across all keys, ordered by block_height
#[utoipa::path(
    get,
    path = "/v1/kv/timeline",
    params(TimelineParams),
    responses(
        (status = 200, description = "Chronological list of all writes", body = QueryResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/timeline")]
pub async fn timeline_kv_handler(
    query: web::Query<TimelineParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.predecessor_id, "predecessor_id")?;
    validate_account_id(&query.current_account_id, "current_account_id")?;
    validate_limit(query.limit)?;
    validate_offset(query.offset)?;

    let order_lower = query.order.to_lowercase();
    if order_lower != "asc" && order_lower != "desc" {
        return Err(ApiError::InvalidParameter(
            "order must be 'asc' or 'desc'".to_string(),
        ));
    }

    if let (Some(from), Some(to)) = (query.from_block, query.to_block) {
        if from > to {
            return Err(ApiError::InvalidParameter(
                "from_block must be less than or equal to to_block".to_string(),
            ));
        }
    }

    tracing::info!(
        target: PROJECT_ID,
        predecessor_id = %query.predecessor_id,
        current_account_id = %query.current_account_id,
        limit = query.limit,
        offset = query.offset,
        order = %query.order,
        from_block = ?query.from_block,
        to_block = ?query.to_block,
        "GET /v1/kv/timeline"
    );

    let db = require_db(&app_state).await?;
    let entries = db.as_ref().ok_or(ApiError::DatabaseUnavailable)?
        .get_kv_timeline(&query)
        .await?;

    let fields = parse_field_set(&query.fields);
    Ok(respond_with_entries(entries, &fields))
}

/// Batch lookup: get values for multiple keys in a single request
#[utoipa::path(
    post,
    path = "/v1/kv/batch",
    request_body = BatchQuery,
    responses(
        (status = 200, description = "Batch results", body = BatchResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[post("/v1/kv/batch")]
pub async fn batch_kv_handler(
    body: web::Json<BatchQuery>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&body.predecessor_id, "predecessor_id")?;
    validate_account_id(&body.current_account_id, "current_account_id")?;
    if body.keys.is_empty() {
        return Err(ApiError::InvalidParameter(
            "keys cannot be empty".to_string(),
        ));
    }
    if body.keys.len() > MAX_BATCH_KEYS {
        return Err(ApiError::InvalidParameter(
            format!("keys cannot exceed {} items", MAX_BATCH_KEYS),
        ));
    }
    for key in &body.keys {
        if key.len() > MAX_BATCH_KEY_LENGTH {
            return Err(ApiError::InvalidParameter(
                format!("each key cannot exceed {} characters", MAX_BATCH_KEY_LENGTH),
            ));
        }
    }

    tracing::info!(
        target: PROJECT_ID,
        predecessor_id = %body.predecessor_id,
        current_account_id = %body.current_account_id,
        key_count = body.keys.len(),
        "POST /v1/kv/batch"
    );

    // Verify DB is available before starting batch
    let _ = require_db(&app_state).await?;

    use futures::stream::{self, StreamExt};
    let items: Vec<BatchResultItem> = stream::iter(body.keys.iter().map(|key| {
        let scylladb = app_state.scylladb.clone();
        let predecessor_id = body.predecessor_id.clone();
        let current_account_id = body.current_account_id.clone();
        let key = key.clone();
        async move {
            let guard = scylladb.read().await;
            let Some(db) = guard.as_ref() else {
                return BatchResultItem {
                    key,
                    found: false,
                    value: None,
                    error: Some("Database unavailable".to_string()),
                };
            };
            match db.get_kv_last(&predecessor_id, &current_account_id, &key).await {
                Ok(value) => BatchResultItem {
                    key,
                    found: value.is_some(),
                    value,
                    error: None,
                },
                Err(e) => {
                    // Log full error internally, return generic message to client
                    tracing::warn!(target: PROJECT_ID, error = %e, key = %key, "Batch key lookup failed");
                    BatchResultItem {
                        key,
                        found: false,
                        value: None,
                        error: Some("Lookup failed".to_string()),
                    }
                }
            }
        }
    }))
    .buffered(10)
    .collect()
    .await;

    Ok(HttpResponse::Ok().json(BatchResponse { results: items }))
}

/// List edge sources for a given edge type and target
#[utoipa::path(
    get,
    path = "/v1/kv/edges",
    params(EdgesParams),
    responses(
        (status = 200, description = "List of edge sources", body = EdgesResponse),
        (status = 400, description = "Invalid parameters", body = ApiError),
        (status = 503, description = "Database unavailable", body = ApiError),
    ),
    tag = "kv"
)]
#[get("/v1/kv/edges")]
pub async fn edges_handler(
    query: web::Query<EdgesParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_key(&query.edge_type, "edge_type", MAX_EDGE_TYPE_LENGTH)?;
    validate_account_id(&query.target, "target")?;
    validate_limit(query.limit)?;

    if query.after_source.is_none() {
        validate_offset(query.offset)?;
    }

    if let Some(ref cursor) = query.after_source {
        validate_account_id(cursor, "after_source")?;
    }

    tracing::info!(
        target: PROJECT_ID,
        edge_type = %query.edge_type,
        target = %query.target,
        limit = query.limit,
        offset = query.offset,
        after_source = ?query.after_source,
        "GET /v1/kv/edges"
    );

    let db = require_db(&app_state).await?;
    let sources = db.as_ref().ok_or(ApiError::DatabaseUnavailable)?
        .query_edges(
            &query.edge_type,
            &query.target,
            query.limit,
            query.offset,
            query.after_source.as_deref(),
        )
        .await?;

    let count = sources.len();
    Ok(HttpResponse::Ok().json(EdgesResponse { sources, count }))
}

/// Count edges for a given edge type and target
#[utoipa::path(
    get,
    path = "/v1/kv/edges/count",
    params(EdgesCountParams),
    responses(
        (status = 200, description = "Edge count", body = EdgesCountResponse),
        (status = 400, description = "Invalid parameters", body = ApiError),
        (status = 503, description = "Database unavailable", body = ApiError),
    ),
    tag = "kv"
)]
#[get("/v1/kv/edges/count")]
pub async fn edges_count_handler(
    query: web::Query<EdgesCountParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_key(&query.edge_type, "edge_type", MAX_EDGE_TYPE_LENGTH)?;
    validate_account_id(&query.target, "target")?;

    tracing::info!(
        target: PROJECT_ID,
        edge_type = %query.edge_type,
        target = %query.target,
        "GET /v1/kv/edges/count"
    );

    let db = require_db(&app_state).await?;
    let count = db.as_ref().ok_or(ApiError::DatabaseUnavailable)?
        .count_edges(&query.edge_type, &query.target)
        .await?;

    Ok(HttpResponse::Ok().json(EdgesCountResponse {
        edge_type: query.edge_type.clone(),
        target: query.target.clone(),
        count,
    }))
}


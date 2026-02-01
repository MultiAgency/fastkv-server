use actix_web::{get, post, web, HttpResponse};
use crate::models::*;
use crate::tree::build_tree;
use crate::AppState;

use std::collections::HashSet;

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

fn validate_account_id(value: &str, name: &str) -> Result<(), ApiError> {
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

fn validate_key(value: &str, name: &str, max_len: usize) -> Result<(), ApiError> {
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

fn validate_offset(offset: usize) -> Result<(), ApiError> {
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
    // Try a simple query to verify DB connection
    match app_state.scylladb.health_check().await {
        Ok(_) => Ok(HttpResponse::Ok().json(HealthResponse {
            status: "ok".to_string(),
        })),
        Err(e) => {
            tracing::warn!(target: PROJECT_ID, error = %e, "Health check failed");
            Ok(HttpResponse::ServiceUnavailable().json(HealthResponse {
                status: "database_unavailable".to_string(),
            }))
        }
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

    let entry = app_state
        .scylladb
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

    let entries = app_state
        .scylladb
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

    let entries = app_state
        .scylladb
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

    let entries = app_state
        .scylladb
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

    let entries = app_state
        .scylladb
        .query_by_key(&query)
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

    use futures::stream::{self, StreamExt};
    let items: Vec<BatchResultItem> = stream::iter(body.keys.iter().map(|key| {
        let scylladb = app_state.scylladb.clone();
        let predecessor_id = body.predecessor_id.clone();
        let current_account_id = body.current_account_id.clone();
        let key = key.clone();
        async move {
            match scylladb.get_kv_last(&predecessor_id, &current_account_id, &key).await {
                Ok(value) => BatchResultItem {
                    key,
                    found: value.is_some(),
                    value,
                    error: None,
                },
                Err(e) => BatchResultItem {
                    key,
                    found: false,
                    value: None,
                    error: Some(e.to_string()),
                },
            }
        }
    }))
    .buffered(10)
    .collect()
    .await;

    Ok(HttpResponse::Ok().json(BatchResponse { results: items }))
}

/// Commit KV data to NEAR blockchain
#[utoipa::path(
    post,
    path = "/v1/kv/commit",
    request_body = CommitRequest,
    responses(
        (status = 200, description = "Successfully committed to NEAR", body = CommitResponse),
        (status = 400, description = "Invalid parameters", body = ApiError),
        (status = 502, description = "NEAR transaction failed", body = ApiError),
        (status = 504, description = "Transaction timed out — check chain before retrying", body = ApiError)
    ),
    tag = "kv"
)]
#[post("/v1/kv/commit")]
pub async fn commit_kv_handler(
    body: web::Json<CommitRequest>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    let near_client = app_state.near_client.as_ref().ok_or_else(|| {
        ApiError::TransactionError("NEAR client not configured".to_string())
    })?;

    if body.keys.is_empty() {
        return Err(ApiError::InvalidParameter("keys cannot be empty".to_string()));
    }
    if body.keys.len() > MAX_COMMIT_KEYS {
        return Err(ApiError::InvalidParameter(
            format!("keys cannot exceed {} items", MAX_COMMIT_KEYS),
        ));
    }

    for ck in &body.keys {
        validate_account_id(&ck.predecessor_id, "predecessor_id")?;
        validate_account_id(&ck.current_account_id, "current_account_id")?;
        validate_key(&ck.key, "key", MAX_KEY_LENGTH)?;
    }

    tracing::info!(target: PROJECT_ID, key_count = body.keys.len(), "POST /v1/kv/commit");

    let fetch_futures: Vec<_> = body.keys.iter().map(|ck| {
        let scylladb = app_state.scylladb.clone();
        let predecessor_id = ck.predecessor_id.clone();
        let current_account_id = ck.current_account_id.clone();
        let key = ck.key.clone();
        async move {
            let value = scylladb
                .get_kv_last(&predecessor_id, &current_account_id, &key)
                .await
                .map_err(|e| ApiError::DatabaseError(format!("Failed to fetch key '{}': {}", key, e)))?;
            match value {
                Some(v) => Ok((key, v)),
                None => Err(ApiError::InvalidParameter(format!("key '{}' not found", key))),
            }
        }
    }).collect();

    let results = futures::future::join_all(fetch_futures).await;
    let mut kv_pairs = Vec::with_capacity(results.len());
    for result in results {
        kv_pairs.push(result?);
    }

    let num_keys = kv_pairs.len();
    let near_timeout = std::time::Duration::from_secs(30);
    let tx_result = actix_web::rt::time::timeout(near_timeout, near_client.commit_batch(kv_pairs)).await;

    let tx_hash = match tx_result {
        Ok(Ok(hash)) => hash,
        Ok(Err(e)) => {
            tracing::error!(target: PROJECT_ID, error = %e, "NEAR transaction failed");
            return Err(ApiError::TransactionError(e.to_string()));
        }
        Err(_) => {
            tracing::error!(target: PROJECT_ID, "NEAR transaction timed out — tx may still land on-chain");
            return Err(ApiError::TransactionTimeout(
                "Transaction timed out after 30s — check chain before retrying".to_string(),
            ));
        }
    };

    tracing::info!(target: PROJECT_ID, tx_hash = %tx_hash, keys = num_keys, "Committed to NEAR");

    Ok(HttpResponse::Ok().json(CommitResponse {
        transaction_hash: tx_hash,
        keys_committed: num_keys,
    }))
}

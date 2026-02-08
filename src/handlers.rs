use crate::models::*;
use crate::scylladb::ScyllaDb;
use crate::tree::build_tree;
use crate::AppState;
use actix_web::{get, post, web, HttpRequest, HttpResponse};
use tokio::sync::RwLockReadGuard;

use std::collections::HashSet;
use std::time::Duration;

const THROTTLE_CLEANUP_THRESHOLD: usize = 10_000;
const THROTTLE_EXPIRY: Duration = Duration::from_secs(600); // 10 minutes

pub(crate) async fn require_db(
    state: &AppState,
) -> Result<RwLockReadGuard<'_, ScyllaDb>, ApiError> {
    let guard = state.scylladb.read().await;
    if guard.is_none() {
        return Err(ApiError::DatabaseUnavailable);
    }
    Ok(RwLockReadGuard::map(guard, |opt| opt.as_ref().unwrap()))
}

/// Attempt to JSON-decode the `"value"` field in a serialized entry.
/// If the value is a JSON string, it is parsed into the decoded JSON type
/// (e.g., `"\"Alice\""` becomes `"Alice"`, `"42"` becomes `42`).
fn decode_value_in_json(json: &mut serde_json::Value) {
    if let Some(map) = json.as_object_mut() {
        if let Some(raw) = map
            .get("value")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
        {
            if let Ok(decoded) = serde_json::from_str::<serde_json::Value>(&raw) {
                map.insert("value".to_string(), decoded);
            }
        }
    }
}

fn respond_paginated(
    entries: Vec<KvEntry>,
    meta: PaginationMeta,
    fields: &Option<HashSet<String>>,
    decode: bool,
) -> HttpResponse {
    if fields.is_some() || decode {
        let filtered: Vec<_> = entries
            .into_iter()
            .map(|e| {
                let mut json = e.to_json_with_fields(fields);
                if decode {
                    decode_value_in_json(&mut json);
                }
                json
            })
            .collect();
        HttpResponse::Ok().json(serde_json::json!({ "data": filtered, "meta": meta }))
    } else {
        HttpResponse::Ok().json(PaginatedResponse {
            data: entries,
            meta,
        })
    }
}

pub(crate) fn validate_account_id(value: &str, name: &str) -> Result<(), ApiError> {
    if value.is_empty() {
        return Err(ApiError::InvalidParameter(format!(
            "{} cannot be empty",
            name
        )));
    }
    if value.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(format!(
            "{} cannot exceed {} characters",
            name, MAX_ACCOUNT_ID_LENGTH
        )));
    }
    Ok(())
}

pub(crate) fn validate_key(value: &str, name: &str, max_len: usize) -> Result<(), ApiError> {
    if value.is_empty() {
        return Err(ApiError::InvalidParameter(format!(
            "{} cannot be empty",
            name
        )));
    }
    if value.len() > max_len {
        return Err(ApiError::InvalidParameter(format!(
            "{} cannot exceed {} characters",
            name, max_len
        )));
    }
    Ok(())
}

pub(crate) fn validate_offset(offset: usize) -> Result<(), ApiError> {
    if offset > MAX_OFFSET {
        return Err(ApiError::InvalidParameter(format!(
            "offset cannot exceed {}",
            MAX_OFFSET
        )));
    }
    Ok(())
}

pub(crate) fn validate_cursor_or_offset(
    cursor: Option<&str>,
    cursor_name: &str,
    offset: usize,
    validate_cursor_fn: impl FnOnce(&str, &str) -> Result<(), ApiError>,
) -> Result<(), ApiError> {
    if let Some(c) = cursor {
        validate_cursor_fn(c, cursor_name)?;
        if offset > 0 {
            return Err(ApiError::InvalidParameter(format!(
                "cannot use both '{}' cursor and 'offset'",
                cursor_name
            )));
        }
    } else {
        validate_offset(offset)?;
    }
    Ok(())
}

fn validate_order(order: &str) -> Result<(), ApiError> {
    if !order.eq_ignore_ascii_case("asc") && !order.eq_ignore_ascii_case("desc") {
        return Err(ApiError::InvalidParameter(
            "order must be 'asc' or 'desc'".to_string(),
        ));
    }
    Ok(())
}

fn validate_block_range(from_block: Option<i64>, to_block: Option<i64>) -> Result<(), ApiError> {
    if from_block.is_some_and(|v| v < 0) || to_block.is_some_and(|v| v < 0) {
        return Err(ApiError::InvalidParameter(
            "block heights cannot be negative".to_string(),
        ));
    }
    if let (Some(from), Some(to)) = (from_block, to_block) {
        if from > to {
            return Err(ApiError::InvalidParameter(
                "from_block must be less than or equal to to_block".to_string(),
            ));
        }
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
            return Err(ApiError::InvalidParameter(format!(
                "key_prefix cannot exceed {} characters",
                MAX_PREFIX_LENGTH
            )));
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
        (status = 200, description = "Entry found or null if not found", body = inline(DataResponse<Option<KvEntry>>)),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/get")]
pub async fn get_kv_handler(
    query: web::Query<GetParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.predecessor_id, "accountId")?;
    validate_account_id(&query.current_account_id, "contractId")?;
    validate_key(&query.key, "key", MAX_KEY_LENGTH)?;

    tracing::info!(
        target: PROJECT_ID,
        accountId = %query.predecessor_id,
        contractId = %query.current_account_id,
        key = %query.key,
        "GET /v1/kv/get"
    );

    let db = require_db(&app_state).await?;
    let entry = db
        .get_kv(&query.predecessor_id, &query.current_account_id, &query.key)
        .await?;

    // Apply field selection and optional value decoding
    let fields = parse_field_set(&query.fields);
    let decode = should_decode(&query.value_format)?;
    match entry {
        Some(entry) => {
            if fields.is_some() || decode {
                let mut json = entry.to_json_with_fields(&fields);
                if decode {
                    decode_value_in_json(&mut json);
                }
                Ok(HttpResponse::Ok().json(serde_json::json!({ "data": json })))
            } else {
                Ok(HttpResponse::Ok().json(DataResponse { data: Some(entry) }))
            }
        }
        None => Ok(HttpResponse::Ok().json(DataResponse {
            data: Option::<KvEntry>::None,
        })),
    }
}

/// Query KV entries with optional prefix filtering and pagination
#[utoipa::path(
    get,
    path = "/v1/kv/query",
    params(QueryParams),
    responses(
        (status = 200, description = "List of matching entries", body = inline(PaginatedResponse<KvEntry>)),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/query")]
pub async fn query_kv_handler(
    query: web::Query<QueryParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.predecessor_id, "accountId")?;
    validate_account_id(&query.current_account_id, "contractId")?;
    validate_limit(query.limit)?;
    validate_prefix(&query.key_prefix)?;

    validate_cursor_or_offset(
        query.after_key.as_deref(),
        "after_key",
        query.offset,
        |c, n| validate_key(c, n, MAX_KEY_LENGTH),
    )?;

    if let Some(ref fmt) = query.format {
        if fmt != "tree" {
            return Err(ApiError::InvalidParameter(
                "format must be 'tree' or omitted".to_string(),
            ));
        }
    }

    tracing::info!(
        target: PROJECT_ID,
        accountId = %query.predecessor_id,
        contractId = %query.current_account_id,
        key_prefix = ?query.key_prefix,
        limit = query.limit,
        offset = query.offset,
        after_key = ?query.after_key,
        "GET /v1/kv/query"
    );

    let db = require_db(&app_state).await?;
    let (entries, has_more, dropped) = db.query_kv_with_pagination(&query).await?;

    if query.format.as_deref() == Some("tree") {
        let items: Vec<(String, String)> = entries.into_iter().map(|e| (e.key, e.value)).collect();
        let tree = build_tree(&items);
        return Ok(HttpResponse::Ok().json(TreeResponse { tree }));
    }

    let next_cursor = entries.last().map(|e| e.key.clone());
    let meta = PaginationMeta {
        has_more,
        truncated: false,
        next_cursor,
        dropped_rows: dropped_to_option(dropped),
    };
    let fields = parse_field_set(&query.fields);
    let decode = should_decode(&query.value_format)?;
    Ok(respond_paginated(entries, meta, &fields, decode))
}

/// Get historical versions of a KV entry
///
/// Scans up to 10,000 rows; `meta.truncated` is true if the cap was hit.
/// Paginate using `from_block`/`to_block` range narrowing + `limit`.
#[utoipa::path(
    get,
    path = "/v1/kv/history",
    params(HistoryParams),
    responses(
        (status = 200, description = "List of historical entries", body = inline(PaginatedResponse<KvEntry>)),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/history")]
pub async fn history_kv_handler(
    query: web::Query<HistoryParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.predecessor_id, "accountId")?;
    validate_account_id(&query.current_account_id, "contractId")?;
    validate_key(&query.key, "key", MAX_KEY_LENGTH)?;
    validate_limit(query.limit)?;

    validate_order(&query.order)?;
    validate_block_range(query.from_block, query.to_block)?;

    tracing::info!(
        target: PROJECT_ID,
        accountId = %query.predecessor_id,
        contractId = %query.current_account_id,
        key = %query.key,
        limit = query.limit,
        order = %query.order,
        from_block = ?query.from_block,
        to_block = ?query.to_block,
        "GET /v1/kv/history"
    );

    let db = require_db(&app_state).await?;
    let (entries, has_more, truncated, dropped) = db.get_kv_history(&query).await?;

    let next_cursor = entries.last().map(|e| e.block_height.to_string());
    let meta = PaginationMeta {
        has_more,
        truncated,
        next_cursor,
        dropped_rows: dropped_to_option(dropped),
    };
    let fields = parse_field_set(&query.fields);
    let decode = should_decode(&query.value_format)?;
    Ok(respond_paginated(entries, meta, &fields, decode))
}

/// Find all writers for a key under a contract, with optional account filter
#[utoipa::path(
    get,
    path = "/v1/kv/writers",
    params(WritersParams),
    responses(
        (status = 200, description = "List of entries from writers", body = inline(PaginatedResponse<KvEntry>)),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/writers")]
pub async fn writers_handler(
    query: web::Query<WritersParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.current_account_id, "contractId")?;
    validate_key(&query.key, "key", MAX_KEY_LENGTH)?;
    validate_limit(query.limit)?;
    if let Some(ref pred) = query.predecessor_id {
        validate_account_id(pred, "accountId")?;
    }

    validate_cursor_or_offset(
        query.after_account.as_deref(),
        "after_account",
        query.offset,
        validate_account_id,
    )?;

    tracing::info!(
        target: PROJECT_ID,
        contractId = %query.current_account_id,
        key = %query.key,
        accountId = ?query.predecessor_id,
        limit = query.limit,
        offset = query.offset,
        after_account = ?query.after_account,
        "GET /v1/kv/writers"
    );

    let db = require_db(&app_state).await?;
    let (entries, has_more, truncated, dropped) = db.query_writers(&query).await?;

    let next_cursor = entries.last().map(|e| e.predecessor_id.clone());
    let meta = PaginationMeta {
        has_more,
        truncated,
        next_cursor,
        dropped_rows: dropped_to_option(dropped),
    };
    let fields = parse_field_set(&query.fields);
    let decode = should_decode(&query.value_format)?;
    Ok(respond_paginated(entries, meta, &fields, decode))
}

/// List unique writer accounts for a contract (or across all contracts with `scan=1`).
///
/// Returns deduplicated predecessor accounts that have written to the given contract.
/// Providing a `key` filter is recommended — omitting it scans the entire contract partition
/// which can be expensive for large contracts. Results are capped at 100,000 unique accounts;
/// if this limit is reached the response will include `meta.truncated: true`.
///
/// When `contractId` is omitted, `scan=1` is required. This performs an expensive full table
/// scan across all contracts, throttled to 1 req/sec per IP. Limit is clamped to 1,000.
#[utoipa::path(
    get,
    path = "/v1/kv/accounts",
    params(AccountsQueryParams),
    responses(
        (status = 200, description = "List of writer accounts", body = inline(PaginatedResponse<String>)),
        (status = 400, description = "Invalid parameters", body = ApiError),
        (status = 429, description = "Too many scan requests", body = ApiError),
        (status = 503, description = "Database unavailable", body = ApiError),
    ),
    tag = "kv"
)]
#[get("/v1/kv/accounts")]
pub async fn accounts_handler(
    req: HttpRequest,
    query: web::Query<AccountsQueryParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    let is_scan = query.contract_id.is_none();

    if is_scan {
        // Full table scan: require explicit opt-in
        if query.scan != Some(1) {
            return Err(ApiError::InvalidParameter(
                "contractId is optional but requires scan=1 (expensive).".to_string(),
            ));
        }
        if query.key.is_some() {
            return Err(ApiError::InvalidParameter(
                "key filter requires contractId".to_string(),
            ));
        }
        if query.offset > 0 {
            return Err(ApiError::InvalidParameter(
                "offset not supported without contractId; use after_account cursor".to_string(),
            ));
        }
    } else {
        validate_account_id(query.contract_id.as_deref().unwrap(), "contractId")?;
    }

    let limit = if is_scan {
        query.limit.min(MAX_SCAN_LIMIT)
    } else {
        query.limit
    };
    validate_limit(limit)?;

    if let Some(ref key) = query.key {
        validate_key(key, "key", MAX_KEY_LENGTH)?;
    }

    validate_cursor_or_offset(
        query.after_account.as_deref(),
        "after_account",
        query.offset,
        validate_account_id,
    )?;

    // Per-IP throttle for scan requests
    if is_scan {
        let ip = req
            .headers()
            .get("X-Forwarded-For")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.split(',').next())
            .map(|s| s.trim())
            .filter(|s| !s.is_empty() && *s != "unknown")
            .map(|s| s.to_string())
            .or_else(|| {
                req.connection_info()
                    .realip_remote_addr()
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| {
                req.connection_info()
                    .peer_addr()
                    .unwrap_or("unknown")
                    .to_string()
            });
        let mut throttle = app_state.scan_throttle.lock().unwrap_or_else(|e| e.into_inner());
        let now = std::time::Instant::now();
        if throttle.len() > THROTTLE_CLEANUP_THRESHOLD {
            let cutoff = now - THROTTLE_EXPIRY;
            throttle.retain(|_, ts| *ts > cutoff);
        }
        if let Some(last) = throttle.get(&ip) {
            if now.duration_since(*last) < std::time::Duration::from_secs(1) {
                return Err(ApiError::TooManyRequests(
                    "Too many scan requests. Try again shortly.".to_string(),
                ));
            }
        }
        throttle.insert(ip, now);
    }

    tracing::info!(
        target: PROJECT_ID,
        contractId = ?query.contract_id,
        scan = is_scan,
        key = ?query.key,
        limit = limit,
        offset = query.offset,
        after_account = ?query.after_account,
        "GET /v1/kv/accounts"
    );

    let db = require_db(&app_state).await?;

    let (accounts, has_more, truncated, dropped) = if is_scan {
        let (accounts, has_more, dropped) = db
            .query_all_accounts(limit, query.after_account.as_deref())
            .await?;
        (accounts, has_more, false, dropped)
    } else {
        db.query_accounts_by_contract(
            query.contract_id.as_deref().unwrap(),
            query.key.as_deref(),
            limit,
            query.offset,
            query.after_account.as_deref(),
        )
        .await?
    };

    let next_cursor = accounts.last().cloned();
    let meta = PaginationMeta {
        has_more,
        truncated,
        next_cursor,
        dropped_rows: dropped_to_option(dropped),
    };

    Ok(HttpResponse::Ok().json(PaginatedResponse {
        data: accounts,
        meta,
    }))
}

/// Compare a key's value at two different block heights
#[utoipa::path(
    get,
    path = "/v1/kv/diff",
    params(DiffParams),
    responses(
        (status = 200, description = "Values at both block heights", body = inline(DataResponse<DiffResponse>)),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/diff")]
pub async fn diff_kv_handler(
    query: web::Query<DiffParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.predecessor_id, "accountId")?;
    validate_account_id(&query.current_account_id, "contractId")?;
    validate_key(&query.key, "key", MAX_KEY_LENGTH)?;

    tracing::info!(
        target: PROJECT_ID,
        accountId = %query.predecessor_id,
        contractId = %query.current_account_id,
        key = %query.key,
        block_height_a = query.block_height_a,
        block_height_b = query.block_height_b,
        "GET /v1/kv/diff"
    );

    let db = require_db(&app_state).await?;
    let (a, b) = futures::future::try_join(
        db.get_kv_at_block(
            &query.predecessor_id,
            &query.current_account_id,
            &query.key,
            query.block_height_a,
        ),
        db.get_kv_at_block(
            &query.predecessor_id,
            &query.current_account_id,
            &query.key,
            query.block_height_b,
        ),
    )
    .await?;

    let fields = parse_field_set(&query.fields);
    let decode = should_decode(&query.value_format)?;
    if fields.is_some() || decode {
        let mut a_json = a.as_ref().map(|e| e.to_json_with_fields(&fields));
        let mut b_json = b.as_ref().map(|e| e.to_json_with_fields(&fields));
        if decode {
            if let Some(ref mut v) = a_json {
                decode_value_in_json(v);
            }
            if let Some(ref mut v) = b_json {
                decode_value_in_json(v);
            }
        }
        Ok(HttpResponse::Ok().json(serde_json::json!({ "data": { "a": a_json, "b": b_json } })))
    } else {
        Ok(HttpResponse::Ok().json(DataResponse {
            data: DiffResponse { a, b },
        }))
    }
}

/// All writes by one account across all keys, ordered by block_height
///
/// Scans up to 10,000 rows; `meta.truncated` is true if the cap was hit.
/// `from_block`/`to_block` are filtered in-memory (not CQL pushdown).
#[utoipa::path(
    get,
    path = "/v1/kv/timeline",
    params(TimelineParams),
    responses(
        (status = 200, description = "Chronological list of all writes", body = inline(PaginatedResponse<KvEntry>)),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/timeline")]
pub async fn timeline_kv_handler(
    query: web::Query<TimelineParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&query.predecessor_id, "accountId")?;
    validate_account_id(&query.current_account_id, "contractId")?;
    validate_limit(query.limit)?;
    validate_offset(query.offset)?;

    validate_order(&query.order)?;
    validate_block_range(query.from_block, query.to_block)?;

    tracing::info!(
        target: PROJECT_ID,
        accountId = %query.predecessor_id,
        contractId = %query.current_account_id,
        limit = query.limit,
        offset = query.offset,
        order = %query.order,
        from_block = ?query.from_block,
        to_block = ?query.to_block,
        "GET /v1/kv/timeline"
    );

    let db = require_db(&app_state).await?;
    let (entries, has_more, truncated, dropped) = db.get_kv_timeline(&query).await?;

    let next_cursor = entries.last().map(|e| e.block_height.to_string());
    let meta = PaginationMeta {
        has_more,
        truncated,
        next_cursor,
        dropped_rows: dropped_to_option(dropped),
    };
    let fields = parse_field_set(&query.fields);
    let decode = should_decode(&query.value_format)?;
    Ok(respond_paginated(entries, meta, &fields, decode))
}

/// Batch lookup: get values for multiple keys in a single request
#[utoipa::path(
    post,
    path = "/v1/kv/batch",
    request_body = BatchQuery,
    responses(
        (status = 200, description = "Batch results", body = inline(DataResponse<Vec<BatchResultItem>>)),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[post("/v1/kv/batch")]
pub async fn batch_kv_handler(
    body: web::Json<BatchQuery>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_account_id(&body.predecessor_id, "accountId")?;
    validate_account_id(&body.current_account_id, "contractId")?;
    if body.keys.is_empty() {
        return Err(ApiError::InvalidParameter(
            "keys cannot be empty".to_string(),
        ));
    }
    if body.keys.len() > MAX_BATCH_KEYS {
        return Err(ApiError::InvalidParameter(format!(
            "keys cannot exceed {} items",
            MAX_BATCH_KEYS
        )));
    }
    for key in &body.keys {
        if key.is_empty() {
            return Err(ApiError::InvalidParameter(
                "key in batch cannot be empty".to_string(),
            ));
        }
        if key.len() > MAX_BATCH_KEY_LENGTH {
            return Err(ApiError::InvalidParameter(format!(
                "each key cannot exceed {} characters",
                MAX_BATCH_KEY_LENGTH
            )));
        }
    }

    tracing::info!(
        target: PROJECT_ID,
        accountId = %body.predecessor_id,
        contractId = %body.current_account_id,
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

    Ok(HttpResponse::Ok().json(DataResponse { data: items }))
}

/// List edge sources for a given edge type and target
#[utoipa::path(
    get,
    path = "/v1/kv/edges",
    params(EdgesParams),
    responses(
        (status = 200, description = "List of edge sources", body = inline(PaginatedResponse<EdgeSourceEntry>)),
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

    validate_cursor_or_offset(
        query.after_source.as_deref(),
        "after_source",
        query.offset,
        validate_account_id,
    )?;

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
    let (sources, has_more, dropped) = db
        .query_edges(
            &query.edge_type,
            &query.target,
            query.limit,
            query.offset,
            query.after_source.as_deref(),
        )
        .await?;

    let next_cursor = sources.last().map(|e| e.source.clone());
    let meta = PaginationMeta {
        has_more,
        truncated: false,
        next_cursor,
        dropped_rows: dropped_to_option(dropped),
    };

    Ok(HttpResponse::Ok().json(PaginatedResponse {
        data: sources,
        meta,
    }))
}

/// Count edges for a given edge type and target
#[utoipa::path(
    get,
    path = "/v1/kv/edges/count",
    params(EdgesCountParams),
    responses(
        (status = 200, description = "Edge count", body = inline(DataResponse<EdgesCountResponse>)),
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
    let count = db.count_edges(&query.edge_type, &query.target).await?;

    Ok(HttpResponse::Ok().json(DataResponse {
        data: EdgesCountResponse {
            edge_type: query.edge_type.clone(),
            target: query.target.clone(),
            count,
        },
    }))
}

/// Indexer status: block height and server time
#[utoipa::path(
    get,
    path = "/v1/status",
    responses(
        (status = 200, description = "Indexer status", body = StatusResponse),
    ),
    tag = "kv"
)]
#[get("/v1/status")]
pub async fn status_handler(app_state: web::Data<AppState>) -> HttpResponse {
    let guard = app_state.scylladb.read().await;
    let indexer_block = match guard.as_ref() {
        Some(db) => db.get_indexer_block_height().await.ok().flatten(),
        None => None,
    };

    HttpResponse::Ok().json(StatusResponse {
        indexer_block,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

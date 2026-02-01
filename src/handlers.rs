use actix_web::{get, web, HttpResponse};
use crate::models::*;
use crate::AppState;

const PROJECT_ID: &str = "fastkv-server";
const MAX_OFFSET: usize = 100_000;
const MAX_PREFIX_LENGTH: usize = 1000;
const MAX_ACCOUNT_ID_LENGTH: usize = 256;
const MAX_KEY_LENGTH: usize = 10000;

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
        Err(_) => Ok(HttpResponse::ServiceUnavailable().json(HealthResponse {
            status: "database_unavailable".to_string(),
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
    // Validate required parameters
    if query.predecessor_id.is_empty() {
        return Err(ApiError::InvalidParameter(
            "predecessor_id cannot be empty".to_string(),
        ));
    }
    if query.predecessor_id.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("predecessor_id cannot exceed {} characters", MAX_ACCOUNT_ID_LENGTH),
        ));
    }
    if query.current_account_id.is_empty() {
        return Err(ApiError::InvalidParameter(
            "current_account_id cannot be empty".to_string(),
        ));
    }
    if query.current_account_id.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("current_account_id cannot exceed {} characters", MAX_ACCOUNT_ID_LENGTH),
        ));
    }
    if query.key.is_empty() {
        return Err(ApiError::InvalidParameter(
            "key cannot be empty".to_string(),
        ));
    }
    if query.key.len() > MAX_KEY_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("key cannot exceed {} characters", MAX_KEY_LENGTH),
        ));
    }

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
    let fields = query.parse_fields();
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
    // Validate required parameters
    if query.predecessor_id.is_empty() {
        return Err(ApiError::InvalidParameter(
            "predecessor_id cannot be empty".to_string(),
        ));
    }
    if query.predecessor_id.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("predecessor_id cannot exceed {} characters", MAX_ACCOUNT_ID_LENGTH),
        ));
    }
    if query.current_account_id.is_empty() {
        return Err(ApiError::InvalidParameter(
            "current_account_id cannot be empty".to_string(),
        ));
    }
    if query.current_account_id.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("current_account_id cannot exceed {} characters", MAX_ACCOUNT_ID_LENGTH),
        ));
    }

    // Validate limit
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter(
            "limit must be between 1 and 1000".to_string(),
        ));
    }

    // Validate offset
    if query.offset > MAX_OFFSET {
        return Err(ApiError::InvalidParameter(
            format!("offset cannot exceed {}", MAX_OFFSET),
        ));
    }

    // Validate key_prefix is not empty if provided
    if let Some(ref prefix) = query.key_prefix {
        if prefix.is_empty() {
            return Err(ApiError::InvalidParameter(
                "key_prefix cannot be empty string (omit parameter if not filtering)".to_string(),
            ));
        }
        if prefix.len() > MAX_PREFIX_LENGTH {
            return Err(ApiError::InvalidParameter(
                format!("key_prefix cannot exceed {} characters", MAX_PREFIX_LENGTH),
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

    // Apply field selection
    let fields = query.parse_fields();
    if fields.is_some() {
        let filtered: Vec<_> = entries
            .into_iter()
            .map(|e| e.to_json_with_fields(&fields))
            .collect();
        Ok(HttpResponse::Ok().json(serde_json::json!({ "entries": filtered })))
    } else {
        Ok(HttpResponse::Ok().json(QueryResponse { entries }))
    }
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
    // Validate required parameters
    if query.predecessor_id.is_empty() {
        return Err(ApiError::InvalidParameter(
            "predecessor_id cannot be empty".to_string(),
        ));
    }
    if query.predecessor_id.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("predecessor_id cannot exceed {} characters", MAX_ACCOUNT_ID_LENGTH),
        ));
    }
    if query.current_account_id.is_empty() {
        return Err(ApiError::InvalidParameter(
            "current_account_id cannot be empty".to_string(),
        ));
    }
    if query.current_account_id.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("current_account_id cannot exceed {} characters", MAX_ACCOUNT_ID_LENGTH),
        ));
    }
    if query.key.is_empty() {
        return Err(ApiError::InvalidParameter(
            "key cannot be empty".to_string(),
        ));
    }
    if query.key.len() > MAX_KEY_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("key cannot exceed {} characters", MAX_KEY_LENGTH),
        ));
    }

    // Validate limit
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter(
            "limit must be between 1 and 1000".to_string(),
        ));
    }

    // Validate order parameter
    let order_lower = query.order.to_lowercase();
    if order_lower != "asc" && order_lower != "desc" {
        return Err(ApiError::InvalidParameter(
            "order must be 'asc' or 'desc'".to_string(),
        ));
    }

    // Validate block range if provided
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

    // Apply field selection
    let fields = query.parse_fields();
    if fields.is_some() {
        let filtered: Vec<_> = entries
            .into_iter()
            .map(|e| e.to_json_with_fields(&fields))
            .collect();
        Ok(HttpResponse::Ok().json(serde_json::json!({ "entries": filtered })))
    } else {
        Ok(HttpResponse::Ok().json(QueryResponse { entries }))
    }
}

/// Count KV entries for a given predecessor and account
#[utoipa::path(
    get,
    path = "/v1/kv/count",
    params(CountParams),
    responses(
        (status = 200, description = "Count of matching entries", body = CountResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "kv"
)]
#[get("/v1/kv/count")]
pub async fn count_kv_handler(
    query: web::Query<CountParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    // Validate required parameters
    if query.predecessor_id.is_empty() {
        return Err(ApiError::InvalidParameter(
            "predecessor_id cannot be empty".to_string(),
        ));
    }
    if query.predecessor_id.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("predecessor_id cannot exceed {} characters", MAX_ACCOUNT_ID_LENGTH),
        ));
    }
    if query.current_account_id.is_empty() {
        return Err(ApiError::InvalidParameter(
            "current_account_id cannot be empty".to_string(),
        ));
    }
    if query.current_account_id.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("current_account_id cannot exceed {} characters", MAX_ACCOUNT_ID_LENGTH),
        ));
    }

    // Validate key_prefix is not empty if provided
    if let Some(ref prefix) = query.key_prefix {
        if prefix.is_empty() {
            return Err(ApiError::InvalidParameter(
                "key_prefix cannot be empty string (omit parameter if not filtering)".to_string(),
            ));
        }
        if prefix.len() > MAX_PREFIX_LENGTH {
            return Err(ApiError::InvalidParameter(
                format!("key_prefix cannot exceed {} characters", MAX_PREFIX_LENGTH),
            ));
        }
    }

    tracing::info!(
        target: PROJECT_ID,
        predecessor_id = %query.predecessor_id,
        current_account_id = %query.current_account_id,
        key_prefix = ?query.key_prefix,
        "GET /v1/kv/count"
    );

    let (count, estimated) = app_state
        .scylladb
        .count_kv(&query)
        .await?;

    Ok(HttpResponse::Ok().json(CountResponse {
        count,
        estimated,
    }))
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
    // Validate required parameters
    if query.current_account_id.is_empty() {
        return Err(ApiError::InvalidParameter(
            "current_account_id cannot be empty".to_string(),
        ));
    }
    if query.current_account_id.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("current_account_id cannot exceed {} characters", MAX_ACCOUNT_ID_LENGTH),
        ));
    }
    if query.key.is_empty() {
        return Err(ApiError::InvalidParameter(
            "key cannot be empty".to_string(),
        ));
    }
    if query.key.len() > MAX_KEY_LENGTH {
        return Err(ApiError::InvalidParameter(
            format!("key cannot exceed {} characters", MAX_KEY_LENGTH),
        ));
    }

    // Validate limit
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter(
            "limit must be between 1 and 1000".to_string(),
        ));
    }

    // Validate offset
    if query.offset > MAX_OFFSET {
        return Err(ApiError::InvalidParameter(
            format!("offset cannot exceed {}", MAX_OFFSET),
        ));
    }

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

    // Apply field selection
    let fields = query.parse_fields();
    if fields.is_some() {
        let filtered: Vec<_> = entries
            .into_iter()
            .map(|e| e.to_json_with_fields(&fields))
            .collect();
        Ok(HttpResponse::Ok().json(serde_json::json!({ "entries": filtered })))
    } else {
        Ok(HttpResponse::Ok().json(QueryResponse { entries }))
    }
}

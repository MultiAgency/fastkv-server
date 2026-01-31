use actix_web::{get, web, HttpResponse};
use crate::models::*;
use crate::AppState;

const PROJECT_ID: &str = "fastkv-server";

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Service is healthy", body = HealthResponse)
    ),
    tag = "health"
)]
#[get("/health")]
pub async fn health_check() -> Result<HttpResponse, ApiError> {
    Ok(HttpResponse::Ok().json(HealthResponse { status: "ok" }))
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
    if query.predecessor_id.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "predecessor_id cannot be empty".to_string(),
        ));
    }
    if query.current_account_id.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "current_account_id cannot be empty".to_string(),
        ));
    }
    if query.key.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "key cannot be empty".to_string(),
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
    if query.predecessor_id.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "predecessor_id cannot be empty".to_string(),
        ));
    }
    if query.current_account_id.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "current_account_id cannot be empty".to_string(),
        ));
    }

    // Validate limit
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter(
            "limit must be between 1 and 1000".to_string(),
        ));
    }

    // Validate key_prefix is not empty if provided
    if let Some(ref prefix) = query.key_prefix {
        if prefix.is_empty() {
            return Err(ApiError::InvalidParameter(
                "key_prefix cannot be empty string (omit parameter if not filtering)".to_string(),
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
    if query.predecessor_id.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "predecessor_id cannot be empty".to_string(),
        ));
    }
    if query.current_account_id.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "current_account_id cannot be empty".to_string(),
        ));
    }
    if query.key.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "key cannot be empty".to_string(),
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
    if query.predecessor_id.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "predecessor_id cannot be empty".to_string(),
        ));
    }
    if query.current_account_id.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "current_account_id cannot be empty".to_string(),
        ));
    }

    // Validate key_prefix is not empty if provided
    if let Some(ref prefix) = query.key_prefix {
        if prefix.is_empty() {
            return Err(ApiError::InvalidParameter(
                "key_prefix cannot be empty string (omit parameter if not filtering)".to_string(),
            ));
        }
    }

    tracing::info!(
        target: PROJECT_ID,
        predecessor_id = %query.predecessor_id,
        current_account_id = %query.current_account_id,
        key_prefix = ?query.key_prefix,
        accurate = query.accurate,
        "GET /v1/kv/count"
    );

    let count = app_state
        .scylladb
        .count_kv(&query)
        .await?;

    // Currently always accurate (we count actual rows)
    // In the future, could add estimation logic when accurate=false
    Ok(HttpResponse::Ok().json(CountResponse {
        count,
        estimated: false,
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
    if query.current_account_id.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "current_account_id cannot be empty".to_string(),
        ));
    }
    if query.key.trim().is_empty() {
        return Err(ApiError::InvalidParameter(
            "key cannot be empty".to_string(),
        ));
    }

    // Validate limit
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter(
            "limit must be between 1 and 1000".to_string(),
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

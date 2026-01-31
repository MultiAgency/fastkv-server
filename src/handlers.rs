use actix_web::{get, web, HttpResponse};
use crate::models::*;
use crate::AppState;

const PROJECT_ID: &str = "fastkv-server";

#[get("/health")]
pub async fn health_check() -> Result<HttpResponse, ApiError> {
    Ok(HttpResponse::Ok().json(HealthResponse { status: "ok" }))
}

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

    // Return unwrapped entry or null
    match entry {
        Some(entry) => Ok(HttpResponse::Ok().json(entry)),
        None => Ok(HttpResponse::Ok().json(serde_json::Value::Null)),
    }
}

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

    Ok(HttpResponse::Ok().json(QueryResponse { entries }))
}

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

    Ok(HttpResponse::Ok().json(QueryResponse { entries }))
}

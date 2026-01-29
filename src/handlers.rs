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
    tracing::info!(
        target: PROJECT_ID,
        "GET /v1/kv/get predecessor_id={} current_account_id={} key={}",
        query.predecessor_id,
        query.current_account_id,
        query.key
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
    // Validate limit
    if query.limit > 1000 {
        return Err(ApiError::InvalidParameter(
            "limit cannot exceed 1000".to_string(),
        ));
    }

    tracing::info!(
        target: PROJECT_ID,
        "GET /v1/kv/query predecessor_id={} current_account_id={} key_prefix={:?} limit={} offset={}",
        query.predecessor_id,
        query.current_account_id,
        query.key_prefix,
        query.limit,
        query.offset
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
    // Validate limit
    if query.limit > 1000 {
        return Err(ApiError::InvalidParameter(
            "limit cannot exceed 1000".to_string(),
        ));
    }

    tracing::info!(
        target: PROJECT_ID,
        "GET /v1/kv/reverse current_account_id={} key={} limit={} offset={}",
        query.current_account_id,
        query.key,
        query.limit,
        query.offset
    );

    let entries = app_state
        .scylladb
        .reverse_kv_with_dedup(&query)
        .await?;

    Ok(HttpResponse::Ok().json(QueryResponse { entries }))
}

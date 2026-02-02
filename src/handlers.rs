use actix_web::{get, post, web, HttpResponse, http::header};
use crate::models::*;
use crate::tree::build_tree;
use crate::AppState;

use std::collections::HashSet;

#[get("/")]
pub async fn index() -> HttpResponse {
    HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, "text/html; charset=utf-8"))
        .insert_header(("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; img-src data:;"))
        .body(r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NEAR Garden</title>
<meta name="description" content="Read-only API for NEAR on-chain key-value data and SocialDB. No API key required.">
<meta property="og:title" content="NEAR Garden">
<meta property="og:description" content="Read-only API for NEAR on-chain key-value data and SocialDB. No API key required.">
<meta property="og:type" content="website">
<meta property="og:url" content="https://fastdata.up.railway.app">
<meta name="twitter:card" content="summary">
<meta name="twitter:title" content="NEAR Garden">
<meta name="twitter:description" content="Read-only API for NEAR on-chain key-value data and SocialDB. No API key required.">
<link rel="canonical" href="https://fastdata.up.railway.app">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#127793;</text></svg>">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0a0f;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,monospace;min-height:100vh;display:flex;align-items:center;justify-content:center}
main{max-width:640px;padding:3rem 2rem}
h1{font-size:2.4rem;font-weight:700;background:linear-gradient(135deg,#6ef,#a78bfa);-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;color:transparent;margin-bottom:.5rem}
.tag{display:inline-block;font-size:.75rem;background:#1a1a2e;border:1px solid #333;border-radius:4px;padding:2px 8px;margin-bottom:1.5rem;color:#aaa}
p.desc{color:#b0b0b0;line-height:1.6;margin-bottom:2rem}
h2{font-size:.85rem;text-transform:uppercase;letter-spacing:.1em;color:#888;margin:1.5rem 0 .5rem}
ul{list-style:none}
li a{display:block;padding:4px 0;font-size:.9rem;color:#aaa;text-decoration:none;transition:color .15s}
li a:hover{color:#6ef}
li a code{color:#6ef;margin-right:6px;font-size:.75rem;font-family:inherit}
.example{margin-top:1.5rem;background:#111119;border:1px solid #1a1a2e;border-radius:6px;padding:.8rem 1rem;font-size:.8rem;color:#aaa;overflow-x:auto;white-space:pre}
.example span{color:#6ef}
.btns{display:flex;gap:1rem;flex-wrap:wrap;margin-top:2rem}
a.btn{display:inline-block;padding:.7rem 1.8rem;background:linear-gradient(135deg,#6ef,#a78bfa);color:#0a0a0f;font-weight:600;text-decoration:none;border-radius:6px;font-size:.95rem;transition:opacity .2s}
a.btn:hover{opacity:.85}
a.btn.alt{background:linear-gradient(135deg,#a78bfa,#f0abfc)}
footer{margin-top:2rem;padding-top:1.5rem;border-top:1px solid #1a1a2e}
footer p{color:#888;font-size:.75rem}
footer a{color:#6ef;text-decoration:none}
footer a:hover{text-decoration:underline}
@media(max-width:400px){main{padding:2rem 1.2rem}h1{font-size:1.8rem}}
</style>
</head>
<body>
<main>
<header>
<h1>NEAR Garden</h1>
<span class="tag">v1 &middot; NEAR Protocol</span>
</header>
<p class="desc">Read-only API for NEAR on-chain key-value data and SocialDB. No API key required.</p>
<h2>Key-Value</h2>
<ul>
<li><a href="/docs"><code>GET</code> /v1/kv/get</a></li>
<li><a href="/docs"><code>GET</code> /v1/kv/query</a></li>
<li><a href="/docs"><code>GET</code> /v1/kv/history</a></li>
<li><a href="/docs"><code>GET</code> /v1/kv/reverse</a></li>
<li><a href="/docs"><code>GET</code> /v1/kv/by-key</a></li>
<li><a href="/docs"><code>GET</code> /v1/kv/diff</a></li>
<li><a href="/docs"><code>GET</code> /v1/kv/timeline</a></li>
<li><a href="/docs"><code>POST</code> /v1/kv/batch</a></li>
</ul>
<h2>Social</h2>
<ul>
<li><a href="/docs"><code>POST</code> /v1/social/get</a></li>
<li><a href="/docs"><code>POST</code> /v1/social/keys</a></li>
<li><a href="/docs"><code>GET</code> /v1/social/index</a></li>
<li><a href="/docs"><code>GET</code> /v1/social/profile</a></li>
<li><a href="/docs"><code>GET</code> /v1/social/followers</a></li>
<li><a href="/docs"><code>GET</code> /v1/social/following</a></li>
<li><a href="/docs"><code>GET</code> /v1/social/feed/account</a></li>
</ul>
<div class="example"><span>$</span> curl https://fastdata.up.railway.app/v1/social/profile?account_id=root.near</div>
<div class="btns">
<a class="btn" href="/docs">API Docs &rarr;</a>
<a class="btn alt" href="https://near.directory">NEAR Directory &rarr;</a>
</div>
<footer>
<p>NEAR Directory is a demo app built using <a href="https://hackmd.io/@fastnear/__fastdata">FastData</a>. Powered by <a href="https://fastnear.com">FastNear</a>.</p>
</footer>
</main>
</body>
</html>"#)
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

    let (a, b) = futures::future::try_join(
        app_state.scylladb.get_kv_at_block(
            &query.predecessor_id, &query.current_account_id, &query.key, query.block_height_a,
        ),
        app_state.scylladb.get_kv_at_block(
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

    let entries = app_state
        .scylladb
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


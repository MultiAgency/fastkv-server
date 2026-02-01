use actix_web::{get, post, web, HttpResponse};
use futures::stream::StreamExt;

use crate::models::*;
use crate::tree::build_tree;
use crate::AppState;

const PROJECT_ID: &str = "fastkv-server";
const SOCIAL_CONTRACT: &str = "social.near";
const MAX_KEYS: usize = 100;
const MAX_ACCOUNT_ID_LENGTH: usize = 256;

// ===== Key pattern parsing =====

enum KeyPattern {
    /// Exact key lookup for a specific account: "alice.near/post/main"
    Exact {
        account_id: String,
        key: String,
    },
    /// Recursive wildcard: "alice.near/profile/**" -> prefix query
    RecursiveWildcard {
        account_id: String,
        key_prefix: String,
    },
    /// Single-level wildcard: "alice.near/profile/*" -> prefix query, filter depth
    SingleWildcard {
        account_id: String,
        key_prefix: String,
    },
    /// Wildcard account with exact key: "*/widget/name"
    WildcardAccount {
        key: String,
    },
}

fn parse_social_key(pattern: &str) -> Result<KeyPattern, ApiError> {
    if pattern.is_empty() {
        return Err(ApiError::InvalidParameter("key pattern cannot be empty".to_string()));
    }

    let (account_part, key_part) = match pattern.find('/') {
        Some(idx) => (&pattern[..idx], &pattern[idx + 1..]),
        None => {
            // Just an account ID with no key path — treat as recursive wildcard on all keys
            return Ok(KeyPattern::RecursiveWildcard {
                account_id: pattern.to_string(),
                key_prefix: String::new(),
            });
        }
    };

    if account_part == "*" {
        // Wildcard account — only exact key supported
        if key_part.contains('*') {
            return Err(ApiError::InvalidParameter(
                "wildcard account with wildcard key (*/prefix/**) is not supported".to_string(),
            ));
        }
        return Ok(KeyPattern::WildcardAccount {
            key: key_part.to_string(),
        });
    }

    if account_part.len() > MAX_ACCOUNT_ID_LENGTH {
        return Err(ApiError::InvalidParameter(
            "account ID too long".to_string(),
        ));
    }

    if key_part.ends_with("/**") {
        let prefix = &key_part[..key_part.len() - 2]; // strip "**", keep trailing "/"
        Ok(KeyPattern::RecursiveWildcard {
            account_id: account_part.to_string(),
            key_prefix: prefix.to_string(),
        })
    } else if key_part.ends_with("/*") {
        let prefix = &key_part[..key_part.len() - 1]; // strip "*", keep trailing "/"
        Ok(KeyPattern::SingleWildcard {
            account_id: account_part.to_string(),
            key_prefix: prefix.to_string(),
        })
    } else {
        Ok(KeyPattern::Exact {
            account_id: account_part.to_string(),
            key: key_part.to_string(),
        })
    }
}

// ===== Helper: query index entries from reverse view =====

async fn query_index(
    app_state: &AppState,
    action: &str,
    key: &str,
    order: &str,
    limit: usize,
    from: Option<u64>,
) -> Result<Vec<IndexEntry>, ApiError> {
    let index_key = format!("index/{}/{}", action, key);

    let mut rows_stream = app_state
        .scylladb
        .scylla_session
        .execute_iter(
            app_state.scylladb.reverse_kv.clone(),
            (SOCIAL_CONTRACT, &index_key),
        )
        .await
        .map_err(anyhow::Error::from)?
        .rows_stream::<KvRow>()
        .map_err(anyhow::Error::from)?;

    let mut entries: Vec<IndexEntry> = Vec::new();

    while let Some(row_result) = rows_stream.next().await {
        let row = match row_result {
            Ok(row) => row,
            Err(e) => {
                tracing::warn!(target: PROJECT_ID, error = %e, "Failed to deserialize row in query_index");
                continue;
            }
        };

        let block_height = row.block_height.max(0) as u64;

        // Apply cursor filter
        if let Some(from_block) = from {
            if order == "desc" && block_height >= from_block {
                continue;
            }
            if order == "asc" && block_height <= from_block {
                continue;
            }
        }

        let value = serde_json::from_str(&row.value).ok();

        entries.push(IndexEntry {
            account_id: row.predecessor_id,
            block_height,
            value,
        });

        if entries.len() >= limit {
            break;
        }
    }

    // Sort by block_height
    if order == "asc" {
        entries.sort_by_key(|e| e.block_height);
    } else {
        entries.sort_by(|a, b| b.block_height.cmp(&a.block_height));
    }

    entries.truncate(limit);
    Ok(entries)
}

// ===== Handlers =====

/// SocialDB-compatible get: read data by key patterns
#[utoipa::path(
    post,
    path = "/v1/social/get",
    request_body = SocialGetBody,
    responses(
        (status = 200, description = "Nested JSON matching SocialDB get() format"),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[post("/v1/social/get")]
pub async fn social_get_handler(
    body: web::Json<SocialGetBody>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    if body.keys.is_empty() {
        return Err(ApiError::InvalidParameter("keys cannot be empty".to_string()));
    }
    if body.keys.len() > MAX_KEYS {
        return Err(ApiError::InvalidParameter(
            format!("keys cannot exceed {} patterns", MAX_KEYS),
        ));
    }

    let with_block_height = body.options.as_ref()
        .and_then(|o| o.with_block_height)
        .unwrap_or(false);
    let return_deleted = body.options.as_ref()
        .and_then(|o| o.return_deleted)
        .unwrap_or(false);

    tracing::info!(target: PROJECT_ID, key_count = body.keys.len(), "POST /v1/social/get");

    let mut result_root = serde_json::Map::new();

    for pattern in &body.keys {
        let parsed = parse_social_key(pattern)?;

        match parsed {
            KeyPattern::Exact { account_id, key } => {
                let entry = app_state.scylladb
                    .get_kv(&account_id, SOCIAL_CONTRACT, &key)
                    .await?;

                if let Some(entry) = entry {
                    let items = vec![(entry.key.clone(), entry.value.clone())];
                    let subtree = build_tree(&items);
                    let account_obj = result_root
                        .entry(account_id)
                        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                    if let serde_json::Value::Object(ref mut obj) = account_obj {
                        merge_json(obj, &subtree);
                        if with_block_height {
                            insert_block_height(obj, &entry.key, entry.block_height);
                        }
                    }
                } else if return_deleted {
                    let account_obj = result_root
                        .entry(account_id)
                        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                    if let serde_json::Value::Object(ref mut obj) = account_obj {
                        let items = vec![(key, "null".to_string())];
                        let subtree = build_tree(&items);
                        merge_json(obj, &subtree);
                    }
                }
            }
            KeyPattern::RecursiveWildcard { account_id, key_prefix } => {
                let query = QueryParams {
                    predecessor_id: account_id.clone(),
                    current_account_id: SOCIAL_CONTRACT.to_string(),
                    key_prefix: if key_prefix.is_empty() { None } else { Some(key_prefix) },
                    exclude_null: if return_deleted { Some(false) } else { Some(true) },
                    limit: 1000,
                    offset: 0,
                    fields: None,
                    format: None,
                };

                let entries = app_state.scylladb.query_kv_with_pagination(&query).await?;

                let items: Vec<(String, String)> = entries.iter()
                    .map(|e| (e.key.clone(), e.value.clone()))
                    .collect();
                let subtree = build_tree(&items);

                let account_obj = result_root
                    .entry(account_id)
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                if let serde_json::Value::Object(ref mut obj) = account_obj {
                    merge_json(obj, &subtree);
                    if with_block_height {
                        for e in &entries {
                            insert_block_height(obj, &e.key, e.block_height);
                        }
                    }
                }
            }
            KeyPattern::SingleWildcard { account_id, key_prefix } => {
                let query = QueryParams {
                    predecessor_id: account_id.clone(),
                    current_account_id: SOCIAL_CONTRACT.to_string(),
                    key_prefix: Some(key_prefix.clone()),
                    exclude_null: if return_deleted { Some(false) } else { Some(true) },
                    limit: 1000,
                    offset: 0,
                    fields: None,
                    format: None,
                };

                let entries = app_state.scylladb.query_kv_with_pagination(&query).await?;

                // Filter to single level only: key should be prefix + "something" with no more slashes
                let prefix_depth = key_prefix.matches('/').count();
                let items: Vec<(String, String)> = entries.iter()
                    .filter(|e| {
                        let key_depth = e.key.matches('/').count();
                        key_depth == prefix_depth
                    })
                    .map(|e| (e.key.clone(), e.value.clone()))
                    .collect();
                let subtree = build_tree(&items);

                let account_obj = result_root
                    .entry(account_id)
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                if let serde_json::Value::Object(ref mut obj) = account_obj {
                    merge_json(obj, &subtree);
                }
            }
            KeyPattern::WildcardAccount { key } => {
                // Use by-key view to find all predecessors with this exact key
                let by_key_params = ByKeyParams {
                    key: key.clone(),
                    current_account_id: SOCIAL_CONTRACT.to_string(),
                    limit: 1000,
                    offset: 0,
                    fields: None,
                };
                let entries = app_state.scylladb.query_by_key(&by_key_params).await?;

                for entry in &entries {
                    let items = vec![(entry.key.clone(), entry.value.clone())];
                    let subtree = build_tree(&items);
                    let account_obj = result_root
                        .entry(entry.predecessor_id.clone())
                        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                    if let serde_json::Value::Object(ref mut obj) = account_obj {
                        merge_json(obj, &subtree);
                    }
                }
            }
        }
    }

    Ok(HttpResponse::Ok().json(serde_json::Value::Object(result_root)))
}

/// SocialDB-compatible keys: query key structure
#[utoipa::path(
    post,
    path = "/v1/social/keys",
    request_body = SocialKeysBody,
    responses(
        (status = 200, description = "Nested JSON with key structure"),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[post("/v1/social/keys")]
pub async fn social_keys_handler(
    body: web::Json<SocialKeysBody>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    if body.keys.is_empty() {
        return Err(ApiError::InvalidParameter("keys cannot be empty".to_string()));
    }
    if body.keys.len() > MAX_KEYS {
        return Err(ApiError::InvalidParameter(
            format!("keys cannot exceed {} patterns", MAX_KEYS),
        ));
    }

    let return_block_height = body.options.as_ref()
        .and_then(|o| o.return_type.as_deref())
        .map(|t| t == "BlockHeight")
        .unwrap_or(false);
    let return_deleted = body.options.as_ref()
        .and_then(|o| o.return_deleted)
        .unwrap_or(false);
    let values_only = body.options.as_ref()
        .and_then(|o| o.values_only)
        .unwrap_or(false);

    tracing::info!(target: PROJECT_ID, key_count = body.keys.len(), "POST /v1/social/keys");

    let mut result_root = serde_json::Map::new();

    for pattern in &body.keys {
        let parsed = parse_social_key(pattern)?;

        match parsed {
            KeyPattern::Exact { account_id, key } => {
                let entry = app_state.scylladb
                    .get_kv(&account_id, SOCIAL_CONTRACT, &key)
                    .await?;

                if let Some(entry) = entry {
                    if !return_deleted && entry.value == "null" {
                        continue;
                    }
                    let val = if return_block_height {
                        serde_json::json!(entry.block_height)
                    } else {
                        serde_json::json!(true)
                    };
                    let items = vec![(entry.key, val.to_string())];
                    let subtree = build_tree(&items);
                    let account_obj = result_root
                        .entry(account_id)
                        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                    if let serde_json::Value::Object(ref mut obj) = account_obj {
                        merge_json(obj, &subtree);
                    }
                }
            }
            KeyPattern::SingleWildcard { account_id, key_prefix } | KeyPattern::RecursiveWildcard { account_id, key_prefix } => {
                // keys() only supports * (not **), but we handle both the same way for robustness
                // keys() returns key structure — fetch all keys including deleted
                // unless return_deleted is explicitly false AND values_only is set
                let query = QueryParams {
                    predecessor_id: account_id.clone(),
                    current_account_id: SOCIAL_CONTRACT.to_string(),
                    key_prefix: if key_prefix.is_empty() { None } else { Some(key_prefix.clone()) },
                    exclude_null: Some(false),
                    limit: 1000,
                    offset: 0,
                    fields: None,
                    format: None,
                };

                let entries = app_state.scylladb.query_kv_with_pagination(&query).await?;

                tracing::debug!(
                    target: PROJECT_ID,
                    count = entries.len(),
                    keys = ?entries.iter().map(|e| &e.key).collect::<Vec<_>>(),
                    values = ?entries.iter().map(|e| &e.value).collect::<Vec<_>>(),
                    key_prefix = %key_prefix,
                    predecessor_id = %account_id,
                    "social_keys query results"
                );

                // For SingleWildcard, filter to single level
                let prefix_depth = key_prefix.matches('/').count();
                let is_single = matches!(parse_social_key(pattern)?, KeyPattern::SingleWildcard { .. });

                let after_depth: Vec<&KvEntry> = entries.iter()
                    .filter(|e| !is_single || e.key.matches('/').count() == prefix_depth)
                    .collect();
                tracing::debug!(
                    target: PROJECT_ID,
                    before = entries.len(),
                    after = after_depth.len(),
                    is_single,
                    prefix_depth,
                    "social_keys after depth filter"
                );

                let after_deleted: Vec<&&KvEntry> = after_depth.iter()
                    .filter(|e| return_deleted || e.value != "null")
                    .collect();
                tracing::debug!(
                    target: PROJECT_ID,
                    before = after_depth.len(),
                    after = after_deleted.len(),
                    return_deleted,
                    "social_keys after return_deleted filter"
                );

                let filtered: Vec<&KvEntry> = after_deleted.into_iter()
                    .filter(|e| !values_only || !is_intermediate_key(e, &entries))
                    .copied()
                    .collect();
                tracing::debug!(
                    target: PROJECT_ID,
                    before = after_depth.len(),
                    after = filtered.len(),
                    values_only,
                    remaining_keys = ?filtered.iter().map(|e| &e.key).collect::<Vec<_>>(),
                    "social_keys after values_only filter"
                );

                let items: Vec<(String, String)> = filtered.iter()
                    .map(|entry| {
                        let val = if return_block_height {
                            serde_json::json!(entry.block_height)
                        } else {
                            serde_json::json!(true)
                        };
                        (entry.key.clone(), val.to_string())
                    })
                    .collect();
                let subtree = build_tree(&items);
                let account_obj = result_root
                    .entry(account_id.clone())
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                if let serde_json::Value::Object(ref mut obj) = account_obj {
                    deep_merge_json(obj, &subtree);
                }
            }
            KeyPattern::WildcardAccount { key } => {
                let by_key_params = ByKeyParams {
                    key: key.clone(),
                    current_account_id: SOCIAL_CONTRACT.to_string(),
                    limit: 1000,
                    offset: 0,
                    fields: None,
                };
                let entries = app_state.scylladb.query_by_key(&by_key_params).await?;

                for entry in &entries {
                    if !return_deleted && entry.value == "null" {
                        continue;
                    }
                    let val = if return_block_height {
                        serde_json::json!(entry.block_height)
                    } else {
                        serde_json::json!(true)
                    };
                    let items = vec![(entry.key.clone(), val.to_string())];
                    let subtree = build_tree(&items);
                    let account_obj = result_root
                        .entry(entry.predecessor_id.clone())
                        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                    if let serde_json::Value::Object(ref mut obj) = account_obj {
                        merge_json(obj, &subtree);
                    }
                }
            }
        }
    }

    Ok(HttpResponse::Ok().json(serde_json::Value::Object(result_root)))
}

/// Query SocialDB index entries by action and key
#[utoipa::path(
    get,
    path = "/v1/social/index",
    params(SocialIndexParams),
    responses(
        (status = 200, description = "Index entries", body = IndexResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/index")]
pub async fn social_index_handler(
    query: web::Query<SocialIndexParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    if query.action.is_empty() {
        return Err(ApiError::InvalidParameter("action cannot be empty".to_string()));
    }
    if query.key.is_empty() {
        return Err(ApiError::InvalidParameter("key cannot be empty".to_string()));
    }
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter("limit must be between 1 and 1000".to_string()));
    }

    tracing::info!(target: PROJECT_ID, action = %query.action, key = %query.key, "GET /v1/social/index");

    let entries = query_index(&app_state, &query.action, &query.key, &query.order, query.limit, query.from).await?;

    Ok(HttpResponse::Ok().json(IndexResponse { entries }))
}

/// Get a user's profile from SocialDB
#[utoipa::path(
    get,
    path = "/v1/social/profile",
    params(SocialProfileParams),
    responses(
        (status = 200, description = "Profile JSON"),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/profile")]
pub async fn social_profile_handler(
    query: web::Query<SocialProfileParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    if query.account_id.is_empty() {
        return Err(ApiError::InvalidParameter("account_id cannot be empty".to_string()));
    }

    tracing::info!(target: PROJECT_ID, account_id = %query.account_id, "GET /v1/social/profile");

    let params = QueryParams {
        predecessor_id: query.account_id.clone(),
        current_account_id: SOCIAL_CONTRACT.to_string(),
        key_prefix: Some("profile/".to_string()),
        exclude_null: Some(true),
        limit: 1000,
        offset: 0,
        fields: None,
        format: None,
    };

    let entries = app_state.scylladb.query_kv_with_pagination(&params).await?;

    let items: Vec<(String, String)> = entries.into_iter()
        .map(|e| {
            // Strip "profile/" prefix so tree is rooted at profile fields
            let key = e.key.strip_prefix("profile/").unwrap_or(&e.key).to_string();
            (key, e.value)
        })
        .collect();

    let tree = build_tree(&items);
    Ok(HttpResponse::Ok().json(tree))
}

/// Get accounts following a user
#[utoipa::path(
    get,
    path = "/v1/social/followers",
    params(SocialFollowParams),
    responses(
        (status = 200, description = "Follower accounts", body = SocialFollowResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/followers")]
pub async fn social_followers_handler(
    query: web::Query<SocialFollowParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    if query.account_id.is_empty() {
        return Err(ApiError::InvalidParameter("account_id cannot be empty".to_string()));
    }
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter("limit must be between 1 and 1000".to_string()));
    }

    tracing::info!(target: PROJECT_ID, account_id = %query.account_id, "GET /v1/social/followers");

    // Followers are accounts that wrote key "graph/follow/{accountId}" to social.near
    let follow_key = format!("graph/follow/{}", query.account_id);

    let params = AccountsParams {
        current_account_id: SOCIAL_CONTRACT.to_string(),
        key: follow_key,
        exclude_null: Some(true),
        limit: query.limit,
        offset: query.offset,
    };

    let accounts = app_state.scylladb.query_accounts(&params).await?;
    let count = accounts.len();

    Ok(HttpResponse::Ok().json(SocialFollowResponse { accounts, count }))
}

/// Get accounts a user follows
#[utoipa::path(
    get,
    path = "/v1/social/following",
    params(SocialFollowParams),
    responses(
        (status = 200, description = "Following accounts", body = SocialFollowResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/following")]
pub async fn social_following_handler(
    query: web::Query<SocialFollowParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    if query.account_id.is_empty() {
        return Err(ApiError::InvalidParameter("account_id cannot be empty".to_string()));
    }
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter("limit must be between 1 and 1000".to_string()));
    }

    tracing::info!(target: PROJECT_ID, account_id = %query.account_id, "GET /v1/social/following");

    // Following are keys under "graph/follow/" written by this account to social.near
    let keys_params = KeysParams {
        predecessor_id: query.account_id.clone(),
        current_account_id: SOCIAL_CONTRACT.to_string(),
        key_prefix: Some("graph/follow/".to_string()),
        limit: query.limit,
        offset: query.offset,
    };

    let keys = app_state.scylladb.query_keys(&keys_params).await?;

    // Strip "graph/follow/" prefix to get account IDs
    let accounts: Vec<String> = keys.into_iter()
        .filter_map(|k| k.strip_prefix("graph/follow/").map(|s| s.to_string()))
        .collect();
    let count = accounts.len();

    Ok(HttpResponse::Ok().json(SocialFollowResponse { accounts, count }))
}

/// Get likes for an item
#[utoipa::path(
    get,
    path = "/v1/social/likes",
    params(SocialItemParams),
    responses(
        (status = 200, description = "Like entries", body = IndexResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/likes")]
pub async fn social_likes_handler(
    query: web::Query<SocialItemParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_item_params(&query)?;
    tracing::info!(target: PROJECT_ID, path = %query.path, block_height = query.block_height, "GET /v1/social/likes");

    let key = format!("{}\n{}", query.path, query.block_height);
    let entries = query_index(&app_state, "like", &key, &query.order, query.limit, None).await?;

    Ok(HttpResponse::Ok().json(IndexResponse { entries }))
}

/// Get comments for an item
#[utoipa::path(
    get,
    path = "/v1/social/comments",
    params(SocialItemParams),
    responses(
        (status = 200, description = "Comment entries", body = IndexResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/comments")]
pub async fn social_comments_handler(
    query: web::Query<SocialItemParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_item_params(&query)?;
    tracing::info!(target: PROJECT_ID, path = %query.path, block_height = query.block_height, "GET /v1/social/comments");

    let key = format!("{}\n{}", query.path, query.block_height);
    let entries = query_index(&app_state, "comment", &key, &query.order, query.limit, None).await?;

    Ok(HttpResponse::Ok().json(IndexResponse { entries }))
}

/// Get reposts for an item
#[utoipa::path(
    get,
    path = "/v1/social/reposts",
    params(SocialItemParams),
    responses(
        (status = 200, description = "Repost entries", body = IndexResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/reposts")]
pub async fn social_reposts_handler(
    query: web::Query<SocialItemParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    validate_item_params(&query)?;
    tracing::info!(target: PROJECT_ID, path = %query.path, block_height = query.block_height, "GET /v1/social/reposts");

    let key = format!("{}\n{}", query.path, query.block_height);
    let entries = query_index(&app_state, "repost", &key, &query.order, query.limit, None).await?;

    Ok(HttpResponse::Ok().json(IndexResponse { entries }))
}

/// Get posts from a specific account
#[utoipa::path(
    get,
    path = "/v1/social/feed/account",
    params(SocialAccountFeedParams),
    responses(
        (status = 200, description = "Account feed", body = SocialFeedResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/feed/account")]
pub async fn social_account_feed_handler(
    query: web::Query<SocialAccountFeedParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    if query.account_id.is_empty() {
        return Err(ApiError::InvalidParameter("account_id cannot be empty".to_string()));
    }
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter("limit must be between 1 and 1000".to_string()));
    }

    tracing::info!(target: PROJECT_ID, account_id = %query.account_id, "GET /v1/social/feed/account");

    // Query history for post/main key to get all posts with their block heights
    let history_params = HistoryParams {
        predecessor_id: query.account_id.clone(),
        current_account_id: SOCIAL_CONTRACT.to_string(),
        key: "post/main".to_string(),
        limit: query.limit,
        order: query.order.clone(),
        from_block: query.from.map(|f| f as i64),
        to_block: None,
        fields: None,
    };

    let entries = app_state.scylladb.get_kv_history(&history_params).await?;

    let posts: Vec<IndexEntry> = entries.into_iter()
        .map(|e| IndexEntry {
            account_id: e.predecessor_id,
            block_height: e.block_height,
            value: serde_json::from_str(&e.value).ok(),
        })
        .collect();

    Ok(HttpResponse::Ok().json(SocialFeedResponse { posts }))
}

/// Get posts tagged with a hashtag
#[utoipa::path(
    get,
    path = "/v1/social/feed/hashtag",
    params(SocialHashtagFeedParams),
    responses(
        (status = 200, description = "Hashtag feed", body = IndexResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/feed/hashtag")]
pub async fn social_hashtag_feed_handler(
    query: web::Query<SocialHashtagFeedParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    if query.hashtag.is_empty() {
        return Err(ApiError::InvalidParameter("hashtag cannot be empty".to_string()));
    }
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter("limit must be between 1 and 1000".to_string()));
    }

    tracing::info!(target: PROJECT_ID, hashtag = %query.hashtag, "GET /v1/social/feed/hashtag");

    let entries = query_index(&app_state, "hashtag", &query.hashtag, &query.order, query.limit, None).await?;

    Ok(HttpResponse::Ok().json(IndexResponse { entries }))
}

/// Get all recent posts (activity feed)
#[utoipa::path(
    get,
    path = "/v1/social/feed/activity",
    params(SocialActivityFeedParams),
    responses(
        (status = 200, description = "Activity feed", body = IndexResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/feed/activity")]
pub async fn social_activity_feed_handler(
    query: web::Query<SocialActivityFeedParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter("limit must be between 1 and 1000".to_string()));
    }

    tracing::info!(target: PROJECT_ID, "GET /v1/social/feed/activity");

    let entries = query_index(&app_state, "post", "main", &query.order, query.limit, None).await?;

    Ok(HttpResponse::Ok().json(IndexResponse { entries }))
}

/// Get notifications for an account
#[utoipa::path(
    get,
    path = "/v1/social/notifications",
    params(SocialNotificationsParams),
    responses(
        (status = 200, description = "Notification entries", body = IndexResponse),
        (status = 400, description = "Invalid parameters", body = ApiError)
    ),
    tag = "social"
)]
#[get("/v1/social/notifications")]
pub async fn social_notifications_handler(
    query: web::Query<SocialNotificationsParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, ApiError> {
    if query.account_id.is_empty() {
        return Err(ApiError::InvalidParameter("account_id cannot be empty".to_string()));
    }
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter("limit must be between 1 and 1000".to_string()));
    }

    tracing::info!(target: PROJECT_ID, account_id = %query.account_id, "GET /v1/social/notifications");

    let entries = query_index(&app_state, "notify", &query.account_id, &query.order, query.limit, query.from).await?;

    Ok(HttpResponse::Ok().json(IndexResponse { entries }))
}

// ===== Utility functions =====

/// Returns true if this entry's key is a prefix of another entry's key,
/// meaning it's an intermediate path node rather than a leaf value.
fn is_intermediate_key(entry: &KvEntry, all_entries: &[KvEntry]) -> bool {
    let prefix = format!("{}/", entry.key);
    all_entries.iter().any(|other| other.key.starts_with(&prefix))
}

fn validate_item_params(query: &SocialItemParams) -> Result<(), ApiError> {
    if query.path.is_empty() {
        return Err(ApiError::InvalidParameter("path cannot be empty".to_string()));
    }
    if query.limit == 0 || query.limit > 1000 {
        return Err(ApiError::InvalidParameter("limit must be between 1 and 1000".to_string()));
    }
    Ok(())
}

fn merge_json(target: &mut serde_json::Map<String, serde_json::Value>, source: &serde_json::Value) {
    if let serde_json::Value::Object(source_obj) = source {
        for (key, value) in source_obj {
            match (target.get_mut(key), value) {
                (Some(serde_json::Value::Object(ref mut existing)), serde_json::Value::Object(incoming)) => {
                    let incoming_value = serde_json::Value::Object(incoming.clone());
                    merge_json(existing, &incoming_value);
                }
                _ => {
                    target.insert(key.clone(), value.clone());
                }
            }
        }
    }
}

fn deep_merge_json(target: &mut serde_json::Map<String, serde_json::Value>, source: &serde_json::Value) {
    merge_json(target, source);
}

fn insert_block_height(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    block_height: u64,
) {
    // SocialDB convention: add ":block" key alongside the value
    let parts: Vec<&str> = key.split('/').collect();
    if parts.is_empty() {
        return;
    }

    // Navigate to parent, then insert sibling ":block" key
    let mut current = obj as *mut serde_json::Map<String, serde_json::Value>;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // At the leaf — insert ":block" at this level
            let block_key = format!("{}:block", part);
            unsafe {
                (*current).insert(block_key, serde_json::json!(block_height));
            }
        } else {
            unsafe {
                if let Some(serde_json::Value::Object(ref mut nested)) = (*current).get_mut(*part) {
                    current = nested as *mut serde_json::Map<String, serde_json::Value>;
                } else {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_exact_key() {
        match parse_social_key("alice.near/post/main").unwrap() {
            KeyPattern::Exact { account_id, key } => {
                assert_eq!(account_id, "alice.near");
                assert_eq!(key, "post/main");
            }
            _ => panic!("Expected Exact"),
        }
    }

    #[test]
    fn test_parse_recursive_wildcard() {
        match parse_social_key("alice.near/profile/**").unwrap() {
            KeyPattern::RecursiveWildcard { account_id, key_prefix } => {
                assert_eq!(account_id, "alice.near");
                assert_eq!(key_prefix, "profile/");
            }
            _ => panic!("Expected RecursiveWildcard"),
        }
    }

    #[test]
    fn test_parse_single_wildcard() {
        match parse_social_key("alice.near/profile/*").unwrap() {
            KeyPattern::SingleWildcard { account_id, key_prefix } => {
                assert_eq!(account_id, "alice.near");
                assert_eq!(key_prefix, "profile/");
            }
            _ => panic!("Expected SingleWildcard"),
        }
    }

    #[test]
    fn test_parse_wildcard_account() {
        match parse_social_key("*/widget/name").unwrap() {
            KeyPattern::WildcardAccount { key } => {
                assert_eq!(key, "widget/name");
            }
            _ => panic!("Expected WildcardAccount"),
        }
    }

    #[test]
    fn test_parse_account_only() {
        match parse_social_key("alice.near").unwrap() {
            KeyPattern::RecursiveWildcard { account_id, key_prefix } => {
                assert_eq!(account_id, "alice.near");
                assert_eq!(key_prefix, "");
            }
            _ => panic!("Expected RecursiveWildcard for bare account"),
        }
    }

    #[test]
    fn test_parse_wildcard_account_with_wildcard_key_rejected() {
        assert!(parse_social_key("*/profile/**").is_err());
    }

    #[test]
    fn test_merge_json() {
        let mut target = serde_json::Map::new();
        target.insert("a".to_string(), serde_json::json!({"x": 1}));

        let source = serde_json::json!({"a": {"y": 2}, "b": 3});
        merge_json(&mut target, &source);

        assert_eq!(target["a"]["x"], 1);
        assert_eq!(target["a"]["y"], 2);
        assert_eq!(target["b"], 3);
    }

    #[test]
    fn test_merge_json_deep() {
        // Simulates merging graph/follow/efiz.near then graph/follow/sleet.near
        let mut target = serde_json::Map::new();

        let tree1 = build_tree(&[("graph/follow/efiz.near".to_string(), "true".to_string())]);
        merge_json(&mut target, &tree1);

        let tree2 = build_tree(&[("graph/follow/sleet.near".to_string(), "true".to_string())]);
        merge_json(&mut target, &tree2);

        assert_eq!(target["graph"]["follow"]["efiz.near"], true);
        assert_eq!(target["graph"]["follow"]["sleet.near"], true);
    }
}

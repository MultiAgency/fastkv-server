use actix_web::{get, post, web, HttpResponse};
use futures::stream::StreamExt;

use crate::handlers::{require_db, validate_account_id, validate_cursor_or_offset};
use crate::models::*;
use crate::tree::build_tree;
use crate::AppState;

use std::sync::LazyLock;

static SOCIAL_CONTRACT: LazyLock<String> = LazyLock::new(|| {
    std::env::var("SOCIAL_CONTRACT").unwrap_or_else(|_| "social.near".to_string())
});

fn resolve_contract(contract_id: &Option<String>) -> Result<&str, ApiError> {
    match contract_id {
        Some(id) => {
            validate_account_id(id, "contract_id")?;
            Ok(id.as_str())
        }
        None => Ok(&SOCIAL_CONTRACT),
    }
}

// ===== Key pattern parsing =====

enum KeyPattern {
    /// Exact key lookup for a specific account: "alice.near/post/main"
    Exact { account_id: String, key: String },
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
    WildcardAccount { key: String },
}

fn parse_social_key(pattern: &str) -> Result<KeyPattern, ApiError> {
    if pattern.is_empty() {
        return Err(ApiError::InvalidParameter(
            "key pattern cannot be empty".to_string(),
        ));
    }

    let (account_part, key_part) = match pattern.find('/') {
        Some(idx) => (&pattern[..idx], &pattern[idx + 1..]),
        None => {
            // Just an account ID with no key path — treat as recursive wildcard on all keys
            if pattern.len() > MAX_ACCOUNT_ID_LENGTH {
                return Err(ApiError::InvalidParameter(
                    "account ID too long".to_string(),
                ));
            }
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

    if key_part.len() > MAX_KEY_LENGTH {
        return Err(ApiError::InvalidParameter(format!(
            "key pattern cannot exceed {} characters",
            MAX_KEY_LENGTH
        )));
    }

    if key_part == "**" {
        Ok(KeyPattern::RecursiveWildcard {
            account_id: account_part.to_string(),
            key_prefix: String::new(),
        })
    } else if key_part == "*" {
        Ok(KeyPattern::SingleWildcard {
            account_id: account_part.to_string(),
            key_prefix: String::new(),
        })
    } else if key_part.ends_with("/**") {
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

struct IndexQuery<'a> {
    app_state: &'a AppState,
    contract_id: &'a str,
    action: &'a str,
    key: &'a str,
    order: &'a str,
    limit: usize,
    from: Option<u64>,
    account_id: Option<&'a str>,
}

/// Returns (entries, dropped_rows).
async fn query_index(q: IndexQuery<'_>) -> Result<(Vec<IndexEntry>, usize), ApiError> {
    let IndexQuery {
        app_state,
        contract_id,
        action,
        key,
        order,
        limit,
        from,
        account_id,
    } = q;
    let index_key = format!("index/{}/{}", action, key);

    let scylladb = require_db(app_state).await?;
    let mut rows_stream = scylladb
        .scylla_session
        .execute_iter(scylladb.reverse_kv.clone(), (contract_id, &index_key))
        .await
        .map_err(anyhow::Error::from)?
        .rows_stream::<KvRow>()
        .map_err(anyhow::Error::from)?;

    let mut entries: Vec<IndexEntry> = Vec::new();
    let mut error_count = 0usize;

    let stream_result = actix_web::rt::time::timeout(
        std::time::Duration::from_secs(30),
        async {
            while let Some(row_result) = rows_stream.next().await {
                let row = match row_result {
                    Ok(row) => row,
                    Err(e) => {
                        error_count += 1;
                        tracing::warn!(target: PROJECT_ID, error = %e, "Failed to deserialize row in query_index");
                        if error_count >= MAX_STREAM_ERRORS {
                            tracing::error!(target: PROJECT_ID, error_count, "Aborting query_index: too many deserialization errors");
                            return Err(ApiError::DatabaseError(
                                "An internal database error occurred".to_string(),
                            ));
                        }
                        continue;
                    }
                };

                let block_height = bigint_to_u64(row.block_height);

                // Apply cursor filter
                if let Some(from_block) = from {
                    if order == "desc" && block_height >= from_block {
                        continue;
                    }
                    if order == "asc" && block_height <= from_block {
                        continue;
                    }
                }

                // Apply account_id filter
                if let Some(aid) = account_id {
                    if row.predecessor_id != aid {
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
            Ok(())
        },
    )
    .await;

    match stream_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            tracing::warn!(target: PROJECT_ID, "query_index stream timed out after 30s, returning partial results");
        }
    }

    // Sort by block_height
    if order == "asc" {
        entries.sort_by_key(|e| e.block_height);
    } else {
        entries.sort_by(|a, b| b.block_height.cmp(&a.block_height));
    }

    entries.truncate(limit);
    Ok((entries, error_count))
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
        return Err(ApiError::InvalidParameter(
            "keys cannot be empty".to_string(),
        ));
    }
    if body.keys.len() > MAX_SOCIAL_KEYS {
        return Err(ApiError::InvalidParameter(format!(
            "keys cannot exceed {} patterns",
            MAX_SOCIAL_KEYS
        )));
    }

    let contract = resolve_contract(&body.contract_id)?;
    let with_block_height = body
        .options
        .as_ref()
        .and_then(|o| o.with_block_height)
        .unwrap_or(false);
    let return_deleted = body
        .options
        .as_ref()
        .and_then(|o| o.return_deleted)
        .unwrap_or(false);

    tracing::info!(target: PROJECT_ID, key_count = body.keys.len(), contract_id = %contract, "POST /v1/social/get");

    let scylladb = require_db(&app_state).await?;
    let mut result_root = serde_json::Map::new();
    let mut truncated = false;

    for pattern in &body.keys {
        let parsed = parse_social_key(pattern)?;

        match parsed {
            KeyPattern::Exact { account_id, key } => {
                let entry = scylladb.get_kv(&account_id, contract, &key).await?;

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
            KeyPattern::RecursiveWildcard {
                account_id,
                key_prefix,
            } => {
                let query = QueryParams {
                    predecessor_id: account_id.clone(),
                    current_account_id: contract.to_string(),
                    key_prefix: if key_prefix.is_empty() {
                        None
                    } else {
                        Some(key_prefix)
                    },
                    exclude_null: if return_deleted {
                        Some(false)
                    } else {
                        Some(true)
                    },
                    limit: MAX_SOCIAL_RESULTS,
                    offset: 0,
                    fields: None,
                    format: None,
                    value_format: None,
                    after_key: None,
                };

                let (entries, _has_more, dropped) =
                    scylladb.query_kv_with_pagination(&query).await?;
                if dropped > 0 {
                    tracing::warn!(target: PROJECT_ID, dropped, "Dropped rows in social get (recursive wildcard)");
                }
                if entries.len() >= MAX_SOCIAL_RESULTS {
                    truncated = true;
                }

                let items: Vec<(String, String)> = entries
                    .iter()
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
            KeyPattern::SingleWildcard {
                account_id,
                key_prefix,
            } => {
                let query = QueryParams {
                    predecessor_id: account_id.clone(),
                    current_account_id: contract.to_string(),
                    key_prefix: if key_prefix.is_empty() {
                        None
                    } else {
                        Some(key_prefix.clone())
                    },
                    exclude_null: if return_deleted {
                        Some(false)
                    } else {
                        Some(true)
                    },
                    limit: MAX_SOCIAL_RESULTS,
                    offset: 0,
                    fields: None,
                    format: None,
                    value_format: None,
                    after_key: None,
                };

                let (entries, _has_more, dropped) =
                    scylladb.query_kv_with_pagination(&query).await?;
                if dropped > 0 {
                    tracing::warn!(target: PROJECT_ID, dropped, "Dropped rows in social get (single wildcard)");
                }
                if entries.len() >= MAX_SOCIAL_RESULTS {
                    truncated = true;
                }

                // Filter to single level only: key should be prefix + "something" with no more slashes
                let prefix_depth = key_prefix.matches('/').count();
                let items: Vec<(String, String)> = entries
                    .iter()
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
                let by_key_params = WritersParams {
                    key: key.clone(),
                    current_account_id: contract.to_string(),
                    predecessor_id: None,
                    exclude_null: None,
                    limit: MAX_SOCIAL_RESULTS,
                    offset: 0,
                    fields: None,
                    value_format: None,
                    after_account: None,
                };
                let (entries, _has_more, _truncated, dropped) =
                    scylladb.query_writers(&by_key_params).await?;
                if dropped > 0 {
                    tracing::warn!(target: PROJECT_ID, dropped, "Dropped rows in social get (wildcard account)");
                }
                if entries.len() >= MAX_SOCIAL_RESULTS {
                    truncated = true;
                }

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

    let mut response = HttpResponse::Ok();
    if truncated {
        response.insert_header(("X-Results-Truncated", "true"));
    }
    Ok(response.json(serde_json::Value::Object(result_root)))
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
        return Err(ApiError::InvalidParameter(
            "keys cannot be empty".to_string(),
        ));
    }
    if body.keys.len() > MAX_SOCIAL_KEYS {
        return Err(ApiError::InvalidParameter(format!(
            "keys cannot exceed {} patterns",
            MAX_SOCIAL_KEYS
        )));
    }

    let contract = resolve_contract(&body.contract_id)?;
    let return_block_height = body
        .options
        .as_ref()
        .and_then(|o| o.return_type.as_deref())
        .map(|t| t == "BlockHeight")
        .unwrap_or(false);
    let return_deleted = body
        .options
        .as_ref()
        .and_then(|o| o.return_deleted)
        .unwrap_or(false);
    let values_only = body
        .options
        .as_ref()
        .and_then(|o| o.values_only)
        .unwrap_or(false);

    tracing::info!(target: PROJECT_ID, key_count = body.keys.len(), contract_id = %contract, "POST /v1/social/keys");

    let scylladb = require_db(&app_state).await?;
    let mut result_root = serde_json::Map::new();
    let mut truncated = false;

    for pattern in &body.keys {
        let parsed = parse_social_key(pattern)?;

        match parsed {
            KeyPattern::Exact { account_id, key } => {
                let entry = scylladb.get_kv(&account_id, contract, &key).await?;

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
            KeyPattern::SingleWildcard {
                account_id,
                key_prefix,
            }
            | KeyPattern::RecursiveWildcard {
                account_id,
                key_prefix,
            } => {
                // keys() only supports * (not **), but we handle both the same way for robustness
                // keys() returns key structure — fetch all keys including deleted
                // unless return_deleted is explicitly false AND values_only is set
                let query = QueryParams {
                    predecessor_id: account_id.clone(),
                    current_account_id: contract.to_string(),
                    key_prefix: if key_prefix.is_empty() {
                        None
                    } else {
                        Some(key_prefix.clone())
                    },
                    exclude_null: Some(false),
                    limit: MAX_SOCIAL_RESULTS,
                    offset: 0,
                    fields: None,
                    format: None,
                    value_format: None,
                    after_key: None,
                };

                let (entries, _has_more, dropped) =
                    scylladb.query_kv_with_pagination(&query).await?;
                if dropped > 0 {
                    tracing::warn!(target: PROJECT_ID, dropped, "Dropped rows in social keys");
                }
                if entries.len() >= MAX_SOCIAL_RESULTS {
                    truncated = true;
                }

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
                let is_single = pattern.ends_with("/*") && !pattern.ends_with("/**");

                let after_depth: Vec<&KvEntry> = entries
                    .iter()
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

                let after_deleted: Vec<&&KvEntry> = after_depth
                    .iter()
                    .filter(|e| return_deleted || e.value != "null")
                    .collect();
                tracing::debug!(
                    target: PROJECT_ID,
                    before = after_depth.len(),
                    after = after_deleted.len(),
                    return_deleted,
                    "social_keys after return_deleted filter"
                );

                // Build set of parent prefixes for O(n) filtering instead of O(n²)
                // A key like "a/b/c" contributes prefixes: "a/", "a/b/"
                let parent_prefixes: std::collections::HashSet<String> = if values_only {
                    entries
                        .iter()
                        .flat_map(|e| {
                            let parts: Vec<_> = e.key.split('/').collect();
                            (0..parts.len().saturating_sub(1))
                                .map(move |i| format!("{}/", parts[..=i].join("/")))
                        })
                        .collect()
                } else {
                    std::collections::HashSet::new()
                };

                let before_values_only = after_deleted.len();
                let filtered: Vec<&KvEntry> = after_deleted
                    .into_iter()
                    .filter(|e| {
                        if !values_only {
                            return true;
                        }
                        // Keep entry if it's NOT a parent of any other entry
                        !parent_prefixes.contains(&format!("{}/", e.key))
                    })
                    .copied()
                    .collect();
                tracing::debug!(
                    target: PROJECT_ID,
                    before = before_values_only,
                    after = filtered.len(),
                    values_only,
                    remaining_keys = ?filtered.iter().map(|e| &e.key).collect::<Vec<_>>(),
                    "social_keys after values_only filter"
                );

                let items: Vec<(String, String)> = filtered
                    .iter()
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
                    merge_json(obj, &subtree);
                }
            }
            KeyPattern::WildcardAccount { key } => {
                let by_key_params = WritersParams {
                    key: key.clone(),
                    current_account_id: contract.to_string(),
                    predecessor_id: None,
                    exclude_null: None,
                    limit: MAX_SOCIAL_RESULTS,
                    offset: 0,
                    fields: None,
                    value_format: None,
                    after_account: None,
                };
                let (entries, _has_more, _truncated, dropped) =
                    scylladb.query_writers(&by_key_params).await?;
                if dropped > 0 {
                    tracing::warn!(target: PROJECT_ID, dropped, "Dropped rows in social keys (wildcard account)");
                }
                if entries.len() >= MAX_SOCIAL_RESULTS {
                    truncated = true;
                }

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

    let mut response = HttpResponse::Ok();
    if truncated {
        response.insert_header(("X-Results-Truncated", "true"));
    }
    Ok(response.json(serde_json::Value::Object(result_root)))
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
        return Err(ApiError::InvalidParameter(
            "action cannot be empty".to_string(),
        ));
    }
    if query.key.is_empty() {
        return Err(ApiError::InvalidParameter(
            "key cannot be empty".to_string(),
        ));
    }
    validate_limit(query.limit)?;
    let contract = resolve_contract(&query.contract_id)?;

    tracing::info!(target: PROJECT_ID, action = %query.action, key = %query.key, contract_id = %contract, "GET /v1/social/index");

    let (entries, dropped) = query_index(IndexQuery {
        app_state: &app_state,
        contract_id: contract,
        action: &query.action,
        key: &query.key,
        order: &query.order,
        limit: query.limit,
        from: query.from,
        account_id: query.account_id.as_deref(),
    })
    .await?;

    let mut response = HttpResponse::Ok();
    if dropped > 0 {
        response.insert_header(("X-Dropped-Rows", dropped.to_string()));
    }
    Ok(response.json(IndexResponse { entries }))
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
    validate_account_id(&query.account_id, "account_id")?;
    let contract = resolve_contract(&query.contract_id)?;

    tracing::info!(target: PROJECT_ID, account_id = %query.account_id, contract_id = %contract, "GET /v1/social/profile");

    let scylladb = require_db(&app_state).await?;
    let params = QueryParams {
        predecessor_id: query.account_id.clone(),
        current_account_id: contract.to_string(),
        key_prefix: Some("profile/".to_string()),
        exclude_null: Some(true),
        limit: MAX_SOCIAL_RESULTS,
        offset: 0,
        fields: None,
        format: None,
        value_format: None,
        after_key: None,
    };

    let (entries, _has_more, dropped) = scylladb.query_kv_with_pagination(&params).await?;
    if dropped > 0 {
        tracing::warn!(target: PROJECT_ID, dropped, "Dropped rows in social profile");
    }

    let items: Vec<(String, String)> = entries
        .into_iter()
        .map(|e| {
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
    validate_account_id(&query.account_id, "account_id")?;
    validate_limit(query.limit)?;
    let contract = resolve_contract(&query.contract_id)?;

    validate_cursor_or_offset(
        query.after_account.as_deref(),
        "after_account",
        query.offset,
        validate_account_id,
    )?;

    tracing::info!(target: PROJECT_ID, account_id = %query.account_id, contract_id = %contract, "GET /v1/social/followers");

    let scylladb = require_db(&app_state).await?;
    // Followers are accounts that wrote key "graph/follow/{accountId}" to the contract
    let follow_key = format!("graph/follow/{}", query.account_id);

    let params = AccountsParams {
        current_account_id: contract.to_string(),
        key: follow_key,
        exclude_null: Some(true),
        limit: query.limit,
        offset: query.offset,
        after_account: query.after_account.clone(),
    };

    let (accounts, has_more, dropped) = scylladb.query_accounts(&params).await?;
    let count = accounts.len();
    let next_cursor = accounts.last().cloned();

    Ok(HttpResponse::Ok().json(SocialFollowResponse {
        accounts,
        count,
        meta: PaginationMeta {
            has_more,
            truncated: false,
            next_cursor,
            dropped_rows: dropped_to_option(dropped),
        },
    }))
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
    validate_account_id(&query.account_id, "account_id")?;
    validate_limit(query.limit)?;
    let contract = resolve_contract(&query.contract_id)?;

    validate_cursor_or_offset(
        query.after_account.as_deref(),
        "after_account",
        query.offset,
        validate_account_id,
    )?;

    tracing::info!(target: PROJECT_ID, account_id = %query.account_id, contract_id = %contract, "GET /v1/social/following");

    let scylladb = require_db(&app_state).await?;
    // Following are keys under "graph/follow/" written by this account to the contract
    let params = QueryParams {
        predecessor_id: query.account_id.clone(),
        current_account_id: contract.to_string(),
        key_prefix: Some("graph/follow/".to_string()),
        exclude_null: Some(true),
        limit: query.limit,
        offset: query.offset,
        fields: None,
        format: None,
        value_format: None,
        after_key: query
            .after_account
            .as_ref()
            .map(|a| format!("graph/follow/{}", a)),
    };

    let (entries, has_more, dropped) = scylladb.query_kv_with_pagination(&params).await?;

    let accounts: Vec<String> = entries
        .into_iter()
        .filter_map(|e| e.key.strip_prefix("graph/follow/").map(|s| s.to_string()))
        .collect();
    let count = accounts.len();
    let next_cursor = accounts.last().cloned();

    Ok(HttpResponse::Ok().json(SocialFollowResponse {
        accounts,
        count,
        meta: PaginationMeta {
            has_more,
            truncated: false,
            next_cursor,
            dropped_rows: dropped_to_option(dropped),
        },
    }))
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
    validate_account_id(&query.account_id, "account_id")?;
    validate_limit(query.limit)?;
    let contract = resolve_contract(&query.contract_id)?;

    tracing::info!(target: PROJECT_ID, account_id = %query.account_id, "GET /v1/social/feed/account");

    let scylladb = require_db(&app_state).await?;
    let from_block = query.from.map(|f| i64::try_from(f).unwrap_or(i64::MAX));
    let include_replies = query.include_replies.unwrap_or(false);

    let to_index_entry = |e: KvEntry| IndexEntry {
        account_id: e.predecessor_id,
        block_height: e.block_height,
        value: serde_json::from_str(&e.value).ok(),
    };

    // Query history for post/main key to get all posts with their block heights
    let history_params = HistoryParams {
        predecessor_id: query.account_id.clone(),
        current_account_id: contract.to_string(),
        key: "post/main".to_string(),
        limit: query.limit,
        order: query.order.clone(),
        from_block,
        to_block: None,
        fields: None,
        value_format: None,
    };

    let comment_params = HistoryParams {
        predecessor_id: query.account_id.clone(),
        current_account_id: contract.to_string(),
        key: "post/comment".to_string(),
        limit: query.limit,
        order: query.order.clone(),
        from_block,
        to_block: None,
        fields: None,
        value_format: None,
    };

    let posts: Vec<IndexEntry> = if include_replies {
        let ((entries, _hm1, _tr1, dropped1), (comment_entries, _hm2, _tr2, dropped2)) =
            futures::future::try_join(
                scylladb.get_kv_history(&history_params),
                scylladb.get_kv_history(&comment_params),
            )
            .await?;
        let total_dropped = dropped1 + dropped2;
        if total_dropped > 0 {
            tracing::warn!(target: PROJECT_ID, dropped = total_dropped, "Dropped rows in social feed");
        }

        let mut combined: Vec<IndexEntry> = entries
            .into_iter()
            .chain(comment_entries.into_iter())
            .map(to_index_entry)
            .collect();

        if query.order == "asc" {
            combined.sort_by_key(|e| e.block_height);
        } else {
            combined.sort_by(|a, b| b.block_height.cmp(&a.block_height));
        }

        combined.truncate(query.limit);
        combined
    } else {
        let (entries, _has_more, _truncated, dropped) =
            scylladb.get_kv_history(&history_params).await?;
        if dropped > 0 {
            tracing::warn!(target: PROJECT_ID, dropped, "Dropped rows in social feed");
        }
        entries.into_iter().map(to_index_entry).collect()
    };

    Ok(HttpResponse::Ok().json(SocialFeedResponse { posts }))
}

// ===== Utility functions =====

fn merge_json(target: &mut serde_json::Map<String, serde_json::Value>, source: &serde_json::Value) {
    if let serde_json::Value::Object(source_obj) = source {
        merge_json_maps(target, source_obj);
    }
}

fn merge_json_maps(
    target: &mut serde_json::Map<String, serde_json::Value>,
    source: &serde_json::Map<String, serde_json::Value>,
) {
    for (key, value) in source {
        match (target.get_mut(key), value) {
            (
                Some(serde_json::Value::Object(ref mut existing)),
                serde_json::Value::Object(incoming),
            ) => {
                merge_json_maps(existing, incoming);
            }
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

fn insert_block_height(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    block_height: u64,
) {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.is_empty() {
        return;
    }

    let mut current = obj;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            let block_key = format!("{}:block", part);
            current.insert(block_key, serde_json::json!(block_height));
        } else {
            match current.get_mut(*part) {
                Some(serde_json::Value::Object(nested)) => {
                    current = nested;
                }
                _ => return,
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
            KeyPattern::RecursiveWildcard {
                account_id,
                key_prefix,
            } => {
                assert_eq!(account_id, "alice.near");
                assert_eq!(key_prefix, "profile/");
            }
            _ => panic!("Expected RecursiveWildcard"),
        }
    }

    #[test]
    fn test_parse_single_wildcard() {
        match parse_social_key("alice.near/profile/*").unwrap() {
            KeyPattern::SingleWildcard {
                account_id,
                key_prefix,
            } => {
                assert_eq!(account_id, "alice.near");
                assert_eq!(key_prefix, "profile/");
            }
            _ => panic!("Expected SingleWildcard"),
        }
    }

    #[test]
    fn test_parse_bare_recursive_wildcard() {
        match parse_social_key("alice.near/**").unwrap() {
            KeyPattern::RecursiveWildcard {
                account_id,
                key_prefix,
            } => {
                assert_eq!(account_id, "alice.near");
                assert_eq!(key_prefix, "");
            }
            _ => panic!("Expected RecursiveWildcard"),
        }
    }

    #[test]
    fn test_parse_bare_single_wildcard() {
        match parse_social_key("alice.near/*").unwrap() {
            KeyPattern::SingleWildcard {
                account_id,
                key_prefix,
            } => {
                assert_eq!(account_id, "alice.near");
                assert_eq!(key_prefix, "");
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
            KeyPattern::RecursiveWildcard {
                account_id,
                key_prefix,
            } => {
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

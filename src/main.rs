mod handlers;
mod models;
mod queries;
mod scylladb;
mod social_handlers;
mod tree;

use crate::handlers::{accounts_handler, batch_kv_handler, diff_kv_handler, edges_handler, edges_count_handler, get_kv_handler, health_check, history_kv_handler, query_kv_handler, status_handler, timeline_kv_handler, writers_handler};
use actix_files::Files;
use crate::social_handlers::{
    social_get_handler, social_keys_handler, social_index_handler,
    social_profile_handler, social_followers_handler, social_following_handler,
    social_account_feed_handler,
};
use crate::scylladb::ScyllaDb;
use actix_cors::Cors;
use actix_web::http::header;
use actix_web::{dev::Service, middleware, web, App, HttpServer};
use dotenvy::dotenv;
use fastnear_primitives::types::ChainId;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

use crate::models::PROJECT_ID;

#[derive(OpenApi)]
#[openapi(
    paths(
        handlers::health_check,
        handlers::status_handler,
        handlers::get_kv_handler,
        handlers::query_kv_handler,
        handlers::history_kv_handler,
        handlers::writers_handler,
        handlers::diff_kv_handler,
        handlers::timeline_kv_handler,
        handlers::batch_kv_handler,
        handlers::accounts_handler,
        handlers::edges_handler,
        handlers::edges_count_handler,
        social_handlers::social_get_handler,
        social_handlers::social_keys_handler,
        social_handlers::social_index_handler,
        social_handlers::social_profile_handler,
        social_handlers::social_followers_handler,
        social_handlers::social_following_handler,
        social_handlers::social_account_feed_handler,
    ),
    components(schemas(
        models::KvEntry,
        models::HealthResponse,
        models::StatusResponse,
        models::GetParams,
        models::QueryParams,
        models::HistoryParams,
        models::WritersParams,
        models::ApiError,
        models::BatchQuery,
        models::BatchResultItem,
        models::TreeResponse,
        models::DiffParams,
        models::DiffResponse,
        models::TimelineParams,
        models::AccountsQueryParams,
        models::EdgesParams,
        models::EdgesCountParams,
        models::EdgeSourceEntry,
        models::EdgesCountResponse,
        models::SocialGetBody,
        models::SocialGetOptions,
        models::SocialKeysBody,
        models::SocialKeysOptions,
        models::SocialIndexParams,
        models::SocialProfileParams,
        models::SocialFollowParams,
        models::SocialAccountFeedParams,
        models::IndexEntry,
        models::IndexResponse,
        models::SocialFollowResponse,
        models::SocialFeedResponse,
        models::PaginationMeta,
    )),
    info(
        title = "FastKV API",
        version = "1.0.0",
        description = "Query FastData KV entries from ScyllaDB. This API provides access to NEAR Protocol data storage."
    ),
    tags(
        (name = "health", description = "Health check endpoints"),
        (name = "kv", description = "Key-Value storage operations"),
        (name = "social", description = "SocialDB-compatible convenience API")
    )
)]
struct ApiDoc;

#[derive(Clone)]
pub struct AppState {
    pub scylladb: Arc<RwLock<Option<ScyllaDb>>>,
    pub chain_id: ChainId,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "scylladb=info,fastkv-server=info".into()),
        )
        .init();

    let chain_id: ChainId = env::var("CHAIN_ID")
        .expect("CHAIN_ID required")
        .try_into()
        .expect("Invalid chain id");

    let scylladb: Arc<RwLock<Option<ScyllaDb>>> = match ScyllaDb::new_scylla_session().await {
        Ok(session) => match ScyllaDb::test_connection(&session).await {
            Ok(_) => match ScyllaDb::new(chain_id, session).await {
                Ok(db) => {
                    tracing::info!(target: PROJECT_ID, "Connected to Scylla");
                    Arc::new(RwLock::new(Some(db)))
                }
                Err(e) => {
                    tracing::warn!(target: PROJECT_ID, error = %e, "Failed to initialize ScyllaDB, starting without database");
                    Arc::new(RwLock::new(None))
                }
            },
            Err(e) => {
                tracing::warn!(target: PROJECT_ID, error = %e, "Failed to connect to ScyllaDB, starting without database");
                Arc::new(RwLock::new(None))
            }
        },
        Err(e) => {
            tracing::warn!(target: PROJECT_ID, error = %e, "Failed to create ScyllaDB session, starting without database");
            Arc::new(RwLock::new(None))
        }
    };

    // Background reconnection task with exponential backoff
    let reconnect_base_secs: u64 = env::var("DB_RECONNECT_INTERVAL_SECS")
        .unwrap_or_else(|_| "5".to_string())
        .parse()
        .unwrap_or(5)
        .max(5);
    let reconnect_max_secs: u64 = 300;
    {
        let scylladb = Arc::clone(&scylladb);
        tokio::spawn(async move {
            let mut delay_secs = reconnect_base_secs;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
                if scylladb.read().await.is_some() {
                    delay_secs = reconnect_base_secs;
                    continue;
                }
                tracing::info!(target: PROJECT_ID, delay_secs, "Attempting to reconnect to ScyllaDB...");
                let session = match ScyllaDb::new_scylla_session().await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(target: PROJECT_ID, error = %e, delay_secs, "ScyllaDB reconnection failed");
                        delay_secs = (delay_secs * 2).min(reconnect_max_secs);
                        continue;
                    }
                };
                if let Err(e) = ScyllaDb::test_connection(&session).await {
                    tracing::warn!(target: PROJECT_ID, error = %e, delay_secs, "ScyllaDB connection test failed");
                    delay_secs = (delay_secs * 2).min(reconnect_max_secs);
                    continue;
                }
                match ScyllaDb::new(chain_id, session).await {
                    Ok(db) => {
                        *scylladb.write().await = Some(db);
                        delay_secs = reconnect_base_secs;
                        tracing::info!(target: PROJECT_ID, "Successfully reconnected to ScyllaDB");
                    }
                    Err(e) => {
                        tracing::warn!(target: PROJECT_ID, error = %e, delay_secs, "ScyllaDB initialization failed");
                        delay_secs = (delay_secs * 2).min(reconnect_max_secs);
                    }
                }
            }
        });
    }

    // Background task to cache indexer block height for response headers
    let indexer_block_cache = Arc::new(AtomicU64::new(0));
    {
        let cache = Arc::clone(&indexer_block_cache);
        let scylladb = Arc::clone(&scylladb);
        tokio::spawn(async move {
            loop {
                if let Some(db) = scylladb.read().await.as_ref() {
                    if let Ok(Some(h)) = db.get_indexer_block_height().await {
                        cache.store(h, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
    }

    HttpServer::new(move || {
        let block_cache = Arc::clone(&indexer_block_cache);

        // Configure CORS middleware
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["GET", "POST"])
            .allowed_headers(vec![
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                header::ACCEPT,
            ])
            .expose_headers(vec!["X-Results-Truncated", "X-Indexer-Block"])
            .max_age(3600);

        App::new()
            .app_data(web::Data::new(AppState {
                scylladb: Arc::clone(&scylladb),
                chain_id,
            }))
            .wrap(cors)
            .wrap_fn({
                let cache = block_cache;
                move |req, srv| {
                    let h = cache.load(Ordering::Relaxed);
                    let fut = srv.call(req);
                    async move {
                        let mut res = fut.await?;
                        if h > 0 {
                            res.headers_mut().insert(
                                header::HeaderName::from_static("x-indexer-block"),
                                header::HeaderValue::from(h),
                            );
                        }
                        Ok(res)
                    }
                }
            })
            .wrap(middleware::Compress::default())
            .wrap(middleware::Logger::new(
                "%{r}a \"%r\"	%s %b \"%{Referer}i\" \"%{User-Agent}i\" %T",
            ))
            .wrap(tracing_actix_web::TracingLogger::default())
            .service(Scalar::with_url("/docs", ApiDoc::openapi()))
            .service(health_check)
            .service(status_handler)
            .service(get_kv_handler)
            .service(query_kv_handler)
            .service(history_kv_handler)
            .service(writers_handler)
            .service(batch_kv_handler)
            .service(diff_kv_handler)
            .service(timeline_kv_handler)
            .service(accounts_handler)
            .service(edges_handler)
            .service(edges_count_handler)
            .service(social_get_handler)
            .service(social_keys_handler)
            .service(social_index_handler)
            .service(social_profile_handler)
            .service(social_followers_handler)
            .service(social_following_handler)
            .service(social_account_feed_handler)
            .service(
                Files::new("/", "./static")
                    .index_file("index.html")
            )
    })
    .bind(format!(
        "0.0.0.0:{}",
        env::var("PORT").unwrap_or_else(|_| "3001".to_string())
    ))?
    .run()
    .await?;

    Ok(())
}

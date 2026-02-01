mod handlers;
mod models;
mod queries;
mod scylladb;
mod social_handlers;
mod tree;

use crate::handlers::{accounts_count_handler, accounts_handler, batch_kv_handler, by_key_handler, count_kv_handler, get_kv_handler, health_check, history_kv_handler, keys_handler, query_kv_handler, reverse_kv_handler};
use crate::social_handlers::{
    social_get_handler, social_keys_handler, social_index_handler,
    social_profile_handler, social_followers_handler, social_following_handler,
    social_likes_handler, social_comments_handler, social_reposts_handler,
    social_account_feed_handler, social_hashtag_feed_handler, social_activity_feed_handler,
    social_notifications_handler,
};
use crate::scylladb::ScyllaDb;
use actix_cors::Cors;
use actix_web::http::header;
use actix_web::{middleware, web, App, HttpServer};
use dotenv::dotenv;
use fastnear_primitives::types::ChainId;
use std::env;
use std::sync::Arc;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

const PROJECT_ID: &str = "fastkv-server";

#[derive(OpenApi)]
#[openapi(
    paths(
        handlers::health_check,
        handlers::get_kv_handler,
        handlers::query_kv_handler,
        handlers::history_kv_handler,
        handlers::count_kv_handler,
        handlers::reverse_kv_handler,
        handlers::batch_kv_handler,
        handlers::by_key_handler,
        handlers::keys_handler,
        handlers::accounts_handler,
        handlers::accounts_count_handler,
        social_handlers::social_get_handler,
        social_handlers::social_keys_handler,
        social_handlers::social_index_handler,
        social_handlers::social_profile_handler,
        social_handlers::social_followers_handler,
        social_handlers::social_following_handler,
        social_handlers::social_likes_handler,
        social_handlers::social_comments_handler,
        social_handlers::social_reposts_handler,
        social_handlers::social_account_feed_handler,
        social_handlers::social_hashtag_feed_handler,
        social_handlers::social_activity_feed_handler,
        social_handlers::social_notifications_handler,
    ),
    components(schemas(
        models::KvEntry,
        models::QueryResponse,
        models::HealthResponse,
        models::CountResponse,
        models::GetParams,
        models::QueryParams,
        models::HistoryParams,
        models::ReverseParams,
        models::CountParams,
        models::ApiError,
        models::BatchQuery,
        models::BatchResultItem,
        models::BatchResponse,
        models::TreeResponse,
        models::ByKeyParams,
        models::KeysParams,
        models::KeysResponse,
        models::AccountsParams,
        models::AccountsResponse,
        models::AccountsCountParams,
        models::SocialGetBody,
        models::SocialGetOptions,
        models::SocialKeysBody,
        models::SocialKeysOptions,
        models::SocialIndexParams,
        models::SocialProfileParams,
        models::SocialFollowParams,
        models::SocialItemParams,
        models::SocialAccountFeedParams,
        models::SocialHashtagFeedParams,
        models::SocialActivityFeedParams,
        models::SocialNotificationsParams,
        models::IndexEntry,
        models::IndexResponse,
        models::SocialFollowResponse,
        models::SocialFeedResponse,
    )),
    info(
        title = "FastKV API",
        version = "1.0.0",
        description = "Query FastData KV entries from ScyllaDB. This API provides access to NEAR Protocol contract storage data."
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
    pub scylladb: Arc<ScyllaDb>,
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

    let scylla_session = ScyllaDb::new_scylla_session()
        .await
        .expect("Can't create scylla session");

    ScyllaDb::test_connection(&scylla_session)
        .await
        .expect("Can't connect to scylla");

    tracing::info!(target: PROJECT_ID, "Connected to Scylla");

    let scylladb = Arc::new(
        ScyllaDb::new(chain_id, scylla_session)
            .await
            .expect("Can't create scylla db"),
    );

    HttpServer::new(move || {
        // Configure CORS middleware
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["GET", "POST"])
            .allowed_headers(vec![
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                header::ACCEPT,
            ])
            .max_age(3600);

        App::new()
            .app_data(web::Data::new(AppState {
                scylladb: Arc::clone(&scylladb),
            }))
            .wrap(cors)
            .wrap(middleware::Compress::default())
            .wrap(middleware::Logger::new(
                "%{r}a \"%r\"	%s %b \"%{Referer}i\" \"%{User-Agent}i\" %T",
            ))
            .wrap(tracing_actix_web::TracingLogger::default())
            .service(Scalar::with_url("/docs", ApiDoc::openapi()))
            .service(health_check)
            .service(get_kv_handler)
            .service(query_kv_handler)
            .service(history_kv_handler)
            .service(count_kv_handler)
            .service(reverse_kv_handler)
            .service(batch_kv_handler)
            .service(by_key_handler)
            .service(keys_handler)
            .service(accounts_handler)
            .service(accounts_count_handler)
            .service(social_get_handler)
            .service(social_keys_handler)
            .service(social_index_handler)
            .service(social_profile_handler)
            .service(social_followers_handler)
            .service(social_following_handler)
            .service(social_likes_handler)
            .service(social_comments_handler)
            .service(social_reposts_handler)
            .service(social_account_feed_handler)
            .service(social_hashtag_feed_handler)
            .service(social_activity_feed_handler)
            .service(social_notifications_handler)
    })
    .bind(format!(
        "0.0.0.0:{}",
        env::var("PORT").unwrap_or_else(|_| "3001".to_string())
    ))?
    .run()
    .await?;

    Ok(())
}

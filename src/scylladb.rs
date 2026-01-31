use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;

use crate::models::{CountParams, HistoryParams, KvEntry, KvHistoryRow, KvRow, QueryParams, ReverseParams};
use crate::queries::build_prefix_query;
use fastnear_primitives::types::ChainId;
use rustls::pki_types::pem::PemObject;
use rustls::{ClientConfig, RootCertStore};
use std::collections::HashSet;
use std::env;
use std::sync::Arc;

pub struct ScyllaDb {
    get_kv: PreparedStatement,
    query_kv_no_prefix: PreparedStatement,
    reverse_kv: PreparedStatement,
    get_kv_history: PreparedStatement,

    pub scylla_session: Session,
    pub table_name: String,
    pub history_table_name: String,
    pub reverse_view_name: String,
}

pub fn create_rustls_client_config() -> Arc<ClientConfig> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("Failed to install default provider");
    }

    let ca_cert_path = env::var("SCYLLA_SSL_CA").expect("SCYLLA_SSL_CA required for TLS");
    let ca_certs = rustls::pki_types::CertificateDer::from_pem_file(&ca_cert_path)
        .unwrap_or_else(|e| panic!("Failed to load CA cert from '{}': {}", ca_cert_path, e));

    let mut root_store = RootCertStore::empty();
    root_store.add(ca_certs)
        .unwrap_or_else(|e| panic!("Failed to add CA certs to root store: {}", e));

    let config = match (env::var("SCYLLA_SSL_CERT").ok(), env::var("SCYLLA_SSL_KEY").ok()) {
        (Some(cert_path), Some(key_path)) => {
            let client_certs = rustls::pki_types::CertificateDer::from_pem_file(&cert_path)
                .unwrap_or_else(|e| panic!("Failed to load client cert from '{}': {}", cert_path, e));
            let client_key = rustls::pki_types::PrivateKeyDer::from_pem_file(&key_path)
                .unwrap_or_else(|e| panic!("Failed to load client key from '{}': {}", key_path, e));
            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(vec![client_certs], client_key)
                .expect("Failed to create client config with mTLS")
        }
        _ => {
            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        }
    };

    Arc::new(config)
}

impl ScyllaDb {
    pub async fn new_scylla_session() -> anyhow::Result<Session> {
        let scylla_url = env::var("SCYLLA_URL").expect("SCYLLA_URL must be set");
        let scylla_username = env::var("SCYLLA_USERNAME").expect("SCYLLA_USERNAME must be set");
        let scylla_password = env::var("SCYLLA_PASSWORD").expect("SCYLLA_PASSWORD must be set");

        let tls_config = env::var("SCYLLA_SSL_CA").ok().map(|_| create_rustls_client_config());

        let session: Session = SessionBuilder::new()
            .known_node(scylla_url)
            .tls_context(tls_config)
            .authenticator_provider(Arc::new(
                scylla::authentication::PlainTextAuthenticator::new(
                    scylla_username,
                    scylla_password,
                ),
            ))
            .build()
            .await?;

        Ok(session)
    }

    pub async fn test_connection(scylla_session: &Session) -> anyhow::Result<()> {
        scylla_session
            .query_unpaged("SELECT now() FROM system.local", &[])
            .await?;
        Ok(())
    }

    pub async fn new(chain_id: ChainId, scylla_session: Session) -> anyhow::Result<Self> {
        // Support custom keyspace or use default pattern
        let keyspace = env::var("KEYSPACE")
            .unwrap_or_else(|_| format!("fastdata_{chain_id}"));

        scylla_session
            .use_keyspace(keyspace, false)
            .await?;

        // Support custom table names
        let table_name = env::var("TABLE_NAME")
            .unwrap_or_else(|_| "s_kv_last".to_string());
        let history_table_name = env::var("HISTORY_TABLE_NAME")
            .unwrap_or_else(|_| "s_kv".to_string());
        let reverse_view_name = env::var("REVERSE_VIEW_NAME")
            .unwrap_or_else(|_| "mv_kv_cur_key".to_string());

        let columns = "predecessor_id, current_account_id, key, value, block_height, block_timestamp, receipt_id, tx_hash";
        let history_columns = "predecessor_id, current_account_id, key, block_height, order_id, value, block_timestamp, receipt_id, tx_hash, signer_id, shard_id, receipt_index, action_index";

        Ok(Self {
            get_kv: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE predecessor_id = ? AND current_account_id = ? AND key = ?", columns, table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            query_kv_no_prefix: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE predecessor_id = ? AND current_account_id = ?", columns, table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            reverse_kv: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE current_account_id = ? AND key = ?", columns, reverse_view_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            get_kv_history: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE predecessor_id = ? AND current_account_id = ? AND key = ?", history_columns, history_table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            scylla_session,
            table_name,
            history_table_name,
            reverse_view_name,
        })
    }

    pub async fn prepare_query(
        scylla_db_session: &Session,
        query_text: &str,
        consistency: scylla::frame::types::Consistency,
    ) -> anyhow::Result<PreparedStatement> {
        let mut query = scylla::statement::Statement::new(query_text);
        query.set_consistency(consistency);
        query.set_request_timeout(Some(std::time::Duration::from_secs(10)));
        Ok(scylla_db_session.prepare(query).await?)
    }

    pub async fn get_kv(
        &self,
        predecessor_id: &str,
        current_account_id: &str,
        key: &str,
    ) -> anyhow::Result<Option<KvEntry>> {
        let result = self
            .scylla_session
            .execute_unpaged(
                &self.get_kv,
                (predecessor_id, current_account_id, key),
            )
            .await?
            .into_rows_result()?;

        let entry = result
            .rows::<KvRow>()?
            .next()
            .transpose()?
            .map(KvEntry::from);

        Ok(entry)
    }

    pub async fn query_kv_with_pagination(
        &self,
        params: &QueryParams,
    ) -> anyhow::Result<Vec<KvEntry>> {
        let rows = if let Some(prefix) = &params.key_prefix {
            // Dynamic query with prefix range
            let (stmt, prefix_start, prefix_end) = build_prefix_query(prefix, &self.table_name);
            self.scylla_session
                .query_unpaged(
                    stmt,
                    (
                        &params.predecessor_id,
                        &params.current_account_id,
                        &prefix_start,
                        &prefix_end,
                    ),
                )
                .await?
                .into_rows_result()?
        } else {
            // Prepared query without prefix
            self.scylla_session
                .execute_unpaged(
                    &self.query_kv_no_prefix,
                    (&params.predecessor_id, &params.current_account_id),
                )
                .await?
                .into_rows_result()?
        };

        let entries: Vec<KvEntry> = rows
            .rows::<KvRow>()?
            .filter_map(|row_result| match row_result {
                Ok(row) => Some(row),
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in query_kv_with_pagination"
                    );
                    None
                }
            })
            .map(KvEntry::from)
            .filter(|entry| {
                // Apply exclude_null filter
                !params.exclude_null.unwrap_or(false) || entry.value != "null"
            })
            .skip(params.offset)
            .take(params.limit)
            .collect();

        Ok(entries)
    }

    pub async fn reverse_kv_with_dedup(
        &self,
        params: &ReverseParams,
    ) -> anyhow::Result<Vec<KvEntry>> {
        let rows = self
            .scylla_session
            .execute_unpaged(&self.reverse_kv, (&params.current_account_id, &params.key))
            .await?
            .into_rows_result()?;

        let mut seen_predecessors = HashSet::new();
        let entries: Vec<KvEntry> = rows
            .rows::<KvRow>()?
            .filter_map(|row_result| match row_result {
                Ok(row) => Some(row),
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in reverse_kv_with_dedup"
                    );
                    None
                }
            })
            .map(KvEntry::from)
            .filter(|entry| {
                // Deduplicate by predecessor_id (keep first = most recent)
                if seen_predecessors.contains(&entry.predecessor_id) {
                    false
                } else {
                    seen_predecessors.insert(entry.predecessor_id.clone());
                    true
                }
            })
            .filter(|entry| {
                // Apply exclude_null filter
                !params.exclude_null.unwrap_or(false) || entry.value != "null"
            })
            .skip(params.offset)
            .take(params.limit)
            .collect();

        Ok(entries)
    }

    pub async fn get_kv_history(
        &self,
        params: &HistoryParams,
    ) -> anyhow::Result<Vec<KvEntry>> {
        // Query s_kv table for full history
        // Note: s_kv has clustering order by current_account_id, key, block_height, order_id
        let result = self
            .scylla_session
            .execute_unpaged(
                &self.get_kv_history,
                (&params.predecessor_id, &params.current_account_id, &params.key),
            )
            .await?
            .into_rows_result()?;

        let mut entries: Vec<KvEntry> = result
            .rows::<KvHistoryRow>()?
            .filter_map(|row_result| match row_result {
                Ok(row) => Some(row),
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in get_kv_history"
                    );
                    None
                }
            })
            .map(KvEntry::from)
            .filter(|entry| {
                // Apply block height range filters if specified
                if let Some(from_block) = params.from_block {
                    if (entry.block_height as i64) < from_block {
                        return false;
                    }
                }
                if let Some(to_block) = params.to_block {
                    if (entry.block_height as i64) > to_block {
                        return false;
                    }
                }
                true
            })
            .collect();

        // Sort by block_height based on order parameter
        entries.sort_by(|a, b| {
            if params.order.to_lowercase() == "asc" {
                a.block_height.cmp(&b.block_height)
            } else {
                b.block_height.cmp(&a.block_height)
            }
        });

        // Apply limit after filtering and sorting
        entries.truncate(params.limit);

        Ok(entries)
    }

    pub async fn count_kv(
        &self,
        params: &CountParams,
    ) -> anyhow::Result<usize> {
        // Query rows to count them
        let rows = if let Some(prefix) = &params.key_prefix {
            // Dynamic query with prefix range
            let (stmt, prefix_start, prefix_end) = build_prefix_query(prefix, &self.table_name);
            self.scylla_session
                .query_unpaged(
                    stmt,
                    (
                        &params.predecessor_id,
                        &params.current_account_id,
                        &prefix_start,
                        &prefix_end,
                    ),
                )
                .await?
                .into_rows_result()?
        } else {
            // Prepared query without prefix
            self.scylla_session
                .execute_unpaged(
                    &self.query_kv_no_prefix,
                    (&params.predecessor_id, &params.current_account_id),
                )
                .await?
                .into_rows_result()?
        };

        // Count rows efficiently without full deserialization
        let count = rows
            .rows::<KvRow>()?
            .filter_map(|row_result| match row_result {
                Ok(_) => Some(()),
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in count_kv"
                    );
                    None
                }
            })
            .count();

        Ok(count)
    }
}

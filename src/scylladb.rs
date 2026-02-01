use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;

use crate::models::{CountParams, HistoryParams, KvEntry, KvHistoryRow, KvRow, QueryParams, ReverseParams};
use crate::queries::build_prefix_query;
use fastnear_primitives::types::ChainId;
use rustls::pki_types::pem::PemObject;
use rustls::{ClientConfig, RootCertStore};
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;

pub struct ScyllaDb {
    get_kv: PreparedStatement,
    get_kv_last: PreparedStatement,
    query_kv_no_prefix: PreparedStatement,
    reverse_kv: PreparedStatement,
    get_kv_history: PreparedStatement,

    pub scylla_session: Session,
    pub table_name: String,
    pub history_table_name: String,
    pub reverse_view_name: String,
}

pub fn create_rustls_client_config() -> anyhow::Result<Arc<ClientConfig>> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("Failed to install default provider");
    }

    let ca_cert_path = env::var("SCYLLA_SSL_CA")
        .map_err(|_| anyhow::anyhow!("SCYLLA_SSL_CA required for TLS"))?;
    let ca_certs = rustls::pki_types::CertificateDer::from_pem_file(&ca_cert_path)
        .map_err(|e| anyhow::anyhow!("Failed to load CA cert from '{}': {}", ca_cert_path, e))?;

    let mut root_store = RootCertStore::empty();
    root_store.add(ca_certs)
        .map_err(|e| anyhow::anyhow!("Failed to add CA certs to root store: {}", e))?;

    let config = match (env::var("SCYLLA_SSL_CERT").ok(), env::var("SCYLLA_SSL_KEY").ok()) {
        (Some(cert_path), Some(key_path)) => {
            let client_certs = rustls::pki_types::CertificateDer::from_pem_file(&cert_path)
                .map_err(|e| anyhow::anyhow!("Failed to load client cert from '{}': {}", cert_path, e))?;
            let client_key = rustls::pki_types::PrivateKeyDer::from_pem_file(&key_path)
                .map_err(|e| anyhow::anyhow!("Failed to load client key from '{}': {}", key_path, e))?;
            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(vec![client_certs], client_key)
                .map_err(|e| anyhow::anyhow!("Failed to create client config with mTLS: {}", e))?
        }
        _ => {
            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        }
    };

    Ok(Arc::new(config))
}

impl ScyllaDb {
    pub async fn new_scylla_session() -> anyhow::Result<Session> {
        let scylla_url = env::var("SCYLLA_URL").expect("SCYLLA_URL must be set");
        let scylla_username = env::var("SCYLLA_USERNAME").expect("SCYLLA_USERNAME must be set");
        let scylla_password = env::var("SCYLLA_PASSWORD").expect("SCYLLA_PASSWORD must be set");

        let tls_config = match env::var("SCYLLA_SSL_CA").ok() {
            Some(_) => Some(create_rustls_client_config()?),
            None => None,
        };

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
        let mut stmt = scylla::statement::Statement::new("SELECT now() FROM system.local");
        stmt.set_request_timeout(Some(std::time::Duration::from_secs(10)));
        scylla_session
            .query_unpaged(stmt, &[])
            .await?;
        Ok(())
    }

    pub async fn health_check(&self) -> anyhow::Result<()> {
        // Simple query to verify connection
        let mut stmt = scylla::statement::Statement::new("SELECT now() FROM system.local");
        stmt.set_request_timeout(Some(std::time::Duration::from_secs(10)));
        self.scylla_session
            .query_unpaged(stmt, &[])
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
            get_kv_last: Self::prepare_query(
                &scylla_session,
                &format!("SELECT value FROM {} WHERE predecessor_id = ? AND current_account_id = ? AND key = ?", table_name),
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

    pub async fn get_kv_last(
        &self,
        predecessor_id: &str,
        current_account_id: &str,
        key: &str,
    ) -> anyhow::Result<Option<String>> {
        let result = self
            .scylla_session
            .execute_unpaged(
                &self.get_kv_last,
                (predecessor_id, current_account_id, key),
            )
            .await?
            .into_rows_result()?;

        let value = result
            .rows::<(Option<String>,)>()?
            .next()
            .transpose()?
            .and_then(|row| row.0);

        Ok(value)
    }

    pub async fn query_kv_with_pagination(
        &self,
        params: &QueryParams,
    ) -> anyhow::Result<Vec<KvEntry>> {
        let mut rows_stream = if let Some(prefix) = &params.key_prefix {
            // Dynamic query with prefix range
            let (stmt, prefix_start, prefix_end) = build_prefix_query(prefix, &self.table_name);
            self.scylla_session
                .query_iter(
                    stmt,
                    (
                        &params.predecessor_id,
                        &params.current_account_id,
                        &prefix_start,
                        &prefix_end,
                    ),
                )
                .await?
                .rows_stream::<KvRow>()?
        } else {
            // Prepared query without prefix
            self.scylla_session
                .execute_iter(
                    self.query_kv_no_prefix.clone(),
                    (&params.predecessor_id, &params.current_account_id),
                )
                .await?
                .rows_stream::<KvRow>()?
        };

        let exclude_null = params.exclude_null.unwrap_or(false);
        let mut skipped = 0;
        let mut entries = Vec::with_capacity(params.limit);

        while let Some(row_result) = rows_stream.next().await {
            let row = match row_result {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in query_kv_with_pagination"
                    );
                    continue;
                }
            };

            let entry = KvEntry::from(row);

            if exclude_null && entry.value == "null" {
                continue;
            }

            if skipped < params.offset {
                skipped += 1;
                continue;
            }

            entries.push(entry);
            if entries.len() >= params.limit {
                break;
            }
        }

        Ok(entries)
    }

    pub async fn reverse_kv_with_dedup(
        &self,
        params: &ReverseParams,
    ) -> anyhow::Result<Vec<KvEntry>> {
        let mut rows_stream = self
            .scylla_session
            .execute_iter(self.reverse_kv.clone(), (&params.current_account_id, &params.key))
            .await?
            .rows_stream::<KvRow>()?;

        let mut seen_predecessors = HashSet::new();
        let mut entries = Vec::new();
        let target_count = params.offset + params.limit;

        while let Some(row_result) = rows_stream.next().await {
            // Safety limit: stop if we've seen too many unique predecessors
            if seen_predecessors.len() >= 100_000 {
                break;
            }

            let row = match row_result {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in reverse_kv_with_dedup"
                    );
                    continue;
                }
            };

            let entry = KvEntry::from(row);

            // Deduplicate by predecessor_id (keep first = most recent)
            if seen_predecessors.contains(&entry.predecessor_id) {
                continue;
            }

            seen_predecessors.insert(entry.predecessor_id.clone());

            // Apply exclude_null filter
            if params.exclude_null.unwrap_or(false) && entry.value == "null" {
                continue;
            }

            entries.push(entry);

            // Early termination when we have enough entries
            if entries.len() >= target_count {
                break;
            }
        }

        // Apply offset and limit
        let result: Vec<KvEntry> = entries
            .into_iter()
            .skip(params.offset)
            .take(params.limit)
            .collect();

        Ok(result)
    }

    pub async fn get_kv_history(
        &self,
        params: &HistoryParams,
    ) -> anyhow::Result<Vec<KvEntry>> {
        const MAX_HISTORY_SCAN: usize = 10_000;

        // Query s_kv table for full history using streaming
        // Note: s_kv has clustering order by current_account_id, key, block_height, order_id
        let mut rows_stream = self
            .scylla_session
            .execute_iter(
                self.get_kv_history.clone(),
                (&params.predecessor_id, &params.current_account_id, &params.key),
            )
            .await?
            .rows_stream::<KvHistoryRow>()?;

        let mut entries: Vec<KvEntry> = Vec::new();
        let mut scanned = 0usize;

        while let Some(row_result) = rows_stream.next().await {
            scanned += 1;
            if scanned > MAX_HISTORY_SCAN {
                break;
            }

            let row = match row_result {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in get_kv_history"
                    );
                    continue;
                }
            };

            let entry = KvEntry::from(row);

            // Apply block height range filters if specified
            if let Some(from_block) = params.from_block {
                if (entry.block_height as i64) < from_block {
                    continue;
                }
            }
            if let Some(to_block) = params.to_block {
                if (entry.block_height as i64) > to_block {
                    continue;
                }
            }

            entries.push(entry);
        }

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
    ) -> anyhow::Result<(usize, bool)> {
        // Primary path: Use COUNT(*) for efficient counting
        let count_result = if let Some(prefix) = &params.key_prefix {
            // Build COUNT(*) query with prefix range
            let query_text = format!(
                "SELECT COUNT(*) FROM {} WHERE predecessor_id = ? AND current_account_id = ? AND key >= ? AND key < ?",
                self.table_name
            );
            let prefix_start = prefix.to_string();
            let prefix_end = format!("{}\u{ff}", prefix); // Compute prefix end inline

            let mut stmt = scylla::statement::Statement::new(query_text);
            stmt.set_request_timeout(Some(std::time::Duration::from_secs(10)));

            self.scylla_session
                .query_unpaged(
                    stmt,
                    (&params.predecessor_id, &params.current_account_id, &prefix_start, &prefix_end),
                )
                .await
        } else {
            // Build COUNT(*) query without prefix
            let query_text = format!(
                "SELECT COUNT(*) FROM {} WHERE predecessor_id = ? AND current_account_id = ?",
                self.table_name
            );

            let mut stmt = scylla::statement::Statement::new(query_text);
            stmt.set_request_timeout(Some(std::time::Duration::from_secs(10)));

            self.scylla_session
                .query_unpaged(
                    stmt,
                    (&params.predecessor_id, &params.current_account_id),
                )
                .await
        };

        // If COUNT(*) succeeds, parse and return the result
        if let Ok(result) = count_result {
            if let Ok(rows_result) = result.into_rows_result() {
                if let Some(Ok(row)) = rows_result.rows::<(i64,)>()?.next() {
                    let count = row.0.max(0) as usize;
                    tracing::debug!(
                        target: "fastkv-server",
                        count = count,
                        predecessor_id = %params.predecessor_id,
                        current_account_id = %params.current_account_id,
                        "COUNT(*) query succeeded"
                    );
                    return Ok((count, false));
                }
            }
        }

        // Fallback: COUNT(*) failed, iterate through rows (legacy method)
        tracing::warn!(
            target: "fastkv-server",
            predecessor_id = %params.predecessor_id,
            current_account_id = %params.current_account_id,
            "COUNT(*) query failed, falling back to row iteration"
        );

        let mut rows_stream = if let Some(prefix) = &params.key_prefix {
            // Dynamic query with prefix range
            let (stmt, prefix_start, prefix_end) = build_prefix_query(prefix, &self.table_name);
            self.scylla_session
                .query_iter(
                    stmt,
                    (
                        &params.predecessor_id,
                        &params.current_account_id,
                        &prefix_start,
                        &prefix_end,
                    ),
                )
                .await?
                .rows_stream::<KvRow>()?
        } else {
            // Prepared query without prefix
            self.scylla_session
                .execute_iter(
                    self.query_kv_no_prefix.clone(),
                    (&params.predecessor_id, &params.current_account_id),
                )
                .await?
                .rows_stream::<KvRow>()?
        };

        // Count rows via streaming, limit to 1M
        let mut count = 0;
        let mut estimated = false;
        while let Some(row_result) = rows_stream.next().await {
            match row_result {
                Ok(_) => {
                    count += 1;
                    if count >= 1_000_000 {
                        estimated = true;
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in count_kv"
                    );
                }
            }
        }

        Ok((count, estimated))
    }
}

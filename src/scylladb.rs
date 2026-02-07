use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;

use crate::models::{AccountsParams, ContractAccountRow, EdgeRow, EdgeSourceEntry, HistoryParams, KvEntry, KvHistoryRow, KvRow, QueryParams, WritersParams, TimelineParams, MAX_DEDUP_SCAN, MAX_HISTORY_SCAN, bigint_to_u64};
use crate::queries::{build_prefix_query, build_prefix_cursor_query};
use fastnear_primitives::types::ChainId;
use rustls::pki_types::pem::PemObject;
use rustls::{ClientConfig, RootCertStore};
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;

/// Validate that a CQL identifier (keyspace/table name) contains only safe characters.
pub(crate) fn validate_identifier(name: &str, label: &str) -> anyhow::Result<()> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        anyhow::bail!("{} must contain only alphanumeric characters and underscores: {}", label, name);
    }
    Ok(())
}

pub struct ScyllaDb {
    get_kv: PreparedStatement,
    get_kv_last: PreparedStatement,
    query_kv_no_prefix: PreparedStatement,
    query_kv_cursor: PreparedStatement,
    pub(crate) reverse_kv: PreparedStatement,
    reverse_list: PreparedStatement,
    reverse_list_cursor: PreparedStatement,
    get_kv_history: PreparedStatement,
    get_kv_at_block: PreparedStatement,
    get_kv_timeline: PreparedStatement,
    accounts_by_contract: PreparedStatement,
    accounts_by_contract_key: PreparedStatement,
    edges_list: PreparedStatement,
    edges_list_cursor: PreparedStatement,
    edges_count: PreparedStatement,
    meta_query: PreparedStatement,

    pub scylla_session: Session,
    pub table_name: String,
    pub history_table_name: String,
    pub reverse_view_name: String,
    pub kv_accounts_table_name: String,
    pub kv_edges_table_name: String,
    pub kv_reverse_table_name: String,
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

        validate_identifier(&keyspace, "KEYSPACE")?;

        scylla_session
            .use_keyspace(&keyspace, false)
            .await?;

        // Support custom table names
        let table_name = env::var("TABLE_NAME")
            .unwrap_or_else(|_| "s_kv_last".to_string());
        let history_table_name = env::var("HISTORY_TABLE_NAME")
            .unwrap_or_else(|_| "s_kv".to_string());
        let reverse_view_name = env::var("REVERSE_VIEW_NAME")
            .unwrap_or_else(|_| "mv_kv_cur_key".to_string());
        let kv_accounts_table_name = env::var("KV_ACCOUNTS_TABLE_NAME")
            .unwrap_or_else(|_| "kv_accounts".to_string());
        let kv_edges_table_name = env::var("KV_EDGES_TABLE_NAME")
            .unwrap_or_else(|_| "kv_edges".to_string());
        let kv_reverse_table_name = env::var("KV_REVERSE_TABLE_NAME")
            .unwrap_or_else(|_| "kv_reverse".to_string());

        validate_identifier(&table_name, "TABLE_NAME")?;
        validate_identifier(&history_table_name, "HISTORY_TABLE_NAME")?;
        validate_identifier(&reverse_view_name, "REVERSE_VIEW_NAME")?;
        validate_identifier(&kv_accounts_table_name, "KV_ACCOUNTS_TABLE_NAME")?;
        validate_identifier(&kv_edges_table_name, "KV_EDGES_TABLE_NAME")?;
        validate_identifier(&kv_reverse_table_name, "KV_REVERSE_TABLE_NAME")?;

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
            query_kv_cursor: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE predecessor_id = ? AND current_account_id = ? AND key > ?", columns, table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            reverse_kv: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE current_account_id = ? AND key = ? ORDER BY key DESC, block_height DESC, order_id DESC, predecessor_id DESC", columns, reverse_view_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            reverse_list: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE current_account_id = ? AND key = ?", columns, kv_reverse_table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            reverse_list_cursor: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE current_account_id = ? AND key = ? AND predecessor_id > ?", columns, kv_reverse_table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            get_kv_history: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE predecessor_id = ? AND current_account_id = ? AND key = ? AND block_height >= ? AND block_height <= ?", history_columns, history_table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            get_kv_at_block: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE predecessor_id = ? AND current_account_id = ? AND key = ? AND block_height = ?", history_columns, history_table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            get_kv_timeline: Self::prepare_query(
                &scylla_session,
                &format!("SELECT {} FROM {} WHERE predecessor_id = ? AND current_account_id = ?", history_columns, history_table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            // LocalQuorum for kv_accounts: this table is populated asynchronously so
            // LocalOne reads could return stale/partial results after recent writes.
            accounts_by_contract: Self::prepare_query(
                &scylla_session,
                &format!("SELECT predecessor_id FROM {} WHERE current_account_id = ?", kv_accounts_table_name),
                scylla::frame::types::Consistency::LocalQuorum,
            ).await?,
            accounts_by_contract_key: Self::prepare_query(
                &scylla_session,
                &format!("SELECT predecessor_id FROM {} WHERE current_account_id = ? AND key = ?", kv_accounts_table_name),
                scylla::frame::types::Consistency::LocalQuorum,
            ).await?,
            edges_list: Self::prepare_query(
                &scylla_session,
                &format!("SELECT source, block_height FROM {} WHERE edge_type = ? AND target = ?", kv_edges_table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            edges_list_cursor: Self::prepare_query(
                &scylla_session,
                &format!("SELECT source, block_height FROM {} WHERE edge_type = ? AND target = ? AND source > ?", kv_edges_table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            edges_count: Self::prepare_query(
                &scylla_session,
                &format!("SELECT COUNT(*) FROM {} WHERE edge_type = ? AND target = ?", kv_edges_table_name),
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            meta_query: Self::prepare_query(
                &scylla_session,
                "SELECT last_processed_block_height FROM meta WHERE suffix = ?",
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            scylla_session,
            table_name,
            history_table_name,
            reverse_view_name,
            kv_accounts_table_name,
            kv_edges_table_name,
            kv_reverse_table_name,
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

    /// Query writers for a key under a contract using the kv_reverse table.
    /// predecessor_id is the clustering key so rows are naturally deduplicated.
    /// Supports cursor pagination via `after_account`.
    /// Optionally filters to a specific writer (predecessor_id).
    /// Returns (entries, has_more, truncated).
    pub async fn query_writers(
        &self,
        params: &WritersParams,
    ) -> anyhow::Result<(Vec<KvEntry>, bool, bool)> {
        let mut rows_stream = match &params.after_account {
            Some(cursor) => {
                self.scylla_session
                    .execute_iter(self.reverse_list_cursor.clone(), (&params.current_account_id, &params.key, cursor))
                    .await?
                    .rows_stream::<KvRow>()?
            }
            None => {
                self.scylla_session
                    .execute_iter(self.reverse_list.clone(), (&params.current_account_id, &params.key))
                    .await?
                    .rows_stream::<KvRow>()?
            }
        };

        let mut entries = Vec::new();
        let mut skipped = 0;

        while let Some(row_result) = rows_stream.next().await {
            let row = match row_result {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in query_writers"
                    );
                    continue;
                }
            };

            let entry = KvEntry::from(row);

            // Filter by specific writer if requested
            if let Some(ref pred) = params.predecessor_id {
                if entry.predecessor_id != *pred {
                    continue;
                }
            }

            // Apply exclude_null filter
            if params.exclude_null.unwrap_or(false) && entry.value == "null" {
                continue;
            }

            // Apply offset only when not using cursor
            if params.after_account.is_none() && skipped < params.offset {
                skipped += 1;
                continue;
            }

            entries.push(entry);
            if entries.len() > params.limit {
                break;
            }
        }

        let has_more = entries.len() > params.limit;
        entries.truncate(params.limit);

        Ok((entries, has_more, false))
    }

    pub async fn query_accounts(
        &self,
        params: &AccountsParams,
    ) -> anyhow::Result<(Vec<String>, bool)> {
        // Use kv_reverse table for CQL-level cursor pagination
        let mut rows_stream = match &params.after_account {
            Some(cursor) => {
                self.scylla_session
                    .execute_iter(self.reverse_list_cursor.clone(), (&params.current_account_id, &params.key, cursor))
                    .await?
                    .rows_stream::<KvRow>()?
            }
            None => {
                self.scylla_session
                    .execute_iter(self.reverse_list.clone(), (&params.current_account_id, &params.key))
                    .await?
                    .rows_stream::<KvRow>()?
            }
        };

        let mut accounts = Vec::new();
        let mut skipped = 0;

        while let Some(row_result) = rows_stream.next().await {
            let row = match row_result {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in query_accounts"
                    );
                    continue;
                }
            };

            if params.exclude_null.unwrap_or(false) && row.value == "null" {
                continue;
            }

            // Apply offset only when not using cursor
            if params.after_account.is_none() && skipped < params.offset {
                skipped += 1;
                continue;
            }

            accounts.push(row.predecessor_id);
            if accounts.len() > params.limit {
                break;
            }
        }

        let has_more = accounts.len() > params.limit;
        accounts.truncate(params.limit);

        Ok((accounts, has_more))
    }

    /// Returns `(accounts, has_more, truncated)`.
    /// `truncated` is true if scan hit MAX_DEDUP_SCAN.
    pub async fn query_accounts_by_contract(
        &self,
        contract_id: &str,
        key: Option<&str>,
        limit: usize,
        offset: usize,
        after_account: Option<&str>,
    ) -> anyhow::Result<(Vec<String>, bool, bool)> {
        let needs_dedup = key.is_none();

        let mut rows_stream = match key {
            Some(k) => self
                .scylla_session
                .execute_iter(
                    self.accounts_by_contract_key.clone(),
                    (contract_id, k),
                )
                .await?
                .rows_stream::<ContractAccountRow>()?,
            None => self
                .scylla_session
                .execute_iter(
                    self.accounts_by_contract.clone(),
                    (contract_id,),
                )
                .await?
                .rows_stream::<ContractAccountRow>()?,
        };

        let mut seen = HashSet::new();
        let mut accounts = Vec::new();
        let target_count = offset + limit + 1;
        let mut truncated = false;

        while let Some(row_result) = rows_stream.next().await {
            if needs_dedup && seen.len() >= MAX_DEDUP_SCAN {
                truncated = true;
                break;
            }

            let row = match row_result {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in query_accounts_by_contract"
                    );
                    continue;
                }
            };

            if needs_dedup {
                if seen.contains(&row.predecessor_id) {
                    continue;
                }
                seen.insert(row.predecessor_id.clone());
            }

            // Apply cursor: skip accounts <= after_account
            if let Some(cursor) = after_account {
                if row.predecessor_id.as_str() <= cursor {
                    continue;
                }
            }

            accounts.push(row.predecessor_id);

            if accounts.len() >= target_count {
                break;
            }
        }

        let result: Vec<String> = if after_account.is_some() {
            // When using cursor, no offset skipping needed
            accounts
        } else {
            accounts.into_iter().skip(offset).collect()
        };
        let has_more = result.len() > limit;
        let mut result = result;
        result.truncate(limit);

        Ok((result, has_more, truncated))
    }

    /// Returns (entries, has_more).
    pub async fn query_kv_with_pagination(
        &self,
        params: &QueryParams,
    ) -> anyhow::Result<(Vec<KvEntry>, bool)> {
        let mut rows_stream = match (&params.key_prefix, &params.after_key) {
            // Prefix + cursor: key > cursor AND key < prefix_end
            (Some(prefix), Some(cursor)) => {
                let (stmt, cursor_start, prefix_end) = build_prefix_cursor_query(cursor, prefix, &self.table_name);
                self.scylla_session
                    .query_iter(
                        stmt,
                        (
                            &params.predecessor_id,
                            &params.current_account_id,
                            &cursor_start,
                            &prefix_end,
                        ),
                    )
                    .await?
                    .rows_stream::<KvRow>()?
            }
            // Prefix only: key >= prefix AND key < prefix_end
            (Some(prefix), None) => {
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
            }
            // No prefix + cursor: key > cursor
            (None, Some(cursor)) => {
                self.scylla_session
                    .execute_iter(
                        self.query_kv_cursor.clone(),
                        (&params.predecessor_id, &params.current_account_id, cursor),
                    )
                    .await?
                    .rows_stream::<KvRow>()?
            }
            // No prefix, no cursor: all keys
            (None, None) => {
                self.scylla_session
                    .execute_iter(
                        self.query_kv_no_prefix.clone(),
                        (&params.predecessor_id, &params.current_account_id),
                    )
                    .await?
                    .rows_stream::<KvRow>()?
            }
        };

        let exclude_null = params.exclude_null.unwrap_or(false);
        let mut skipped = 0;
        let mut entries = Vec::with_capacity(params.limit + 1);

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

            // Apply offset only when not using cursor
            if params.after_key.is_none() && skipped < params.offset {
                skipped += 1;
                continue;
            }

            entries.push(entry);
            if entries.len() > params.limit {
                break;
            }
        }

        let has_more = entries.len() > params.limit;
        entries.truncate(params.limit);
        Ok((entries, has_more))
    }

    pub async fn get_kv_at_block(
        &self,
        predecessor_id: &str,
        current_account_id: &str,
        key: &str,
        block_height: i64,
    ) -> anyhow::Result<Option<KvEntry>> {
        let result = self
            .scylla_session
            .execute_unpaged(
                &self.get_kv_at_block,
                (predecessor_id, current_account_id, key, block_height),
            )
            .await?
            .into_rows_result()?;

        // Multiple rows can exist per block_height (different order_id values);
        // .last() takes the highest order_id since clustering order is ASC.
        let entry = result
            .rows::<KvHistoryRow>()?
            .filter_map(|r| r.ok())
            .last()
            .map(KvEntry::from);

        Ok(entry)
    }

    /// Returns (entries, has_more, truncated).
    pub async fn get_kv_timeline(
        &self,
        params: &TimelineParams,
    ) -> anyhow::Result<(Vec<KvEntry>, bool, bool)> {
        let mut rows_stream = self
            .scylla_session
            .execute_iter(
                self.get_kv_timeline.clone(),
                (&params.predecessor_id, &params.current_account_id),
            )
            .await?
            .rows_stream::<KvHistoryRow>()?;

        let mut entries: Vec<KvEntry> = Vec::new();
        let mut scanned = 0usize;
        let mut truncated = false;

        while let Some(row_result) = rows_stream.next().await {
            scanned += 1;
            if scanned > MAX_HISTORY_SCAN {
                truncated = true;
                break;
            }

            let row = match row_result {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(target: "fastkv-server", error = %e, "Failed to deserialize row in get_kv_timeline");
                    continue;
                }
            };

            let entry = KvEntry::from(row);

            if let Some(from_block) = params.from_block {
                if entry.block_height < (from_block.max(0) as u64) {
                    continue;
                }
            }
            if let Some(to_block) = params.to_block {
                if entry.block_height > (to_block.max(0) as u64) {
                    continue;
                }
            }

            entries.push(entry);
        }

        entries.sort_by(|a, b| {
            if params.order.to_lowercase() == "asc" {
                a.block_height.cmp(&b.block_height)
            } else {
                b.block_height.cmp(&a.block_height)
            }
        });

        let result: Vec<KvEntry> = entries
            .into_iter()
            .skip(params.offset)
            .collect();
        let has_more = result.len() > params.limit;
        let mut result = result;
        result.truncate(params.limit);

        Ok((result, has_more, truncated))
    }

    /// Returns (entries, has_more).
    pub async fn query_edges(
        &self,
        edge_type: &str,
        target: &str,
        limit: usize,
        offset: usize,
        after_source: Option<&str>,
    ) -> anyhow::Result<(Vec<EdgeSourceEntry>, bool)> {
        let mut rows_stream = match after_source {
            Some(cursor) => {
                self.scylla_session
                    .execute_iter(self.edges_list_cursor.clone(), (edge_type, target, cursor))
                    .await?
                    .rows_stream::<EdgeRow>()?
            }
            None => {
                self.scylla_session
                    .execute_iter(self.edges_list.clone(), (edge_type, target))
                    .await?
                    .rows_stream::<EdgeRow>()?
            }
        };

        let mut skipped = 0;
        let mut entries = Vec::with_capacity(limit + 1);

        while let Some(row_result) = rows_stream.next().await {
            let row = match row_result {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(
                        target: "fastkv-server",
                        error = %e,
                        "Failed to deserialize row in query_edges"
                    );
                    continue;
                }
            };

            if after_source.is_none() && skipped < offset {
                skipped += 1;
                continue;
            }

            entries.push(EdgeSourceEntry {
                source: row.source,
                block_height: bigint_to_u64(row.block_height),
            });

            if entries.len() > limit {
                break;
            }
        }

        let has_more = entries.len() > limit;
        entries.truncate(limit);
        Ok((entries, has_more))
    }

    pub async fn count_edges(
        &self,
        edge_type: &str,
        target: &str,
    ) -> anyhow::Result<usize> {
        let result = self
            .scylla_session
            .execute_unpaged(&self.edges_count, (edge_type, target))
            .await?
            .into_rows_result()?;

        let count = result
            .rows::<(i64,)>()?
            .next()
            .transpose()?
            .map(|row| row.0.max(0) as usize)
            .unwrap_or(0);

        Ok(count)
    }

    /// Returns (entries, has_more, truncated).
    pub async fn get_kv_history(
        &self,
        params: &HistoryParams,
    ) -> anyhow::Result<(Vec<KvEntry>, bool, bool)> {
        let from_block = params.from_block.unwrap_or(0);
        let to_block = params.to_block.unwrap_or(i64::MAX);

        let mut rows_stream = self
            .scylla_session
            .execute_iter(
                self.get_kv_history.clone(),
                (&params.predecessor_id, &params.current_account_id, &params.key, from_block, to_block),
            )
            .await?
            .rows_stream::<KvHistoryRow>()?;

        let mut entries: Vec<KvEntry> = Vec::new();
        let mut scanned = 0usize;
        let mut truncated = false;

        while let Some(row_result) = rows_stream.next().await {
            scanned += 1;
            if scanned > MAX_HISTORY_SCAN {
                truncated = true;
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

            entries.push(KvEntry::from(row));
        }

        entries.sort_by(|a, b| {
            if params.order.to_lowercase() == "asc" {
                a.block_height.cmp(&b.block_height)
            } else {
                b.block_height.cmp(&a.block_height)
            }
        });

        let has_more = entries.len() > params.limit;
        entries.truncate(params.limit);

        Ok((entries, has_more, truncated))
    }

    pub async fn get_indexer_block_height(&self) -> anyhow::Result<Option<u64>> {
        let result = self
            .scylla_session
            .execute_unpaged(&self.meta_query, ("kv-1",))
            .await?
            .into_rows_result()?;

        let block_height = result
            .rows::<(i64,)>()?
            .next()
            .transpose()?
            .map(|row| row.0.max(0) as u64);

        Ok(block_height)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier_accepts_valid() {
        assert!(validate_identifier("fastdata_mainnet", "TEST").is_ok());
        assert!(validate_identifier("s_kv_last", "TEST").is_ok());
        assert!(validate_identifier("mv_kv_by_contract", "TEST").is_ok());
        assert!(validate_identifier("A1_b2", "TEST").is_ok());
    }

    #[test]
    fn test_validate_identifier_rejects_injection() {
        assert!(validate_identifier("; DROP TABLE users", "TEST").is_err());
        assert!(validate_identifier("table; --", "TEST").is_err());
        assert!(validate_identifier("name with spaces", "TEST").is_err());
        assert!(validate_identifier("", "TEST").is_err());
        assert!(validate_identifier("table.name", "TEST").is_err());
        assert!(validate_identifier("name-with-dashes", "TEST").is_err());
    }
}

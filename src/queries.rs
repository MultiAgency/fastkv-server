use scylla::frame::types::Consistency;
use scylla::statement::Statement;

pub(crate) fn build_prefix_query(prefix: &str, table_name: &str) -> (Statement, String, String) {
    let columns = "predecessor_id, current_account_id, key, value, block_height, block_timestamp, receipt_id, tx_hash";
    let query_text = format!(
        "SELECT {} FROM {} WHERE predecessor_id = ? AND current_account_id = ? AND key >= ? AND key < ?",
        columns, table_name
    );

    let mut stmt = Statement::new(query_text);
    stmt.set_consistency(Consistency::LocalOne);
    stmt.set_request_timeout(Some(std::time::Duration::from_secs(10)));

    let prefix_start = prefix.to_string();
    let prefix_end = compute_prefix_end(prefix);

    (stmt, prefix_start, prefix_end)
}

/// Build a prefix query with a cursor: key > cursor AND key < prefix_end.
/// Used when `after_key` cursor is provided alongside a key_prefix.
pub(crate) fn build_prefix_cursor_query(cursor: &str, prefix: &str, table_name: &str) -> (Statement, String, String) {
    let columns = "predecessor_id, current_account_id, key, value, block_height, block_timestamp, receipt_id, tx_hash";
    let query_text = format!(
        "SELECT {} FROM {} WHERE predecessor_id = ? AND current_account_id = ? AND key > ? AND key < ?",
        columns, table_name
    );

    let mut stmt = Statement::new(query_text);
    stmt.set_consistency(Consistency::LocalOne);
    stmt.set_request_timeout(Some(std::time::Duration::from_secs(10)));

    let cursor_start = cursor.to_string();
    let prefix_end = compute_prefix_end(prefix);

    (stmt, cursor_start, prefix_end)
}

pub(crate) fn compute_prefix_end(prefix: &str) -> String {
    // For prefix "graph/follow/", return "graph/follow/\xff"
    // This ensures we capture all keys starting with the prefix
    format!("{}\u{ff}", prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_prefix_end() {
        assert_eq!(compute_prefix_end("graph/follow/"), "graph/follow/\u{ff}");
        assert_eq!(compute_prefix_end("test"), "test\u{ff}");
        assert_eq!(compute_prefix_end(""), "\u{ff}");
    }

    #[test]
    fn test_build_prefix_query() {
        let (_stmt, start, end) = build_prefix_query("graph/follow/", "s_kv_last");
        assert_eq!(start, "graph/follow/");
        assert_eq!(end, "graph/follow/\u{ff}");
    }

    #[test]
    fn test_build_prefix_cursor_query() {
        let (_stmt, cursor, end) = build_prefix_cursor_query("graph/follow/alice.near", "graph/follow/", "s_kv_last");
        assert_eq!(cursor, "graph/follow/alice.near");
        assert_eq!(end, "graph/follow/\u{ff}");
    }
}

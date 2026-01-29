use scylla::frame::types::Consistency;
use scylla::statement::Statement;

pub fn build_prefix_query(prefix: &str) -> (Statement, String, String) {
    let query_text =
        "SELECT predecessor_id, current_account_id, key, value, block_height, block_timestamp, receipt_id, tx_hash FROM s_kv_last WHERE predecessor_id = ? AND current_account_id = ? AND key >= ? AND key < ?";

    let mut stmt = Statement::new(query_text);
    stmt.set_consistency(Consistency::LocalOne);

    let prefix_start = prefix.to_string();
    let prefix_end = compute_prefix_end(prefix);

    (stmt, prefix_start, prefix_end)
}

fn compute_prefix_end(prefix: &str) -> String {
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
        let (_stmt, start, end) = build_prefix_query("graph/follow/");
        assert_eq!(start, "graph/follow/");
        assert_eq!(end, "graph/follow/\u{ff}");
    }
}

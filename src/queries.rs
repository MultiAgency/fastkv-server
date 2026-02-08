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
}

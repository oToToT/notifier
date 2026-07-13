use sha2::{Digest, Sha256};

/// Produce a stable hex digest over `kind` joined with NUL-byte separators to
/// each identifier in `ids` (matching TwitCasting's `livestart\0{id1}\0{id2}`
/// scheme). Use this to derive a stable dedupe key from payload fields when the
/// webhook provider does not supply a globally-unique message ID header.
pub fn dedupe_sha256(kind: &str, ids: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(kind.as_bytes());
    for id in ids {
        hasher.update(b"\0");
        hasher.update(id.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_format_concatenation_with_nul_separators() {
        let expected = format!(
            "{:x}",
            sha2::Sha256::digest(format!("livestart\0{}\0{}", "bid", "mid").as_bytes())
        );
        assert_eq!(dedupe_sha256("livestart", &["bid", "mid"]), expected);
    }

    #[test]
    fn is_deterministic() {
        assert_eq!(
            dedupe_sha256("livestart", &["bid", "mid"]),
            dedupe_sha256("livestart", &["bid", "mid"])
        );
    }

    #[test]
    fn kind_changes_digest() {
        assert_ne!(
            dedupe_sha256("livestart", &["bid", "mid"]),
            dedupe_sha256("liveend", &["bid", "mid"])
        );
    }
}

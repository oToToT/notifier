use anyhow::{Result, anyhow};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Verify a Twitch-style EventSub HMAC signature of the form
/// `sha256=<hex>` against the supplied secret and message parts (typically
/// `message_id`, `timestamp`, and the raw request body).
pub fn verify_hmac_sha256_parts(secret: &str, parts: &[&[u8]], signature: &str) -> Result<()> {
    let Some(hex) = signature.strip_prefix("sha256=") else {
        return Err(anyhow!("missing sha256= prefix"));
    };
    let expected = hex::decode(hex).map_err(|_| anyhow!("invalid signature hex"))?;
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    for part in parts {
        mac.update(part);
    }
    mac.verify_slice(&expected)
        .map_err(|_| anyhow!("HMAC signature mismatch"))
}

/// Compute the expected `sha256=<hex>` signature for the supplied parts. Useful
/// for diagnostics and tests. Never use this to compare signatures in place of
/// [`verify_hmac_sha256_parts`] — constant-time comparison should be preferred.
pub fn expected_hmac_sha256_parts(secret: &str, parts: &[&[u8]]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    for part in parts {
        mac.update(part);
    }
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &[u8] = b"id";
    const TIME: &[u8] = b"time";
    const BODY: &[u8] = b"body";
    const CHANGED: &[u8] = b"changed";

    #[test]
    fn verifies_valid_signature() {
        let secret = "0123456789";
        let parts = [ID, TIME, BODY];
        let signature = expected_hmac_sha256_parts(secret, &parts);
        assert!(verify_hmac_sha256_parts(secret, &parts, &signature).is_ok());
    }

    #[test]
    fn rejects_tampered_body() {
        let secret = "0123456789";
        let parts = [ID, TIME, BODY];
        let signature = expected_hmac_sha256_parts(secret, &parts);
        let bad = [ID, TIME, CHANGED];
        assert!(verify_hmac_sha256_parts(secret, &bad, &signature).is_err());
    }

    #[test]
    fn rejects_missing_prefix_or_bad_hex() {
        let secret = "0123456789";
        let parts = [ID];
        assert!(verify_hmac_sha256_parts(secret, &parts, "deadbeef").is_err());
        assert!(verify_hmac_sha256_parts(secret, &parts, "sha256=nothex").is_err());
    }
}

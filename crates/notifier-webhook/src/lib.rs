//! Common utilities for building notifier source plugins that receive webhooks.
//!
//! Plugins that need signature verification, broadcaster matching, or a unified
//! webhook dispatcher can implement [`WebhookProvider`] and wrap it in
//! [`WebhookSource`] to obtain a ready [`notifier_runtime::SourcePlugin`].
//! Plugins that only want the helpers (broadcasters, common spec, HMAC, dedupe)
//! can import those pieces directly and stay in control of their own router.

mod dedupe;
mod dispatcher;
mod hmac;
mod input;
mod spec;

pub use dedupe::dedupe_sha256;
pub use dispatcher::{
    WebhookError, WebhookOutcome, WebhookProvider, WebhookSource, WebhookState,
    single_webhook_router,
};
pub use hmac::{expected_hmac_sha256_parts, verify_hmac_sha256_parts};
pub use input::{
    BroadcasterInput, BroadcasterValidator, configured_broadcasters, matching_route_ids,
    parse_broadcaster_input, validate_broadcasters,
};
pub use spec::{CommonSpec, validate_common_spec};

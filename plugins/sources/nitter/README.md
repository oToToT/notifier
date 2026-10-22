# notifier-source-nitter

Polls Nitter RSS feeds and emits a `tweet` event for unseen items.

Source spec:

```json
{
  "instance_url": "https://nitter.net",
  "tweet_url_base": "https://fxtwitter.com",
  "first_fetch": "mark_seen",
  "poll_interval_seconds": 300,
  "request_timeout_seconds": 20,
  "retry_initial_seconds": 30,
  "retry_max_seconds": 1800
}
```

`instance_url` is always used for fetching `{instance_url}/{user}/rss`. `tweet_url_base` is
optional and rewrites only `tweet.url` when a status ID can be parsed. Common values are
`https://fxtwitter.com`, `https://vxtwitter.com`, and `https://x.com`.

Route input:

```json
{
  "users": ["example", "another_example"]
}
```

Template variables:

- `event`: `id`, `kind`, `published_at`
- `user`: `username`, `rss_url`, `profile_url`
- `tweet`: `id`, `title`, `description`, `url`, `published_at`

The default `first_fetch` mode is `mark_seen`, which records the current RSS items without
sending notifications. `notify_existing` sends the current feed contents on the first
successful fetch, then records them as seen. RSS `guid` values are used as item keys, falling
back to item links.

Each configured user has an independent polling and retry schedule. Failed fetches
use exponential backoff with jitter up to `retry_max_seconds`, including when other
users succeed. A successful fetch resets that user's retry delay and resumes the
normal polling interval. Requests use `request_timeout_seconds` (default `20`).

Failures produce one summary warning at most once per minute per source, with a
bounded sample error. Per-user failure details are available at debug level, and
recovery is logged at info level.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Clone)]
pub struct Storage {
    connection: Arc<Mutex<Connection>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub id: i64,
    pub route_id: String,
    pub message: String,
    pub attempts: u32,
}

impl Storage {
    pub fn open(path: &str) -> Result<Self> {
        let connection =
            Connection::open(path).with_context(|| format!("failed to open SQLite at {path}"))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "busy_timeout", 5_000)?;
        let storage = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        storage.migrate()?;
        Ok(storage)
    }

    fn migrate(&self) -> Result<()> {
        self.connection
            .lock()
            .expect("SQLite mutex poisoned")
            .execute_batch(
                "
            CREATE TABLE IF NOT EXISTS deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_plugin TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                route_id TEXT NOT NULL,
                message TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'queued',
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at INTEGER NOT NULL,
                last_error TEXT,
                created_at INTEGER NOT NULL,
                UNIQUE(source_plugin, dedupe_key, route_id)
            );
            CREATE INDEX IF NOT EXISTS deliveries_claim
                ON deliveries(state, available_at, id);
            CREATE TABLE IF NOT EXISTS source_baselines (
                source_id TEXT NOT NULL,
                scope TEXT NOT NULL,
                completed_at INTEGER NOT NULL,
                PRIMARY KEY(source_id, scope)
            );
            CREATE TABLE IF NOT EXISTS source_seen_items (
                source_id TEXT NOT NULL,
                scope TEXT NOT NULL,
                item_key TEXT NOT NULL,
                seen_at INTEGER NOT NULL,
                PRIMARY KEY(source_id, scope, item_key)
            );
            ",
            )?;
        Ok(())
    }

    pub fn enqueue(
        &self,
        source_plugin: &str,
        dedupe_key: &str,
        route_id: &str,
        message: &str,
    ) -> Result<bool> {
        let now = unix_seconds();
        let changed = self
            .connection
            .lock()
            .expect("SQLite mutex poisoned")
            .execute(
                "INSERT OR IGNORE INTO deliveries
             (source_plugin, dedupe_key, route_id, message, available_at, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
                params![source_plugin, dedupe_key, route_id, message, now, now],
            )?;
        Ok(changed == 1)
    }

    pub fn enqueue_batch(
        &self,
        source_plugin: &str,
        dedupe_key: &str,
        deliveries: &[(String, String)],
    ) -> Result<usize> {
        let now = unix_seconds();
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let mut inserted = 0;
        for (route_id, message) in deliveries {
            inserted += transaction.execute(
                "INSERT OR IGNORE INTO deliveries
                 (source_plugin, dedupe_key, route_id, message, available_at, created_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![source_plugin, dedupe_key, route_id, message, now, now],
            )?;
        }
        transaction.commit()?;
        Ok(inserted)
    }

    pub fn claim(&self) -> Result<Option<Delivery>> {
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let delivery = transaction
            .query_row(
                "SELECT id, route_id, message, attempts
                 FROM deliveries
                 WHERE state = 'queued' AND available_at <= ?
                 ORDER BY id LIMIT 1",
                [unix_seconds()],
                |row| {
                    Ok(Delivery {
                        id: row.get(0)?,
                        route_id: row.get(1)?,
                        message: row.get(2)?,
                        attempts: row.get(3)?,
                    })
                },
            )
            .optional()?;
        if let Some(delivery) = &delivery {
            transaction.execute(
                "UPDATE deliveries SET state = 'processing' WHERE id = ?",
                [delivery.id],
            )?;
        }
        transaction.commit()?;
        Ok(delivery)
    }

    pub fn complete(&self, id: i64) -> Result<()> {
        self.connection
            .lock()
            .expect("SQLite mutex poisoned")
            .execute(
                "UPDATE deliveries SET state = 'delivered', last_error = NULL WHERE id = ?",
                [id],
            )?;
        Ok(())
    }

    pub fn retry(&self, id: i64, attempts: u32, delay_seconds: u64, error: &str) -> Result<()> {
        self.connection
            .lock()
            .expect("SQLite mutex poisoned")
            .execute(
                "UPDATE deliveries
             SET state = 'queued', attempts = ?, available_at = ?, last_error = ?
             WHERE id = ?",
                params![
                    attempts,
                    unix_seconds().saturating_add(delay_seconds as i64),
                    error,
                    id
                ],
            )?;
        Ok(())
    }

    pub fn dead_letter(&self, id: i64, attempts: u32, error: &str) -> Result<()> {
        self.connection
            .lock()
            .expect("SQLite mutex poisoned")
            .execute(
                "UPDATE deliveries
             SET state = 'dead', attempts = ?, last_error = ?
             WHERE id = ?",
                params![attempts, error, id],
            )?;
        Ok(())
    }

    pub fn recover(&self, active_routes: &HashSet<String>) -> Result<()> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        connection.execute(
            "UPDATE deliveries SET state = 'queued' WHERE state = 'processing'",
            [],
        )?;
        let mut statement =
            connection.prepare("SELECT id, route_id FROM deliveries WHERE state = 'queued'")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (id, route_id) = row?;
            if !active_routes.contains(&route_id) {
                connection.execute(
                    "UPDATE deliveries SET state = 'dead', last_error = ? WHERE id = ?",
                    params!["route no longer exists", id],
                )?;
            }
        }
        Ok(())
    }

    pub fn source_baseline_completed(&self, source_id: &str, scope: &str) -> Result<bool> {
        let exists = self
            .connection
            .lock()
            .expect("SQLite mutex poisoned")
            .query_row(
                "SELECT 1 FROM source_baselines WHERE source_id = ? AND scope = ?",
                params![source_id, scope],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(exists)
    }

    pub fn complete_source_baseline(&self, source_id: &str, scope: &str) -> Result<()> {
        let now = unix_seconds();
        self.connection
            .lock()
            .expect("SQLite mutex poisoned")
            .execute(
                "INSERT OR IGNORE INTO source_baselines
                 (source_id, scope, completed_at)
                 VALUES (?, ?, ?)",
                params![source_id, scope, now],
            )?;
        Ok(())
    }

    pub fn mark_source_items_seen(
        &self,
        source_id: &str,
        scope: &str,
        item_keys: &[String],
    ) -> Result<usize> {
        let now = unix_seconds();
        let mut connection = self.connection.lock().expect("SQLite mutex poisoned");
        let transaction = connection.transaction()?;
        let mut inserted = 0;
        for item_key in item_keys {
            inserted += transaction.execute(
                "INSERT OR IGNORE INTO source_seen_items
                 (source_id, scope, item_key, seen_at)
                 VALUES (?, ?, ?, ?)",
                params![source_id, scope, item_key, now],
            )?;
        }
        transaction.commit()?;
        Ok(inserted)
    }

    pub fn unseen_source_items(
        &self,
        source_id: &str,
        scope: &str,
        item_keys: &[String],
    ) -> Result<Vec<String>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut unseen = Vec::new();
        for item_key in item_keys {
            let exists = connection
                .query_row(
                    "SELECT 1 FROM source_seen_items
                     WHERE source_id = ? AND scope = ? AND item_key = ?",
                    params![source_id, scope, item_key],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if !exists {
                unseen.push(item_key.clone());
            }
        }
        Ok(unseen)
    }

    #[cfg(test)]
    pub(crate) fn state(&self, id: i64) -> Result<String> {
        Ok(self
            .connection
            .lock()
            .expect("SQLite mutex poisoned")
            .query_row("SELECT state FROM deliveries WHERE id = ?", [id], |row| {
                row.get(0)
            })?)
    }
}

fn unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deduplicates_and_recovers_deliveries() {
        let storage = Storage::open(":memory:").unwrap();
        assert!(
            storage
                .enqueue("source", "event", "route", "hello")
                .unwrap()
        );
        assert!(
            !storage
                .enqueue("source", "event", "route", "hello")
                .unwrap()
        );
        let delivery = storage.claim().unwrap().unwrap();
        assert_eq!(delivery.message, "hello");
        storage.recover(&HashSet::from(["route".into()])).unwrap();
        assert_eq!(storage.claim().unwrap().unwrap().id, delivery.id);
    }

    #[test]
    fn removed_routes_become_dead_letters() {
        let storage = Storage::open(":memory:").unwrap();
        storage.enqueue("source", "event", "gone", "hello").unwrap();
        let delivery = storage.claim().unwrap().unwrap();
        storage.recover(&HashSet::new()).unwrap();
        assert_eq!(storage.state(delivery.id).unwrap(), "dead");
    }

    #[test]
    fn persists_source_baselines_and_seen_items() {
        let storage = Storage::open(":memory:").unwrap();
        assert!(!storage.source_baseline_completed("source", "user").unwrap());
        storage.complete_source_baseline("source", "user").unwrap();
        assert!(storage.source_baseline_completed("source", "user").unwrap());

        let keys = vec!["one".into(), "two".into()];
        assert_eq!(
            storage
                .unseen_source_items("source", "user", &keys)
                .unwrap(),
            keys
        );
        assert_eq!(
            storage
                .mark_source_items_seen("source", "user", &keys)
                .unwrap(),
            2
        );
        assert_eq!(
            storage
                .mark_source_items_seen("source", "user", &["one".into()])
                .unwrap(),
            0
        );
        assert!(
            storage
                .unseen_source_items("source", "user", &keys)
                .unwrap()
                .is_empty()
        );
    }
}

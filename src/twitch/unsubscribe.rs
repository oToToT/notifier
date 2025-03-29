use crate::db;

pub fn remove_from_db(pool: &db::Pool, id: &str) -> rusqlite::Result<()> {
    pool.get()
        .expect("Failed to get connection from pool")
        .execute(
            "DELETE FROM twitcasting WHERE id = ?",
            rusqlite::params![id],
        )?;
    Ok(())
}

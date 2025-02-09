use super::{private, Storage};
use crate::{begin_immediate::SqliteConnectionExt, Error, Options};
use serde::{de::DeserializeOwned, Serialize};
use std::str::FromStr;

#[derive(Debug)]
pub struct UpdateInPlace {
    pub(crate) pool: sqlx::sqlite::SqlitePool,
}

impl private::Sealed for UpdateInPlace {}

impl Storage for UpdateInPlace {
    async fn open(options: Options) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let db_path = if options.in_memory {
            "sqlite::memory:".to_string()
        } else if let Some(p) = options.db_path {
            p.to_str().unwrap().to_string()
        } else {
            "sqlite://kvqlite.db".to_string()
        };

        let pool_options = sqlx::sqlite::SqliteConnectOptions::from_str(&db_path)?
            .busy_timeout(std::time::Duration::from_secs(5))
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .create_if_missing(true);

        let pool = sqlx::SqlitePool::connect_with(pool_options).await?;

        let mut conn = pool.acquire().await?;

        let mut tx = conn.begin_immediate().await?;

        sqlx::query(
            "create table if not exists kvs (
            key blob not null primary key,
            value blob not null,
            inserted_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
            updated_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))
        )
        ",
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(Self { pool })
    }

    async fn read<K, V>(&self, key: &K) -> Result<Option<V>, Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: DeserializeOwned,
    {
        let mut conn = self.pool.acquire().await?;

        let value_bytes: Option<(Vec<u8>,)> = sqlx::query_as(
            "
        select
            value
        from kvs
        where key = ?;
        ",
        )
        .bind(key.as_ref())
        .fetch_optional(&mut *conn)
        .await?;

        if let Some((value_bytes,)) = value_bytes {
            Ok(ciborium::from_reader(&value_bytes[..])?)
        } else {
            Ok(None)
        }
    }

    async fn write<K, V>(&self, key: &K, value: &V) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: Serialize + ?Sized,
    {
        let mut conn = self.pool.acquire().await?;

        let mut value_bytes = vec![];
        ciborium::into_writer(value, &mut value_bytes)?;

        sqlx::query(
            "
            insert into kvs(key, value)
            values(?, ?)
            on conflict(key) do update set value = excluded.value;
        ",
        )
        .bind(key.as_ref())
        .bind(value_bytes)
        .execute(&mut *conn)
        .await?;

        Ok(())
    }

    async fn delete<K>(&self, key: &K) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
    {
        let mut conn = self.pool.acquire().await?;

        sqlx::query(
            "
        delete from kvs
        where key = ?
        ",
        )
        .bind(key.as_ref())
        .execute(&mut *conn)
        .await?;

        Ok(())
    }

    /// the distinct number of keys in the system
    async fn keys_count(&self) -> Result<u64, Error> {
        let mut conn = self.pool.acquire().await?;

        let (entries_count,): (u64,) = sqlx::query_as(
            "
            select count(*) from kvs
            ",
        )
        .fetch_one(&mut *conn)
        .await?;

        Ok(entries_count)
    }

    /// all distinct keys in the system
    async fn keys(&self) -> Result<Vec<Vec<u8>>, Error> {
        let mut conn = self.pool.acquire().await?;

        let keys: Vec<Vec<u8>> = sqlx::query_as(
            "
            select
                key
            from kvs
            ",
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|(key,)| key)
        .collect();

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Db;

    #[tokio::test]
    async fn roundtrip() {
        let db: Db<UpdateInPlace> = Db::builder().in_memory().finish().await.unwrap();

        db.write("hello", "world").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();

        assert_eq!(value, "world");
    }

    #[tokio::test]
    async fn updates() {
        let db: Db<UpdateInPlace> = Db::builder().in_memory().finish().await.unwrap();

        db.write("hello", "world").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "world");

        db.write("hello", "joe").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "joe");

        let count = db.keys_count().await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn deletes() {
        let db: Db<UpdateInPlace> = Db::builder().in_memory().finish().await.unwrap();

        db.write("hello", "world").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "world");

        db.write("hello", "joe").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "joe");

        let count = db.keys_count().await.unwrap();
        assert_eq!(count, 1);

        db.delete("hello").await.unwrap();
        let count = db.keys_count().await.unwrap();

        assert_eq!(count, 0);

        let keys = db.keys().await.unwrap();
        assert!(keys.is_empty());

        let value = db.read::<str, String>("hello").await.unwrap();
        assert!(value.is_none())
    }
}

use super::{private, Storage};
use crate::{begin_immediate::SqliteConnectionExt, Error, Options};
use serde::{de::DeserializeOwned, Serialize};
use std::str::FromStr;

#[derive(Debug)]
pub struct Append {
    pub(crate) pool: sqlx::sqlite::SqlitePool,
}

impl private::Sealed for Append {}

impl Storage for Append {
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
            "create table if not exists keys (
            id integer primary key,
            key blob not null,
            inserted_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))
        )
        ",
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "
            create table if not exists vvalues (
                id integer primary key,
                key_id integer not null,
                value blob not null,
                inserted_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),

                foreign key(key_id) references keys(id) on delete cascade
            )",
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "
            create unique index if not exists keys_key on keys (key);
        ",
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "
            create index if not exists vvalues_inserted_at on vvalues (inserted_at);
        ",
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "
            create index if not exists vvalues_key_id on vvalues (key_id);
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
            vvalues.value
        from keys
        inner join vvalues
            on vvalues.key_id = keys.id
        where key = ?
        order by vvalues.inserted_at desc
        limit 1
        ",
        )
        .bind(key.as_ref())
        .fetch_optional(&mut *conn)
        .await?;

        if let Some((value_bytes,)) = value_bytes {
            let value: V = ciborium::from_reader(&value_bytes[..])?;
            Ok(Some(value))
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

        let mut tx = conn.begin_immediate().await?;

        let mut value_bytes = vec![];
        ciborium::into_writer(value, &mut value_bytes)?;

        let (key_id,): (i64,) = sqlx::query_as(
            "
        insert into keys (key) values(?)
        on conflict do update set key=excluded.key
        returning id;
        ",
        )
        .bind(key.as_ref())
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            "insert into vvalues (key_id, value) values(?, ?);
        ",
        )
        .bind(key_id)
        .bind(value_bytes)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn delete<K>(&self, key: &K) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
    {
        let mut conn = self.pool.acquire().await?;

        sqlx::query(
            "
        delete from keys
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
            select count(*) from keys
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
            select key from keys
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
        let db: Db<Append> = Db::builder().in_memory().finish().await.unwrap();

        db.write("hello", "world").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();

        assert_eq!(value, "world");

        let entries_count = db.entries_count().await.unwrap();
        assert_eq!(entries_count, 1);
    }

    #[tokio::test]
    async fn updates() {
        let db: Db<Append> = Db::builder().in_memory().finish().await.unwrap();

        db.write("hello", "world").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "world");

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        db.write("hello", "joe").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "joe");

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        db.write("hello", "mike").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "mike");

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        db.write("hello", "robert").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "robert");

        let count = db.entries_count().await.unwrap();
        assert_eq!(count, 4);

        let keys_count = db.keys_count().await.unwrap();
        assert_eq!(keys_count, 1);
    }

    #[tokio::test]
    async fn collect_garbage_old_values() {
        let db: Db<Append> = Db::builder().in_memory().finish().await.unwrap();

        db.write("hello", "world").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "world");

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        db.write("hello", "joe").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "joe");

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        db.write("hello", "mike").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "mike");

        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        db.write("hello", "robert").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();
        assert_eq!(value, "robert");

        let entries_count = db.entries_count().await.unwrap();

        assert_eq!(entries_count, 4);

        db.collect_garbage().await.unwrap();

        let keys_count = db.keys_count().await.unwrap();
        assert_eq!(keys_count, 1);

        let value: String = db.read("hello").await.unwrap().unwrap();

        assert_eq!(value, "robert");

        let count = db.entries_count().await.unwrap();

        assert_eq!(count, 1);

        let keys_count = db.keys_count().await.unwrap();
        assert_eq!(keys_count, 1);

        let keys = db.keys().await.unwrap();
        assert_eq!(keys, vec![b"hello"]);
    }

    #[tokio::test]
    async fn deletes() {
        let db: Db<Append> = Db::builder().in_memory().finish().await.unwrap();

        db.write("a", "b").await.unwrap();

        db.write("hello", "world").await.unwrap();
        let value: String = db.read("hello").await.unwrap().unwrap();

        assert_eq!(value, "world");

        let keys_count = db.keys_count().await.unwrap();
        assert_eq!(keys_count, 2);

        let entries_count = db.entries_count().await.unwrap();
        assert_eq!(entries_count, 2);

        db.delete("hello").await.unwrap();

        assert!(db.read::<str, String>("hello").await.unwrap().is_none());

        let keys_count = db.keys_count().await.unwrap();
        assert_eq!(keys_count, 1);

        let entries_count = db.entries_count().await.unwrap();
        assert_eq!(entries_count, 1);

        let keys = db.keys().await.unwrap();
        assert_eq!(keys, vec![b"a"]);
    }
}

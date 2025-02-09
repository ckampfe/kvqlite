use super::{private, Storage};
use crate::{Error, Options};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::OptionalExtension;
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug)]
pub struct UpdateInPlace {
    pub(crate) pool: r2d2::Pool<SqliteConnectionManager>,
}

impl private::Sealed for UpdateInPlace {}

impl Storage for UpdateInPlace {
    fn open(options: Options) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let db_path = if let Some(p) = options.db_path {
            p.to_str().unwrap().to_string()
        } else {
            "sqlite://kvqlite.db".to_string()
        };

        let manager = if options.in_memory {
            SqliteConnectionManager::memory()
        } else {
            SqliteConnectionManager::file(db_path)
        };
        let pool = r2d2::Pool::new(manager)?;

        let mut conn = pool.get()?;

        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        tx.execute(
            "create table if not exists kvs (
            key blob not null primary key,
            value blob not null,
            inserted_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
            updated_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))
        )
        ",
            [],
        )?;

        tx.commit()?;

        Ok(Self { pool })
    }

    fn read<K, V>(&self, key: &K) -> Result<Option<V>, Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: DeserializeOwned,
    {
        let conn = self.pool.get()?;

        let mut statement = conn.prepare(
            "
        select
            value
        from kvs
        where key = ?
        ",
        )?;

        let value_bytes: Option<Vec<u8>> = statement
            .query_row([key.as_ref()], |row| row.get(0))
            .optional()?;

        if let Some(value_bytes) = value_bytes {
            Ok(ciborium::from_reader(&value_bytes[..])?)
        } else {
            Ok(None)
        }
    }

    fn write<K, V>(&self, key: &K, value: &V) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: Serialize + ?Sized,
    {
        let conn = self.pool.get()?;

        let mut value_bytes = vec![];
        ciborium::into_writer(value, &mut value_bytes)?;

        conn.execute(
            "
        insert into kvs(key, value)
        values(?, ?)
        on conflict(key) do update set value = excluded.value;
        ",
            [key.as_ref(), &value_bytes],
        )?;

        Ok(())
    }

    fn delete<K>(&self, key: &K) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
    {
        let conn = self.pool.get()?;

        conn.execute(
            "
        delete from kvs
        where key = ?
        ",
            [key.as_ref()],
        )?;

        Ok(())
    }

    /// the distinct number of keys in the system
    fn keys_count(&self) -> Result<i64, Error> {
        let conn = self.pool.get()?;

        let mut statement = conn.prepare(
            "
            select count(*) from kvs
            ",
        )?;

        let entries_count: i64 = statement.query_row([], |row| row.get(0))?;

        Ok(entries_count)
    }

    /// all distinct keys in the system
    fn keys(&self) -> Result<Vec<Vec<u8>>, Error> {
        let conn = self.pool.get()?;

        let mut statement = conn.prepare(
            "
            select
                key
            from kvs
            ",
        )?;

        let mut keys = vec![];
        let rows = statement.query_map([], |row| row.get(0))?;

        for row in rows {
            keys.push(row?)
        }

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Db;

    #[test]
    fn roundtrip() {
        let db: Db<UpdateInPlace> = Db::builder().in_memory().finish().unwrap();

        db.write("hello", "world").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();

        assert_eq!(value, "world");
    }

    #[test]
    fn updates() {
        let db: Db<UpdateInPlace> = Db::builder().in_memory().finish().unwrap();

        db.write("hello", "world").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "world");

        db.write("hello", "joe").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "joe");

        let count = db.keys_count().unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn deletes() {
        let db: Db<UpdateInPlace> = Db::builder().in_memory().finish().unwrap();

        db.write("hello", "world").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "world");

        db.write("hello", "joe").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "joe");

        let count = db.keys_count().unwrap();
        assert_eq!(count, 1);

        db.delete("hello").unwrap();
        let count = db.keys_count().unwrap();

        assert_eq!(count, 0);

        let keys = db.keys().unwrap();
        assert!(keys.is_empty());

        let value = db.read::<str, String>("hello").unwrap();
        assert!(value.is_none())
    }
}

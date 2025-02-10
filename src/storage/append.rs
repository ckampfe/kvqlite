use super::{private, Storage};
use crate::{Error, Options};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OptionalExtension};
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug)]
pub struct Append {
    pub(crate) pool: r2d2::Pool<SqliteConnectionManager>,
}

impl private::Sealed for Append {}

impl Storage for Append {
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

        conn.pragma_update(None, "FOREIGN_KEYS", "ON")?;

        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        tx.execute(
            "create table if not exists keys (
            id integer primary key,
            key blob not null,
            inserted_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))
        )
        ",
            [],
        )?;

        tx.execute(
            "
            create table if not exists vvalues (
                id integer primary key,
                key_id integer not null,
                value blob not null,
                inserted_at datetime not null default(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),

                foreign key(key_id) references keys(id) on delete cascade
            )",
            [],
        )?;

        tx.execute(
            "
            create unique index if not exists keys_key on keys (key);
        ",
            [],
        )?;

        tx.execute(
            "
            create index if not exists vvalues_inserted_at on vvalues (inserted_at);
        ",
            [],
        )?;

        tx.execute(
            "
                create index if not exists vvalues_key_id on vvalues (key_id);
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
            vvalues.value
        from keys
        inner join vvalues
            on vvalues.key_id = keys.id
        where key = ?
        order by vvalues.inserted_at desc
        limit 1
        ",
        )?;

        let value_bytes: Option<Vec<u8>> = statement
            .query_row([key.as_ref()], |row| row.get(0))
            .optional()?;

        if let Some(value_bytes) = value_bytes {
            let value: V = ciborium::from_reader(&value_bytes[..])?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn write<K, V>(&self, key: &K, value: &V) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: Serialize + ?Sized,
    {
        let mut conn = self.pool.get()?;

        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        let mut value_bytes = vec![];
        ciborium::into_writer(value, &mut value_bytes)?;

        let key_id = {
            let mut statement = tx.prepare(
                "
        insert into keys (key) values(?)
        on conflict do update set key=excluded.key
        returning id;
        ",
            )?;

            let key_id: i64 = statement.query_row([key.as_ref()], |row| row.get(0))?;
            key_id
        };

        tx.execute(
            "insert into vvalues (key_id, value) values(?, ?)",
            params![key_id, value_bytes],
        )?;

        tx.commit()?;

        Ok(())
    }

    fn delete<K>(&self, key: &K) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
    {
        let conn = self.pool.get()?;

        conn.execute(
            "
        delete from keys
        where key = ?",
            [key.as_ref()],
        )?;

        Ok(())
    }

    fn keys_count(&self) -> Result<i64, Error> {
        let conn = self.pool.get()?;

        let mut statement = conn.prepare(
            "
        select count(*) from keys
        ",
        )?;

        let entries_count: i64 = statement.query_row([], |row| row.get(0))?;

        Ok(entries_count)
    }

    fn keys(&self) -> Result<Vec<Vec<u8>>, Error> {
        let conn = self.pool.get()?;

        let mut statement = conn.prepare(
            "
        select key from keys
        ",
        )?;

        let rows = statement.query_map([], |row| row.get(0))?;

        let mut keys = vec![];

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
        let db: Db<Append> = Db::builder().in_memory().finish().unwrap();

        db.write("hello", "world").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();

        assert_eq!(value, "world");

        let entries_count = db.entries_count().unwrap();
        assert_eq!(entries_count, 1);
    }

    #[test]
    fn updates() {
        let db: Db<Append> = Db::builder().in_memory().finish().unwrap();

        db.write("hello", "world").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "world");

        std::thread::sleep(std::time::Duration::from_millis(1));

        db.write("hello", "joe").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "joe");

        std::thread::sleep(std::time::Duration::from_millis(1));

        db.write("hello", "mike").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "mike");

        std::thread::sleep(std::time::Duration::from_millis(1));

        db.write("hello", "robert").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "robert");

        std::thread::sleep(std::time::Duration::from_millis(1));

        let count = db.entries_count().unwrap();
        assert_eq!(count, 4);

        let keys_count = db.keys_count().unwrap();
        assert_eq!(keys_count, 1);
    }

    #[test]
    fn collect_garbage_old_values() {
        let db: Db<Append> = Db::builder().in_memory().finish().unwrap();

        db.write("hello", "world").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "world");

        std::thread::sleep(std::time::Duration::from_millis(1));

        db.write("hello", "joe").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "joe");

        std::thread::sleep(std::time::Duration::from_millis(1));

        db.write("hello", "mike").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "mike");

        std::thread::sleep(std::time::Duration::from_millis(1));

        db.write("hello", "robert").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();
        assert_eq!(value, "robert");

        std::thread::sleep(std::time::Duration::from_millis(1));

        let entries_count = db.entries_count().unwrap();

        assert_eq!(entries_count, 4);

        db.collect_garbage().unwrap();

        let keys_count = db.keys_count().unwrap();
        assert_eq!(keys_count, 1);

        let value: String = db.read("hello").unwrap().unwrap();

        assert_eq!(value, "robert");

        let count = db.entries_count().unwrap();

        assert_eq!(count, 1);

        let keys_count = db.keys_count().unwrap();
        assert_eq!(keys_count, 1);

        let keys = db.keys().unwrap();
        assert_eq!(keys, vec![b"hello"]);
    }

    #[test]
    fn deletes() {
        let db: Db<Append> = Db::builder().in_memory().finish().unwrap();

        db.write("a", "b").unwrap();

        db.write("hello", "world").unwrap();
        let value: String = db.read("hello").unwrap().unwrap();

        assert_eq!(value, "world");

        let keys_count = db.keys_count().unwrap();
        assert_eq!(keys_count, 2);

        let entries_count = db.entries_count().unwrap();
        assert_eq!(entries_count, 2);

        db.delete("hello").unwrap();

        assert!(db.read::<str, String>("hello").unwrap().is_none());

        let keys_count = db.keys_count().unwrap();
        assert_eq!(keys_count, 1);

        let entries_count = db.entries_count().unwrap();
        assert_eq!(entries_count, 1);

        let keys = db.keys().unwrap();
        assert_eq!(keys, vec![b"a"]);
    }
}

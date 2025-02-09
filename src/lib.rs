use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use storage::append::Append;
use storage::update_in_place::UpdateInPlace;
use storage::Storage;
use thiserror::Error;

mod storage;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error with sqlite")]
    R2d2Error(#[from] r2d2::Error),
    #[error("ldfkj")]
    SqliteError(#[from] r2d2_sqlite::rusqlite::Error),
    #[error("could not serialize")]
    Serialization(#[from] ciborium::ser::Error<std::io::Error>),
    #[error("could not deserialize")]
    Deserialization(#[from] ciborium::de::Error<std::io::Error>),
}

#[derive(Clone, Debug)]
pub struct Db<T>
where
    T: Storage,
{
    storage: T,
}

impl<T> Db<T>
where
    T: Storage,
{
    /// create a new database or open an existing one with default configuration
    pub fn new() -> Result<Db<UpdateInPlace>, Error> {
        Db::<UpdateInPlace>::builder().finish()
    }

    /// builder for configuration
    pub fn builder() -> Builder<T> {
        Builder {
            storage: PhantomData,
            options: Options::default(),
        }
    }

    /// write a key/value
    pub fn write<K, V>(&self, key: &K, value: &V) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: Serialize + ?Sized,
    {
        self.storage.write(key, value)
    }

    /// read a value
    pub fn read<K, V>(&self, key: &K) -> Result<Option<V>, Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: DeserializeOwned,
    {
        self.storage.read(key)
    }

    /// delete a key/value
    pub fn delete<K>(&self, key: &K) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
    {
        self.storage.delete(key)
    }

    /// get the current keys
    pub fn keys(&self) -> Result<Vec<Vec<u8>>, Error> {
        self.storage.keys()
    }

    /// get the current number of keys
    pub fn keys_count(&self) -> Result<i64, Error> {
        self.storage.keys_count()
    }
}

impl Db<Append> {
    /// keep only the latest entry for each key,
    /// deleting values that are not the latest value
    pub fn collect_garbage(&self) -> Result<(), Error> {
        let conn = self.storage.pool.get()?;

        conn.execute(
            "
            with current_values as (
                select
                    id,
                    max(inserted_at)
                from vvalues
                group by key_id
            )
            delete from vvalues
            where id not in (
                select
                    id
                from current_values
            )
        ",
            [],
        )?;

        Ok(())
    }

    /// the total number of entries, including duplicates and deletes
    pub fn entries_count(&self) -> Result<i64, Error> {
        let conn = self.storage.pool.get()?;

        let mut statement = conn.prepare(
            "
            select
                count(*)
            from vvalues
            ",
        )?;

        let entries_count: i64 = statement.query_row([], |row| row.get(0))?;

        Ok(entries_count)
    }

    // TODO
    // pub fn read_as_of()
    // pub fn read_range()
}

pub struct Builder<T> {
    options: Options,
    storage: PhantomData<T>,
}

impl<T> Builder<T> {
    pub fn finish(self) -> Result<Db<T>, Error>
    where
        T: Storage,
    {
        let storage = T::open(self.options)?;
        Ok(Db { storage })
    }

    pub fn in_memory(mut self) -> Self {
        self.options.in_memory = true;
        self
    }

    pub fn with_db_path(mut self, path: &Path) -> Self {
        self.options.db_path = Some(path.to_path_buf());
        self
    }
}

#[derive(Default)]
pub struct Options {
    in_memory: bool,
    db_path: Option<PathBuf>,
}

// #[cfg(test)]
// mod tests {
//     use super::*;
// }

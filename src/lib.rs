use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use storage::append::Append;
use storage::update_in_place::UpdateInPlace;
use storage::Storage;
use thiserror::Error;

mod begin_immediate;
mod storage;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error with sqlx")]
    SqlxError(#[from] sqlx::Error),
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
    pub async fn new() -> Result<Db<UpdateInPlace>, Error> {
        Db::<UpdateInPlace>::builder().finish().await
    }

    /// builder for configuration
    pub fn builder() -> Builder<T> {
        Builder {
            storage: PhantomData,
            options: Options::default(),
        }
    }

    /// write a key/value
    pub async fn write<K, V>(&self, key: &K, value: &V) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: Serialize + ?Sized,
    {
        self.storage.write(key, value).await
    }

    /// read a value
    pub async fn read<K, V>(&self, key: &K) -> Result<Option<V>, Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: DeserializeOwned,
    {
        self.storage.read(key).await
    }

    /// delete a key/value
    pub async fn delete<K>(&self, key: &K) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
    {
        self.storage.delete(key).await
    }

    /// get the current keys
    pub async fn keys(&self) -> Result<Vec<Vec<u8>>, Error> {
        self.storage.keys().await
    }

    /// get the current number of keys
    pub async fn keys_count(&self) -> Result<u64, Error> {
        self.storage.keys_count().await
    }
}

impl Db<Append> {
    /// keep only the latest entry for each key,
    /// deleting values that are not the latest value
    pub async fn collect_garbage(&self) -> Result<(), Error> {
        let mut conn = self.storage.pool.acquire().await?;

        sqlx::query(
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
        )
        .execute(&mut *conn)
        .await?;

        Ok(())
    }

    /// the total number of entries, including duplicates and deletes
    pub async fn entries_count(&self) -> Result<u64, Error> {
        let mut conn = self.storage.pool.acquire().await?;

        let (entries_count,): (u64,) = sqlx::query_as(
            "
            select
                count(*)
            from vvalues
            ",
        )
        .fetch_one(&mut *conn)
        .await?;

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
    pub async fn finish(self) -> Result<Db<T>, Error>
    where
        T: Storage,
    {
        let storage = T::open(self.options).await?;
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

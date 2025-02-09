use crate::{Error, Options};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub mod append;
pub mod update_in_place;

pub(crate) mod private {
    pub trait Sealed {}
}

pub trait Storage: private::Sealed {
    fn open(options: Options) -> Result<Self, Error>
    where
        Self: Sized;

    fn read<K, V>(&self, key: &K) -> Result<Option<V>, Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: DeserializeOwned;

    fn write<K, V>(&self, key: &K, value: &V) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: Serialize + ?Sized;

    fn delete<K>(&self, key: &K) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized;

    fn keys(&self) -> Result<Vec<Vec<u8>>, Error>;

    fn keys_count(&self) -> Result<i64, Error>;
}

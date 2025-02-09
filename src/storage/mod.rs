use crate::{Error, Options};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub mod append;
pub mod update_in_place;

pub(crate) mod private {
    pub trait Sealed {}
}

pub trait Storage: private::Sealed {
    #[allow(async_fn_in_trait)]
    async fn open(options: Options) -> Result<Self, Error>
    where
        Self: Sized;

    #[allow(async_fn_in_trait)]
    async fn read<K, V>(&self, key: &K) -> Result<Option<V>, Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: DeserializeOwned;

    #[allow(async_fn_in_trait)]
    async fn write<K, V>(&self, key: &K, value: &V) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized,
        V: Serialize + ?Sized;

    #[allow(async_fn_in_trait)]
    async fn delete<K>(&self, key: &K) -> Result<(), Error>
    where
        K: AsRef<[u8]> + ?Sized;

    #[allow(async_fn_in_trait)]
    async fn keys(&self) -> Result<Vec<Vec<u8>>, Error>;

    #[allow(async_fn_in_trait)]
    async fn keys_count(&self) -> Result<u64, Error>;
}

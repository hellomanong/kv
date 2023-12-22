use crate::*;
use prost::Message;
use sled::{Db, Error, IVec};
use std::path::Path;
use std::str;
use tracing::info;

#[derive(Debug)]
pub struct SledDb(Db);

impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }

    fn get_full_key(table: &str, key: &str) -> String {
        format!("{table}:{key}")
    }

    fn get_table_prefix(table: &str) -> String {
        format!("{table}")
    }
}

fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some))
}

impl Storage for SledDb {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = Self::get_full_key(table, key);
        let result = self.0.get(name.as_bytes())?.map(|v| v.as_ref().try_into());
        flip(result)
    }

    fn set(&self, table: &str, key: &str, v: Value) -> Result<Option<Value>, KvError> {
        let name = Self::get_full_key(table, key);
        let buf = Vec::try_from(v)?;
        let result = self.0.insert(name, buf)?.map(|v| v.as_ref().try_into());
        flip(result)
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let name = Self::get_full_key(table, key);
        Ok(self.0.contains_key(name)?)
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = Self::get_full_key(table, key);
        let result = self.0.remove(name)?.map(|v| v.as_ref().try_into());
        flip(result)
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let prefix = Self::get_table_prefix(table);
        let result = self.0.scan_prefix(prefix).map(|v| v.into()).collect();
        Ok(result)
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let prefix = Self::get_table_prefix(table);
        let iter = self.0.scan_prefix(prefix);
        Ok(Box::new(StorageIter::new(iter)))
    }
}

impl From<Result<(IVec, IVec), sled::Error>> for Kvpair {
    fn from(value: Result<(IVec, IVec), Error>) -> Self {
        match value {
            Ok((k, v)) => match v.as_ref().try_into() {
                Ok(v) => Kvpair::new(ivec_to_key(k.as_ref()), v),
                Err(_) => Kvpair::default(),
            },
            Err(e) => Kvpair::default(),
        }
    }
}

fn ivec_to_key(ivec: &[u8]) -> &str {
    let s = str::from_utf8(ivec).unwrap();
    let mut iter = s.split(":");
    iter.next();
    iter.next().unwrap()
}

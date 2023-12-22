mod memory;
mod sleddb;

pub use memory::MemTable;
pub use sleddb::SledDb;
use crate::{KvError, Kvpair, Value};
use anyhow::Result;

pub trait Storage {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    fn set(&self, table: &str, key: &str, value: Value) -> Result<Option<Value>, KvError>;
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError>;
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item=Kvpair>>, KvError>;
}

pub struct StorageIter<T> {
    data: T,
}

impl<T> StorageIter<T> {
    pub fn new(data: T) -> Self {
        Self {
            data
        }
    }
}

impl<T> Iterator for StorageIter<T>
    where
        T: Iterator,
        T::Item: Into<Kvpair>,
{
    type Item = Kvpair;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().map(|v| v.into())
    }
}

impl From<(String, Value)> for Kvpair {
    fn from(value: (String, Value)) -> Self {
        Kvpair::new(value.0, value.1)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::memory::MemTable;
    use crate::storage::sleddb::SledDb;
    use tempfile::tempdir;
    use super::*;

    #[test]
    fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basic_interface(store);
    }

    #[test]
    fn memtable_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(store);
    }

    #[test]
    fn sleddb_basic_interface_should_work() {
        let dir = tempdir().unwrap();
        let db = SledDb::new(dir);
        test_basic_interface(db);
    }

    #[test]
    fn sleddb_get_all_should_world() {
        let dir = tempdir().unwrap();
        let db = SledDb::new(dir);
        test_get_all(db);
    }

    fn test_basic_interface<T: Storage>(store: T) {
        let v = store.set("t1", "hello".into(), "world".into());
        assert!(v.unwrap().is_none());

        let v1 = store.set("t1", "hello".into(), 10.into());
        assert_eq!(v1.unwrap(), Some("world".into()));

        let v = store.get("t1", "hello");
        assert_eq!(v.unwrap(), Some(10.into()));

        assert_eq!(None, store.get("t1", "hello1").unwrap());
        assert!(store.get("t2", "hello1").unwrap().is_none());

        assert_eq!(store.contains("t1", "hello").unwrap(), true);
        assert_eq!(store.contains("t1", "hello1").unwrap(), false);
        assert_eq!(store.contains("t2", "hello").unwrap(), false);

        let v = store.del("t1", "hello");
        assert_eq!(v.unwrap(), Some(10.into()));

        assert_eq!(None, store.del("t1", "hello1").unwrap());
        assert_eq!(None, store.del("t2", "hello").unwrap());
        assert_eq!(None, store.del("t1", "hello").unwrap());
    }

    fn test_get_all(store: impl Storage) {
        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();

        let mut data = store.get_all("t2").unwrap();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into()),
            ])
    }

    fn test_get_iter(store: impl Storage) {
        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();
        let mut data = store.get_iter("t2").unwrap().collect::<Vec<_>>();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into()),
            ],
        )
    }
}
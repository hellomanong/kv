use crate::{CommandResponse, CommandService, Hget, Hset, KvError, Storage, Value};
use std::borrow::Borrow;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let value = store.get(self.table.borrow(), self.key.borrow());
        match value {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into()
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, &v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into()
            },
            None => Value::default().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandRequest, MemTable};
    use crate::service::{assert_res_ok, dispatch};

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(&res, &[Value::default()], &[]);
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", 10_i64.into());
        dispatch(cmd, &store);
        let cmd = CommandRequest::new_hget("t1", "hello");
        let res = dispatch(cmd, &store);
        assert_res_ok(&res, &[10.into()], &[]);
    }
}
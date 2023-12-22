use crate::*;
use axum::http::header::REFERER;
use futures::stream;
use std::sync::Arc;
use tracing::debug;
use tracing::field::debug;

mod command_service;
mod topic;
mod topic_service;

pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

pub trait Notify<Arg> {
    fn notify(&self, arg: &Arg);
}

pub trait NotifyMut<Arg> {
    fn notify(&self, arg: &mut Arg);
}

pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
    broadcaster: Arc<Broadcaster>,
}

impl<Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            broadcaster: Arc::clone(&self.broadcaster),
        }
    }
}

pub struct ServiceInner<Store> {
    store: Store,
    on_received: Vec<fn(&CommandRequest)>,
    on_executed: Vec<fn(&CommandResponse)>,
    on_before_send: Vec<fn(&mut CommandResponse)>,
    on_after_send: Vec<fn()>,
}

impl<Store: Storage> Service<Store> {
    pub fn execute(&self, cmd: CommandRequest) -> StreamingResponse {
        debug!("Got request: {:?}", cmd);
        self.inner.on_received.notify(&cmd);
        let mut res = dispatch(cmd.clone(), &self.inner.store);

        if res == CommandResponse::default() {
            dispatch_stream(cmd, Arc::clone(&self.broadcaster))
        } else {
            debug!("Excuted response: {:?}", res);
            self.inner.on_executed.notify(&res);
            self.inner.on_before_send.notify(&mut res);
            if !self.inner.on_before_send.is_empty() {
                debug!("Modified response:{res:?}");
            }

            Box::pin(stream::once(async move { Arc::new(res) }))
        }
    }
}

impl<Store: Storage> ServiceInner<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            on_received: vec![],
            on_executed: vec![],
            on_before_send: vec![],
            on_after_send: vec![],
        }
    }

    pub fn fn_received(mut self, f: fn(&CommandRequest)) -> Self {
        self.on_received.push(f);
        self
    }

    pub fn fn_executed(mut self, f: fn(&CommandResponse)) -> Self {
        self.on_executed.push(f);
        self
    }
    pub fn fn_before_send(mut self, f: fn(&mut CommandResponse)) -> Self {
        self.on_before_send.push(f);
        self
    }
    pub fn fn_after_send(mut self, f: fn()) -> Self {
        self.on_after_send.push(f);
        self
    }
}

fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(v)) => v.execute(store),
        Some(RequestData::Hset(v)) => v.execute(store),
        None => KvError::InvalidCommand("Requst has no data".into()).into(),
        _ => CommandResponse::default(),
    }
}

fn dispatch_stream(cmd: CommandRequest, topic: impl Topic) -> StreamingResponse {
    match cmd.request_data {
        Some(RequestData::Subscribe(v)) => v.execute(topic),
        Some(RequestData::Unsubscribe(v)) => v.execute(topic),
        Some(RequestData::Publish(v)) => v.execute(topic),
        _ => unreachable!(),
    }
}

impl<store: Storage> From<ServiceInner<store>> for Service<store> {
    fn from(value: ServiceInner<store>) -> Self {
        Self {
            inner: Arc::new(value),
            broadcaster: Arc::new(Broadcaster::default()),
        }
    }
}

impl<Arg> Notify<Arg> for Vec<fn(&Arg)> {
    fn notify(&self, arg: &Arg) {
        for f in self {
            f(arg);
        }
    }
}

impl<Arg> NotifyMut<Arg> for Vec<fn(&mut Arg)> {
    fn notify(&self, arg: &mut Arg) {
        for f in self {
            f(arg)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemTable, Value};
    use axum::http::StatusCode;
    use futures::StreamExt;
    use log::info;
    use std::thread;

    #[tokio::test]
    async fn service_should_works() {
        let service: Service = ServiceInner::new(MemTable::new()).into();
        let cloned = service.clone();

        let handle = tokio::spawn(async move {
            let mut res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            let data = res.next().await.unwrap();
            assert_res_ok(&data, &[Value::default()], &[]);
        });
        handle.await.unwrap();

        let mut res = service.execute(CommandRequest::new_hget("t1", "k1"));
        let data = res.next().await.unwrap();
        assert_res_ok(&data, &["v1".into()], &[]);
    }

    #[tokio::test]
    async fn event_registration_should_work() {
        fn b(cmd: &CommandRequest) {
            info!("Got {cmd:?}");
        }

        fn c(res: &CommandResponse) {
            info!("{res:?}");
        }

        fn d(res: &mut CommandResponse) {
            res.status = StatusCode::CREATED.as_u16() as u32;
        }

        fn e() {
            info!("Data is sent");
        }

        let service: Service = ServiceInner::new(MemTable::new())
            .fn_received(|_: &CommandRequest| {})
            .fn_received(b)
            .fn_executed(c)
            .fn_before_send(d)
            .fn_after_send(e)
            .into();

        let mut res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        let data = res.next().await.unwrap();
        assert_eq!(data.status, StatusCode::CREATED.as_u16() as u32);
        assert_eq!(data.message, "");
        assert_eq!(data.values, vec![Value::default()]);
    }
}

use crate::command_request::RequestData;
#[cfg(test)]
use crate::{Kvpair, Value};

use self::{
    topic::{Broadcaster, Topic},
    topic_service::{StreamingResponse, TopicService},
};

#[cfg(test)]
pub fn assert_res_ok(res: &CommandResponse, p1: &[Value], p2: &[Kvpair]) {
    let mut res = res.clone();
    res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.pairs, p2);
    assert_eq!(res.values, p1);
}

#[cfg(test)]
pub fn assert_res_error(mut res: CommandResponse, values: &[Value], pair: &[Kvpair]) {}

use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use dashmap::{DashMap, DashSet};
use log::{warn, debug, info};
use prost_types::value;
use tokio::sync::mpsc;

use crate::{CommandResponse, Value};

const BROADCAST_CAPACITY: usize = 128;
static NEXT_ID: AtomicU32 = AtomicU32::new(1);

fn get_next_subscription_id() -> u32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

pub trait Topic: Send + Sync {
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>>;
    fn unsubscribe(self, name: String, id: u32);
    fn publish(self, name: String, value: Arc<CommandResponse>);
}

#[derive(Default)]
pub struct Broadcaster {
    topics: DashMap<String, DashSet<u32>>,
    subscriptions: DashMap<u32, mpsc::Sender<Arc<CommandResponse>>>,
}

impl Topic for Arc<Broadcaster> {
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>> {
        let id = {
            let entry = self.topics.entry(name).or_default();
            let id = get_next_subscription_id();
            entry.value().insert(id);
            id
        };

        let (tx, rx) = mpsc::channel(BROADCAST_CAPACITY);
        let v: Value = (id as i64).into();
        let tx1 = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = tx1.send(Arc::new(v.into())).await {
                warn!("Failed to send subscription id: {}. Error: {:?}", id, e);
            }
        });
        
        self.subscriptions.insert(id, tx);
        debug!("Subscription {id} is added");

        rx
    }

    fn unsubscribe(self, name: String, id: u32) {
        if let Some(v) = self.topics.get_mut(&name) {
            v.remove(&id);

            if v.is_empty() {
                info!("Topic: {:?} is deleted", &name);
                drop(v);
                self.topics.remove(&name);
            }
        }

        debug!("Subscription {} is removed1", id);
        self.subscriptions.remove(&id);
    }

    fn publish(self, name: String, value: Arc<CommandResponse>) {
        tokio::spawn(async move {
            match self.topics.get(&name) {
                Some(v) => {
                    let chan = v.value().clone();
                    for id in chan.into_iter() {
                        if let Some(tx) = self.subscriptions.get(&id) {
                            if let Err(e) = tx.send(value.clone()).await {
                                warn!("Publish to {} falied! error: {:?}", id, e);
                            }
                        }
                    }
                },
                None => {}
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use log::info;
    use tracing_test::traced_test;

    use crate::{service::topic::{Broadcaster, Topic}, Value, assert_res_ok};

    
    #[tokio::test]
    #[traced_test]
    async fn pub_sub_should_work() {
        let b = Arc::new(Broadcaster::default());
        let topic_name = "tonystark".to_string();
        let mut stream1 = b.clone().subscribe(topic_name.clone());
        let mut stream2 = b.clone().subscribe(topic_name.clone());

        let v: Value = "hello".into();
        b.clone().publish(topic_name.clone(), Arc::new(v.clone().into()));

        let id1: i64 = stream1.recv().await.unwrap().as_ref().try_into().unwrap();
        let id2: i64 = stream2.recv().await.unwrap().as_ref().try_into().unwrap();
        info!("------------id1:{id1}-----------id2:{id2}-------------");
        assert_ne!(id1, id2);

        //publish的时候，会把值也发送到这个channel，channel里面会接受，id和value两种
        let res1 = stream1.recv().await.unwrap();
        let res2 = stream2.recv().await.unwrap();
        assert_eq!(res1, res2);

        assert_res_ok(res1.as_ref(), &[v.clone()], &[]);
        b.clone().unsubscribe(topic_name.clone(), id1 as _);

        let v: Value = "world".into();
        b.clone().publish(topic_name.clone(), Arc::new(v.clone().into()));

        assert!(stream1.recv().await.is_none());
        let res2 = stream2.recv().await.unwrap();
        assert_res_ok(&res2, &[v.clone()], &[])

    }
}
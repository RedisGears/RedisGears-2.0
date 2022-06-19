use std::sync::{Arc, Weak};
use std::cell::RefCell;
use crate::RefCellWrapper;

pub(crate) type NotificationCallback = Box<dyn Fn(&str, &str, Box<dyn FnOnce(Result<(), String>) + Send + Sync>)>;

pub(crate) enum ConsumerKey {
    Key(String),
    Prefix(String),
}

#[derive(Clone)]
pub(crate) struct NotificationConsumerStats {
    pub(crate) num_trigger: usize,
    pub(crate) num_success: usize,
    pub(crate) num_failed: usize,
    pub(crate) last_error: Option<String>,
}

pub(crate) struct NotificationConsumer {
    key: ConsumerKey,
    callback: Option<NotificationCallback>,
    stats: Arc<RefCellWrapper<NotificationConsumerStats>>,
}

impl NotificationConsumer {
    fn new(key: ConsumerKey, callback: NotificationCallback) -> NotificationConsumer {
        NotificationConsumer {
            key: key,
            callback: Some(callback),
            stats: Arc::new(RefCellWrapper{ref_cell:RefCell::new(NotificationConsumerStats{
                num_trigger: 0,
                num_success: 0,
                num_failed: 0,
                last_error: None,
            })}),
        }
    }

    pub(crate) fn set_callback(&mut self, callback: NotificationCallback) -> Option<NotificationCallback> {
        let old_callback = self.callback.take();
        self.callback = Some(callback);
        old_callback
    }

    pub(crate) fn get_stats(&self) -> NotificationConsumerStats {
        self.stats.ref_cell.borrow().clone()
    }
}

fn fire_event(consumer: &Arc<RefCell<NotificationConsumer>>, event: &str, key: &str) {
    let c = consumer.borrow();
    {
        let mut stats = c.stats.ref_cell.borrow_mut();
        stats.num_trigger += 1;
    }
    let stats_ref = Arc::clone(&c.stats);
    (c.callback.as_ref().unwrap())(event, key, Box::new(move|res|{
        let mut stats = stats_ref.ref_cell.borrow_mut();
        if let Err(e) = res {
            stats.num_failed += 1;
            stats.last_error = Some(e);
        } else {
            stats.num_success += 1;
        }
    }));
}

pub(crate) struct KeysNotificationsCtx {
    consumers: Vec<Weak<RefCell<NotificationConsumer>>>,
}

impl KeysNotificationsCtx {
    pub(crate) fn new() -> KeysNotificationsCtx {
        KeysNotificationsCtx{ consumers: Vec::new() }
    }

    pub(crate) fn add_consumer_on_prefix(&mut self, prefix: &str, callback: NotificationCallback) -> Arc<RefCell<NotificationConsumer>> {
        let consumer = Arc::new(RefCell::new(NotificationConsumer::new(ConsumerKey::Prefix(prefix.to_string()), callback)));
        self.consumers.push(Arc::downgrade(&consumer));
        consumer
    }

    pub(crate) fn add_consumer_on_key(&mut self, key: &str, callback: NotificationCallback) -> Arc<RefCell<NotificationConsumer>> {
        let consumer = Arc::new(RefCell::new(NotificationConsumer::new(ConsumerKey::Key(key.to_string()), callback)));
        self.consumers.push(Arc::downgrade(&consumer));
        consumer
    }

    pub(crate) fn on_key_touched(&self, event: &str, key: &str) {
        for consumer in self.consumers.iter() {
            let consumer = match consumer.upgrade() {
                Some(c) => c,
                None => continue
            };
            if {
                let c = consumer.borrow_mut();
                match &c.key {
                    ConsumerKey::Key(k) => key == k,
                    ConsumerKey::Prefix(prefix) => key.starts_with(prefix),
                }
            } {
                fire_event(&consumer, event, key);
            }
        }
    }
}
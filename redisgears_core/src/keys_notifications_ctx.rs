use redisgears_plugin_api::redisgears_plugin_api::{
    keys_notifications_consumer_ctx::NotificationRunCtxInterface,
    run_function_ctx::RedisClientCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface,
};

use crate::run_ctx::RedisClient;
use crate::background_run_ctx::BackgroundRunCtx;

pub(crate) struct KeysNotificationsRunCtx;

impl NotificationRunCtxInterface for KeysNotificationsRunCtx {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient {})
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new())
    }
}
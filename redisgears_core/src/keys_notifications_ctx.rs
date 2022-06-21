use redisgears_plugin_api::redisgears_plugin_api::{
    keys_notifications_consumer_ctx::NotificationRunCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
};

use crate::background_run_ctx::BackgroundRunCtx;
use crate::run_ctx::RedisClient;

pub(crate) struct KeysNotificationsRunCtx;

impl NotificationRunCtxInterface for KeysNotificationsRunCtx {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient::new(None))
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(None))
    }
}

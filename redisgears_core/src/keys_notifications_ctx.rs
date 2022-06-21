use redis_module::context::CallOptionsBuilder;

use redisgears_plugin_api::redisgears_plugin_api::{
    keys_notifications_consumer_ctx::NotificationRunCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
};

use crate::background_run_ctx::BackgroundRunCtx;
use crate::run_ctx::{RedisClient, RedisClientCallOptions};

pub(crate) struct KeysNotificationsRunCtx;

impl NotificationRunCtxInterface for KeysNotificationsRunCtx {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient::new(None, 0))
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        let call_options = CallOptionsBuilder::new()
            .script_mode()
            .replicate()
            .verify_acl()
            .errors_as_replies()
            .constract();
        Box::new(BackgroundRunCtx::new(
            None,
            RedisClientCallOptions {
                call_options: call_options,
                flags: 0,
            },
        ))
    }
}

use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
};

use crate::run_ctx::RedisClient;

pub(crate) struct BackgroundRunCtx {}

unsafe impl Sync for BackgroundRunCtx {}
unsafe impl Send for BackgroundRunCtx {}

impl BackgroundRunFunctionCtxInterface for BackgroundRunCtx {
    fn lock<'a>(&'a self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient {})
    }
}

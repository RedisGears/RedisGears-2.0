use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface,
    run_function_ctx::RedisClientCtxInterface, run_function_ctx::RedisLogerCtxInterface,
};

use crate::run_ctx::RedisClient;

use crate::get_ctx;

pub(crate) struct BackgroundRunCtx {}

impl RedisLogerCtxInterface for BackgroundRunCtx {
    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }
}

unsafe impl Sync for BackgroundRunCtx {}
unsafe impl Send for BackgroundRunCtx {}

impl BackgroundRunFunctionCtxInterface for BackgroundRunCtx {
    fn lock<'a>(&'a self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient {})
    }
}

use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface,
    run_function_ctx::RedisBackgroundExecuterCtxInterface,
    run_function_ctx::RedisClientCtxInterface, run_function_ctx::RedisLogerCtxInterface,
};

use crate::get_thread_pool;
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

impl RedisBackgroundExecuterCtxInterface for BackgroundRunCtx {
    fn run_on_backgrond(&self, func: Box<dyn FnOnce() + Send>) {
        get_thread_pool().execute(move || {
            func();
        });
    }
}

impl BackgroundRunFunctionCtxInterface for BackgroundRunCtx {
    fn lock<'a>(&'a self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient {})
    }
}

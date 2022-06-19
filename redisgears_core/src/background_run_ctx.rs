use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
};

use crate::background_run_scope_guard::BackgroundRunScopeGuardCtx;

use redis_module::{ThreadSafeContext};

pub(crate) struct BackgroundRunCtx {}

unsafe impl Sync for BackgroundRunCtx {}
unsafe impl Send for BackgroundRunCtx {}

impl BackgroundRunCtx {
    pub(crate) fn new() -> BackgroundRunCtx {
        BackgroundRunCtx{}
    }
}

impl BackgroundRunFunctionCtxInterface for BackgroundRunCtx {
    fn lock<'a>(&'a self) -> Box<dyn RedisClientCtxInterface> {
        let ctx_guard = ThreadSafeContext::new().lock();
        Box::new(BackgroundRunScopeGuardCtx {
            _ctx_guard: ctx_guard,
        })
    }
}

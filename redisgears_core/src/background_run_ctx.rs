use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
};

use crate::background_run_scope_guard::BackgroundRunScopeGuardCtx;

use redis_module::ThreadSafeContext;

pub(crate) struct BackgroundRunCtx {
    user: Option<String>,
}

unsafe impl Sync for BackgroundRunCtx {}
unsafe impl Send for BackgroundRunCtx {}

impl BackgroundRunCtx {
    pub(crate) fn new(user: Option<String>) -> BackgroundRunCtx {
        BackgroundRunCtx { user: user }
    }
}

impl BackgroundRunFunctionCtxInterface for BackgroundRunCtx {
    fn lock<'a>(&'a self) -> Box<dyn RedisClientCtxInterface> {
        let ctx_guard = ThreadSafeContext::new().lock();
        Box::new(BackgroundRunScopeGuardCtx::new(
            ctx_guard,
            self.user.clone(),
        ))
    }
}

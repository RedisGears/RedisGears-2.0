use redis_module::{
    context::thread_safe::ContextGuard,
    context::{CallOptions, CallOptionsBuilder},
};

use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    CallResult,
};

use crate::{background_run_ctx::BackgroundRunCtx, call_redis_command};

pub(crate) struct BackgroundRunScopeGuardCtx {
    pub(crate) _ctx_guard: ContextGuard,
    call_options: CallOptions,
    user: Option<String>,
}

unsafe impl Sync for BackgroundRunScopeGuardCtx {}
unsafe impl Send for BackgroundRunScopeGuardCtx {}

impl BackgroundRunScopeGuardCtx {
    pub(crate) fn new(ctx_guard: ContextGuard, user: Option<String>) -> BackgroundRunScopeGuardCtx {
        let call_options = CallOptionsBuilder::new()
            .safe()
            .replicate()
            .verify_acl()
            .errors_as_replies();
        BackgroundRunScopeGuardCtx {
            _ctx_guard: ctx_guard,
            call_options: call_options.constract(),
            user: user,
        }
    }
}

impl RedisClientCtxInterface for BackgroundRunScopeGuardCtx {
    fn call(&self, command: &str, args: &[&str]) -> CallResult {
        call_redis_command(self.user.as_ref(), command, &self.call_options, args)
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new(self.user.clone()))
    }

    fn as_redis_client(&self) -> &dyn RedisClientCtxInterface {
        self
    }
}

use redis_module::{context::thread_safe::ContextGuard, RedisError};

use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RedisClientCtxInterface,
    CallResult,
};

use crate::{background_run_ctx::BackgroundRunCtx, get_ctx, redis_value_to_call_reply};

pub(crate) struct BackgroundRunScopeGuardCtx {
    pub(crate) _ctx_guard: ContextGuard,
}

unsafe impl Sync for BackgroundRunScopeGuardCtx {}
unsafe impl Send for BackgroundRunScopeGuardCtx {}

impl RedisClientCtxInterface for BackgroundRunScopeGuardCtx {
    fn call(&self, command: &str, args: &[&str]) -> CallResult {
        let res = get_ctx().call(command, args);
        match res {
            Ok(r) => redis_value_to_call_reply(r),
            Err(e) => match e {
                RedisError::Str(s) => CallResult::Error(s.to_string()),
                RedisError::String(s) => CallResult::Error(s),
                RedisError::WrongArity => CallResult::Error("Wrong arity".to_string()),
                RedisError::WrongType => CallResult::Error("Wrong type".to_string()),
            },
        }
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx::new())
    }

    fn as_redis_client(&self) -> &dyn RedisClientCtxInterface {
        self
    }
}

use redis_module::{context::thread_safe::ContextGuard, Context, RedisError};

use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunScopeGuardInterface, CallResult,
};

use crate::redis_value_to_call_reply;

pub(crate) struct BackgroundRunScopeGuardCtx<'a> {
    pub(crate) _ctx_guard: ContextGuard,
    pub(crate) ctx: &'a Context,
}

impl<'a> BackgroundRunScopeGuardInterface for BackgroundRunScopeGuardCtx<'a> {
    fn call(&self, command: &str, args: &[&str]) -> CallResult {
        let res = self.ctx.call(command, args);
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
}

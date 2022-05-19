use redis_module::{Context, RedisError, ThreadSafeContext};

use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface, run_function_ctx::RunFunctionCtxInterface,
    CallResult,
};

use crate::{get_ctx, get_thread_pool, redis_value_to_call_reply};

use std::slice::Iter;

use crate::background_run_ctx::BackgroundRunCtx;

pub(crate) struct RunCtx<'a> {
    pub(crate) ctx: &'a Context,
    pub(crate) iter: Iter<'a, redis_module::RedisString>,
}

impl<'a> RunFunctionCtxInterface for RunCtx<'a> {
    fn next_arg(&mut self) -> Option<&[u8]> {
        Some(self.iter.next()?.as_slice())
    }

    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }

    fn call(&self, command: &str, args: &[&str]) -> CallResult {
        let redis_ctx = get_ctx();
        let res = redis_ctx.call(command, args);
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

    fn reply_with_simple_string(&self, val: &str) {
        self.ctx.reply_simple_string(val);
    }

    fn reply_with_error(&self, val: &str) {
        self.ctx.reply_error_string(val);
    }

    fn reply_with_long(&self, val: i64) {
        self.ctx.reply_long(val);
    }

    fn reply_with_double(&self, val: f64) {
        self.ctx.reply_double(val);
    }

    fn reply_with_bulk_string(&self, val: &str) {
        self.ctx.reply_bulk_string(val);
    }

    fn reply_with_array(&self, size: usize) {
        self.ctx.reply_array(size);
    }

    fn get_background_ctx(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        let blocked_client = self.ctx.block_client();
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let ctx = thread_ctx.get_ctx();
        Box::new(BackgroundRunCtx {
            thread_ctx: thread_ctx,
            ctx: ctx,
        })
    }

    fn run_on_backgrond(&self, func: Box<dyn FnOnce() + Send>) {
        get_thread_pool().execute(move || {
            func();
        });
    }
}

use redis_module::{Context, RedisError, ThreadSafeContext};

use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface,
    run_function_ctx::RedisBackgroundExecuterCtxInterface,
    run_function_ctx::RedisClientCtxInterface, run_function_ctx::RedisLogerCtxInterface,
    run_function_ctx::ReplyCtxInterface, run_function_ctx::RunFunctionCtxInterface, CallResult,
};

use crate::{get_ctx, get_thread_pool, redis_value_to_call_reply};

use std::slice::Iter;

use crate::background_run_ctx::BackgroundRunCtx;

pub(crate) struct RedisClient {}

unsafe impl Sync for RedisClient {}
unsafe impl Send for RedisClient {}

impl RedisLogerCtxInterface for RedisClient {
    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }
}

impl RedisBackgroundExecuterCtxInterface for RedisClient {
    fn run_on_backgrond(&self, func: Box<dyn FnOnce() + Send>) {
        get_thread_pool().execute(move || {
            func();
        });
    }
}

impl RedisClientCtxInterface for RedisClient {
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

    fn as_redis_client(&self) -> &dyn RedisClientCtxInterface {
        self
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx {})
    }
}

pub(crate) struct RunCtx<'a> {
    pub(crate) ctx: &'a Context,
    pub(crate) iter: Iter<'a, redis_module::RedisString>,
}

impl<'a> ReplyCtxInterface for RunCtx<'a> {
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

    fn as_client(&self) -> &dyn ReplyCtxInterface {
        self
    }
}

impl<'a> RedisLogerCtxInterface for RunCtx<'a> {
    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }
}

unsafe impl<'a> Sync for RunCtx<'a> {}
unsafe impl<'a> Send for RunCtx<'a> {}

impl<'a> RedisBackgroundExecuterCtxInterface for RunCtx<'a> {
    fn run_on_backgrond(&self, func: Box<dyn FnOnce() + Send>) {
        get_thread_pool().execute(move || {
            func();
        });
    }
}

impl<'a> RunFunctionCtxInterface for RunCtx<'a> {
    fn next_arg(&mut self) -> Option<&[u8]> {
        Some(self.iter.next()?.as_slice())
    }

    fn get_background_client(&self) -> Box<dyn ReplyCtxInterface> {
        let blocked_client = self.ctx.block_client();
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let ctx = thread_ctx.get_ctx();
        Box::new(BackgroundClientCtx {
            _thread_ctx: thread_ctx,
            ctx: ctx,
        })
    }

    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient {})
    }
}

pub(crate) struct BackgroundClientCtx {
    _thread_ctx: ThreadSafeContext<redis_module::BlockedClient>,
    ctx: Context,
}

unsafe impl Sync for BackgroundClientCtx {}
unsafe impl Send for BackgroundClientCtx {}

impl ReplyCtxInterface for BackgroundClientCtx {
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

    fn as_client(&self) -> &dyn ReplyCtxInterface {
        self
    }
}

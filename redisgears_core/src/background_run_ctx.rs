use redis_module::{BlockedClient, Context, ThreadSafeContext};

use redisgears_plugin_api::redisgears_plugin_api::{
    run_function_ctx::BackgroundRunFunctionCtxInterface,
    run_function_ctx::BackgroundRunScopeGuardInterface,
};

use crate::background_run_scope_guard::BackgroundRunScopeGuardCtx;

use crate::get_ctx;

pub(crate) struct BackgroundRunCtx {
    pub(crate) thread_ctx: ThreadSafeContext<BlockedClient>,
    pub(crate) ctx: Context,
}

unsafe impl Send for BackgroundRunCtx {}

impl BackgroundRunFunctionCtxInterface for BackgroundRunCtx {
    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }

    fn lock<'a>(&'a self) -> Box<dyn BackgroundRunScopeGuardInterface + 'a> {
        Box::new(BackgroundRunScopeGuardCtx {
            _ctx_guard: self.thread_ctx.lock(),
            ctx: &self.ctx,
        })
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
}

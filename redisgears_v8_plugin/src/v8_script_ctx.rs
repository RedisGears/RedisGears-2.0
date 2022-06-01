use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::LibraryCtxInterface, load_library_ctx::LoadLibraryCtxInterface, GearsApiError,
};

use v8_rs::v8::{
    isolate::V8Isolate, v8_context::V8Context, v8_promise::V8PromiseState,
    v8_script::V8PersistedScript,
};

use std::sync::Arc;

pub(crate) struct V8ScriptCtx {
    pub(crate) script: V8PersistedScript,
    pub(crate) ctx: V8Context,
    pub(crate) isolate: V8Isolate,
    pub(crate) run_on_background: Box<dyn Fn(Box<dyn FnOnce() + Send>) + Send + Sync>,
    pub(crate) log: Box<dyn Fn(&str) + Send + Sync>,
}

impl V8ScriptCtx {
    pub(crate) fn new(
        isolate: V8Isolate,
        ctx: V8Context,
        script: V8PersistedScript,
        run_on_background: Box<dyn Fn(Box<dyn FnOnce() + Send>) + Send + Sync>,
        log: Box<dyn Fn(&str) + Send + Sync>,
    ) -> V8ScriptCtx {
        V8ScriptCtx {
            isolate: isolate,
            ctx: ctx,
            script: script,
            run_on_background: run_on_background,
            log: log,
        }
    }
}

pub(crate) struct V8LibraryCtx {
    pub(crate) script_ctx: Arc<V8ScriptCtx>,
}

impl LibraryCtxInterface for V8LibraryCtx {
    fn load_library(
        &self,
        load_library_ctx: &mut dyn LoadLibraryCtxInterface,
    ) -> Result<(), GearsApiError> {
        let _isolate_scope = self.script_ctx.isolate.enter();
        let _handlers_scope = self.script_ctx.isolate.new_handlers_scope();
        let ctx_scope = self.script_ctx.ctx.enter();
        let trycatch = self.script_ctx.isolate.new_try_catch();

        let script = self.script_ctx.script.to_local(&self.script_ctx.isolate);

        // set private content
        self.script_ctx
            .ctx
            .set_private_data(0, Some(&load_library_ctx));

        let res = script.run(&ctx_scope);

        // reset private data
        self.script_ctx
            .ctx
            .set_private_data::<&mut dyn LoadLibraryCtxInterface>(0, None);

        if res.is_none() {
            let error_utf8 = trycatch
                .get_exception()
                .to_utf8(&self.script_ctx.isolate)
                .unwrap();
            return Err(GearsApiError::Msg(format!(
                "Failed evaluating module: {}",
                error_utf8.as_str()
            )));
        }
        let res = res.unwrap();
        if res.is_promise() {
            let promise = res.as_promise();
            if promise.state() == V8PromiseState::Rejected {
                let error = promise.get_result();
                let error_utf8 = error.to_utf8(&self.script_ctx.isolate).unwrap();
                return Err(GearsApiError::Msg(format!(
                    "Failed evaluating module: {}",
                    error_utf8.as_str()
                )));
            }
        }
        Ok(())
    }
}

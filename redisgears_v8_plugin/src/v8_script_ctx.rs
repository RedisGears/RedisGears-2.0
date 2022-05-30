use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::LibraryCtxInterface, load_library_ctx::LoadLibraryCtxInterface, GearsApiError,
};

use v8_rs::v8::{
    isolate::V8Isolate, v8_context::V8Context, v8_promise::V8PromiseState,
    v8_script::V8PersistedScript,
};

use std::rc::Rc;

pub(crate) struct V8ScriptCtx {
    script: V8PersistedScript,
    ctx: Rc<V8Context>,
    isolate: Rc<V8Isolate>,
}

impl V8ScriptCtx {
    pub(crate) fn new(
        isolate: V8Isolate,
        ctx: V8Context,
        script: V8PersistedScript,
    ) -> V8ScriptCtx {
        V8ScriptCtx {
            isolate: Rc::new(isolate),
            ctx: Rc::new(ctx),
            script: script,
        }
    }
}

impl LibraryCtxInterface for V8ScriptCtx {
    fn load_library(
        &self,
        load_library_ctx: &mut dyn LoadLibraryCtxInterface,
    ) -> Result<(), GearsApiError> {
        let _isolate_scope = self.isolate.enter();
        let _handlers_scope = self.isolate.new_handlers_scope();
        let ctx_scope = self.ctx.enter();
        let trycatch = self.isolate.new_try_catch();

        let script = self.script.to_local(&self.isolate);

        // set private content
        self.ctx.set_private_data(0, Some(&load_library_ctx));
        self.ctx.set_private_data(1, Some(&self.ctx));
        self.ctx.set_private_data(2, Some(&self.isolate));

        let res = script.run(&ctx_scope);

        // reset private data
        self.ctx
            .set_private_data::<&mut dyn LoadLibraryCtxInterface>(0, None);
        self.ctx
            .set_private_data::<&mut dyn LoadLibraryCtxInterface>(1, None);
        self.ctx
            .set_private_data::<&mut dyn LoadLibraryCtxInterface>(2, None);

        if res.is_none() {
            let error_utf8 = trycatch.get_exception().to_utf8(&self.isolate).unwrap();
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
                let error_utf8 = error.to_utf8(&self.isolate).unwrap();
                return Err(GearsApiError::Msg(format!(
                    "Failed evaluating module: {}",
                    error_utf8.as_str()
                )));
            }
        }
        Ok(())
    }
}

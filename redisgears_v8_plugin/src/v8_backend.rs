use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtxInterface, load_library_ctx::LibraryCtxInterface, GearsApiError,
};

use crate::v8_script_ctx::V8ScriptCtx;

use v8_rs::v8::{isolate::V8Isolate, v8_init};

use crate::v8_native_functions::initialize_globals;

use crate::v8_script_ctx::V8LibraryCtx;

use std::alloc::{GlobalAlloc, Layout, System};
use std::str;

use std::sync::Arc;

struct MyAllocator {
    allocator: Option<&'static dyn GlobalAlloc>,
}

unsafe impl GlobalAlloc for MyAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        match self.allocator.as_ref() {
            Some(a) => a.alloc(layout),
            None => System.alloc(layout),
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        match self.allocator.as_ref() {
            Some(a) => a.dealloc(ptr, layout),
            None => System.dealloc(ptr, layout),
        }
    }
}

#[global_allocator]
static mut GLOBAL: MyAllocator = MyAllocator { allocator: None };

pub(crate) struct V8Backend;

impl BackendCtxInterface for V8Backend {
    fn get_name(&self) -> &'static str {
        "js"
    }

    fn initialize(&self, allocator: &'static dyn GlobalAlloc) -> Result<(), GearsApiError> {
        unsafe { GLOBAL.allocator = Some(allocator) }
        v8_init(); /* Initializing v8 */
        Ok(())
    }

    fn compile_library(
        &self,
        blob: &str,
        run_on_background: Box<dyn Fn(Box<dyn FnOnce() + Send>) + Send + Sync>,
        log: Box<dyn Fn(&str) + Send + Sync>,
    ) -> Result<Box<dyn LibraryCtxInterface>, GearsApiError> {
        let isolate = V8Isolate::new();

        let script_ctx = {
            let (ctx, script) = {
                let isolate_scope = isolate.enter();
                let _handlers_scope = isolate.new_handlers_scope();

                let ctx = isolate_scope.new_context(None);
                let ctx_scope = ctx.enter();

                let v8code_str = isolate.new_string(blob);

                let trycatch = isolate.new_try_catch();
                let script = match ctx_scope.compile(&v8code_str) {
                    Some(s) => s,
                    None => {
                        let error_utf8 = trycatch.get_exception().to_utf8(&isolate).unwrap();
                        return Err(GearsApiError::Msg(format!(
                            "Failed compiling code, {}",
                            error_utf8.as_str()
                        )));
                    }
                };

                let script = script.persist(&isolate);
                (ctx, script)
            };
            let script_ctx = Arc::new(V8ScriptCtx::new(
                isolate,
                ctx,
                script,
                run_on_background,
                log,
            ));
            {
                let _isolate_scope = script_ctx.isolate.enter();
                let _handlers_scope = script_ctx.isolate.new_handlers_scope();
                let ctx_scope = script_ctx.ctx.enter();
                let globals = ctx_scope.get_globals();
                initialize_globals(&script_ctx, &globals, &ctx_scope);
            }

            script_ctx
        };

        Ok(Box::new(V8LibraryCtx {
            script_ctx: script_ctx,
        }))
    }
}

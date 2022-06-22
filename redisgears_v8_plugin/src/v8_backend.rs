use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtxInterface, backend_ctx::CompiledLibraryInterface,
    load_library_ctx::LibraryCtxInterface, CallResult, GearsApiError,
};

use crate::v8_script_ctx::V8ScriptCtx;

use v8_rs::v8::{isolate::V8Isolate, v8_init_with_error_handlers};

use crate::v8_native_functions::initialize_globals;

use crate::v8_script_ctx::V8LibraryCtx;

use std::alloc::{GlobalAlloc, Layout, System};
use std::str;

use std::sync::{Arc, Weak};

struct Globals {
    allocator: Option<&'static dyn GlobalAlloc>,
    log: Option<Box<dyn Fn(&str) + 'static>>,
}

unsafe impl GlobalAlloc for Globals {
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
static mut GLOBAL: Globals = Globals {
    allocator: None,
    log: None,
};

pub(crate) fn log(msg: &str) {
    unsafe { (GLOBAL.log.as_ref().unwrap())(msg) };
}

pub(crate) struct V8Backend {
    pub(crate) script_ctx_vec: Vec<Weak<V8ScriptCtx>>,
}

impl V8Backend {
    fn isolates_gc(&mut self) {
        let indexes = self
            .script_ctx_vec
            .iter()
            .enumerate()
            .filter(|(_i, v)| v.strong_count() == 0)
            .map(|(i, _v)| i)
            .collect::<Vec<usize>>();
        for i in indexes.iter().rev() {
            self.script_ctx_vec.swap_remove(*i);
        }
    }
}

impl BackendCtxInterface for V8Backend {
    fn get_name(&self) -> &'static str {
        "js"
    }

    fn initialize(
        &self,
        allocator: &'static dyn GlobalAlloc,
        log_callback: Box<dyn Fn(&str) + 'static>,
    ) -> Result<(), GearsApiError> {
        unsafe {
            GLOBAL.allocator = Some(allocator);
            GLOBAL.log = Some(log_callback);
        }
        v8_init_with_error_handlers(
            Box::new(|line, msg| {
                let msg = format!("v8 fatal error on {}, {}", line, msg);
                log(&msg);
                panic!("{}", msg);
            }),
            Box::new(|line, is_heap_oom| {
                let msg = format!("v8 oom error on {}, is_heap_oom:{}", line, is_heap_oom);
                log(&msg);
                panic!("{}", msg);
            })
        ); 
        Ok(())
    }

    fn compile_library(
        &mut self,
        blob: &str,
        compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
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
            let script_ctx = Arc::new(V8ScriptCtx::new(isolate, ctx, script, compiled_library_api));
            self.script_ctx_vec.push(Arc::downgrade(&script_ctx));
            if self.script_ctx_vec.len() > 100 {
                // let try to do some gc
                self.isolates_gc();
            }
            {
                let _isolate_scope = script_ctx.isolate.enter();
                let _handlers_scope = script_ctx.isolate.new_handlers_scope();
                let ctx_scope = script_ctx.ctx.enter();
                let globals = ctx_scope.get_globals();

                let oom_script_ctx = Arc::downgrade(&script_ctx);

                script_ctx.isolate.set_near_oom_callback(move |curr_limit, initial_limit| {
                    let msg = format!("V8 near OOM notification arrive after script ctx was deleted, curr_limit={}, initial_limit={}", curr_limit, initial_limit);
                    let script_ctx = match oom_script_ctx.upgrade() {
                        Some(s_c) => s_c,
                        None => {
                            log(&msg);
                            panic!("{}", msg);
                        }
                    };

                    script_ctx.compiled_library_api.log(&msg);
                    // todo: think how to handle this, for now we will crash the processes.
                    // script_ctx.isolate.terminate_execution();

                    // script_ctx.compiled_library_api.log("Increase max memory to allow aborting the script");
                    
                    curr_limit
                });

                initialize_globals(&script_ctx, &globals, &ctx_scope);
            }

            script_ctx
        };

        Ok(Box::new(V8LibraryCtx {
            script_ctx: script_ctx,
        }))
    }

    fn debug(&mut self, args: &[&str]) -> Result<CallResult, GearsApiError> {
        let mut args = args.iter();
        let sub_command = args
            .next()
            .map_or(
                Err(GearsApiError::Msg(
                    "Subcommand was not provided".to_string(),
                )),
                |v| Ok(v),
            )?
            .to_lowercase();
        match sub_command.as_ref() {
            "isolates_stats" => {
                let active = self
                    .script_ctx_vec
                    .iter()
                    .filter(|v| v.strong_count() > 0)
                    .collect::<Vec<&Weak<V8ScriptCtx>>>()
                    .len() as i64;
                let not_active = self
                    .script_ctx_vec
                    .iter()
                    .filter(|v| v.strong_count() == 0)
                    .collect::<Vec<&Weak<V8ScriptCtx>>>()
                    .len() as i64;
                Ok(CallResult::Array(vec![
                    CallResult::BulkStr("active".to_string()),
                    CallResult::Long(active),
                    CallResult::BulkStr("not_active".to_string()),
                    CallResult::Long(not_active),
                ]))
            }
            "isolates_strong_count" => {
                let isolates_strong_count = self
                    .script_ctx_vec
                    .iter()
                    .map(|v| CallResult::Long(v.strong_count() as i64))
                    .collect::<Vec<CallResult>>();
                Ok(CallResult::Array(isolates_strong_count))
            }
            "isolates_gc" => {
                self.isolates_gc();
                Ok(CallResult::SimpleStr("OK".to_string()))
            }
            _ => Err(GearsApiError::Msg(format!(
                "Unknown subcommand '{}'",
                sub_command
            ))),
        }
    }
}

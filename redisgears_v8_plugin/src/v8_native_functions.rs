use redisgears_plugin_api::redisgears_plugin_api::{
    function_ctx::FunctionCtx, load_library_ctx::LoadLibraryCtx,
    run_function_ctx::BackgroundRunFunctionCtx, run_function_ctx::BackgroundRunScopeGuard,
    run_function_ctx::RunFunctionCtx, CallResult, GearsApiError,
};

use v8_rs::v8::{
    isolate::V8Isolate, v8_context::V8Context, v8_context_scope::V8ContextScope,
    v8_object_template::V8LocalObjectTemplate, v8_value::V8LocalValue, v8_version,
};

use crate::v8_function_ctx::V8Function;

use std::str;
use std::sync::Arc;

pub(crate) struct BackgroundExecutionCtx<'a> {
    pub(crate) bg_execution_ctx: &'a dyn BackgroundRunFunctionCtx,
    pub(crate) args: Vec<String>,
}

pub(crate) enum ExecutionCtx<'a> {
    Load(&'a mut dyn LoadLibraryCtx),
    Run(&'a mut dyn RunFunctionCtx),
    BackgroundRun(BackgroundExecutionCtx<'a>),
}

impl<'a> ExecutionCtx<'a> {
    pub(crate) fn register_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtx>,
    ) -> Result<(), GearsApiError> {
        match self {
            ExecutionCtx::Load(c) => c.register_function(name, function_ctx),
            _ => Err(GearsApiError::Msg(
                "Can not register function on run context".to_string(),
            )),
        }
    }

    pub(crate) fn log(&self, msg: &str) {
        match self {
            ExecutionCtx::Load(c) => c.log(msg),
            ExecutionCtx::Run(c) => c.log(msg),
            ExecutionCtx::BackgroundRun(c) => c.bg_execution_ctx.log(msg),
        }
    }

    pub(crate) fn is_loading(&self) -> bool {
        match self {
            ExecutionCtx::Load(_) => true,
            _ => false,
        }
    }

    pub(crate) fn is_background(&self) -> bool {
        match self {
            ExecutionCtx::BackgroundRun(_) => true,
            _ => false,
        }
    }

    pub(crate) fn call(&self, command: &str, args: &[&str]) -> Result<CallResult, String> {
        match self {
            ExecutionCtx::Load(_) => Err("Call 'call' out of context".to_string()),
            ExecutionCtx::BackgroundRun(_) => {
                Err("Call 'call' from background require entering atomic block".to_string())
            }
            ExecutionCtx::Run(c) => Ok(c.call(command, args)),
        }
    }

    pub(crate) fn reply_with_simple_string(&self, val: &str) {
        match self {
            ExecutionCtx::Run(ctx) => ctx.reply_with_simple_string(val),
            ExecutionCtx::BackgroundRun(ctx) => ctx.bg_execution_ctx.reply_with_simple_string(val),
            _ => panic!("Called reply function out of ctx"),
        }
    }

    pub(crate) fn reply_with_error(&self, val: &str) {
        match self {
            ExecutionCtx::Run(ctx) => ctx.reply_with_error(val),
            ExecutionCtx::BackgroundRun(ctx) => ctx.bg_execution_ctx.reply_with_error(val),
            _ => panic!("Called reply function out of ctx"),
        }
    }

    pub(crate) fn reply_with_long(&self, val: i64) {
        match self {
            ExecutionCtx::Run(ctx) => ctx.reply_with_long(val),
            ExecutionCtx::BackgroundRun(ctx) => ctx.bg_execution_ctx.reply_with_long(val),
            _ => panic!("Called reply function out of ctx"),
        }
    }

    pub(crate) fn reply_with_double(&self, val: f64) {
        match self {
            ExecutionCtx::Run(ctx) => ctx.reply_with_double(val),
            ExecutionCtx::BackgroundRun(ctx) => ctx.bg_execution_ctx.reply_with_double(val),
            _ => panic!("Called reply function out of ctx"),
        }
    }

    pub(crate) fn reply_with_bulk_string(&self, val: &str) {
        match self {
            ExecutionCtx::Run(ctx) => ctx.reply_with_bulk_string(val),
            ExecutionCtx::BackgroundRun(ctx) => ctx.bg_execution_ctx.reply_with_bulk_string(val),
            _ => panic!("Called reply function out of ctx"),
        }
    }

    pub(crate) fn reply_with_array(&self, size: usize) {
        match self {
            ExecutionCtx::Run(ctx) => ctx.reply_with_array(size),
            ExecutionCtx::BackgroundRun(ctx) => ctx.bg_execution_ctx.reply_with_array(size),
            _ => panic!("Called reply function out of ctx"),
        }
    }

    pub(crate) fn lock(&'a self) -> Box<dyn BackgroundRunScopeGuard + 'a> {
        match self {
            ExecutionCtx::BackgroundRun(ctx) => ctx.bg_execution_ctx.lock(),
            _ => panic!("Called reply function out of ctx"),
        }
    }
}

pub(crate) fn call_result_to_js_object(
    isolate: &V8Isolate,
    ctx_scope: &V8ContextScope,
    res: CallResult,
) -> Option<V8LocalValue> {
    match res {
        CallResult::SimpleStr(s) => Some(isolate.new_string(&s).to_value()),
        CallResult::BulkStr(s) => Some(isolate.new_string(&s).to_value()),
        CallResult::Error(e) => {
            isolate.raise_exception_str(&e);
            None
        }
        CallResult::Long(l) => Some(isolate.new_long(l)),
        CallResult::Double(d) => Some(isolate.new_double(d)),
        CallResult::Array(a) => {
            let mut has_error = false;
            let vals = a
                .into_iter()
                .map(|v| {
                    let res = call_result_to_js_object(isolate, ctx_scope, v);
                    if res.is_none() {
                        has_error = true;
                    }
                    res
                })
                .collect::<Vec<Option<V8LocalValue>>>();
            if has_error {
                return None;
            }

            let array = isolate.new_array(
                &vals
                    .iter()
                    .map(|v| v.as_ref().unwrap())
                    .collect::<Vec<&V8LocalValue>>(),
            );
            Some(array.to_value())
        }
        _ => panic!("Not yet supproted"),
    }
}

pub(crate) fn get_globals(isolate: &V8Isolate) -> V8LocalObjectTemplate {
    let mut redis = isolate.new_object_template();

    redis.add_native_function(isolate, "register_function", |args, isolate, curr_ctx_scope| {
        if args.len() != 2 {
            isolate.raise_exception_str("Wrong number of arguments to 'register_function' function");
            return None;
        }

        let function_name = args.get(0);
        if !function_name.is_string() {
            isolate.raise_exception_str("First argument to 'register_function' must be a string representing the function name");
            return None;
        }
        let function_name_utf8 = function_name.to_utf8(isolate).unwrap();

        let function_callback = args.get(1);
        if !function_callback.is_function() {
            isolate.raise_exception_str("Second argument to 'register_function' must be a function");
            return None;
        }
        let persisted_function = function_callback.persist(isolate);

        let load_ctx = curr_ctx_scope.get_private_data_mut::<ExecutionCtx>(0).unwrap();
        if !load_ctx.is_loading() {
            isolate.raise_exception_str("Called 'register_function' out of context");
            return None;
        }
        let ctx: &Arc<V8Context> = curr_ctx_scope.get_private_data_mut(1).unwrap();
        let isolate: &Arc<V8Isolate> = curr_ctx_scope.get_private_data_mut(2).unwrap();

        let f = V8Function::new(ctx, isolate, persisted_function, function_callback.is_async_function());

        let res = load_ctx.register_function(function_name_utf8.as_str(), Box::new(f));
        if let Err(err) = res {
            match err {
                GearsApiError::Msg(s) => isolate.raise_exception_str(&s),
            }
            return None;
        }
        None
    });

    redis.add_native_function(isolate, "v8_version", |_args, isolate, _curr_ctx_scope| {
        let v = v8_version();
        let v_v8_str = isolate.new_string(v);
        Some(v_v8_str.to_value())
    });

    redis.add_native_function(isolate, "log", |args, isolate, curr_ctx_scope| {
        if args.len() != 1 {
            isolate.raise_exception_str("Wrong number of arguments to 'log' function");
            return None;
        }

        let msg = args.get(0);
        if !msg.is_string() {
            isolate.raise_exception_str("First argument to 'log' must be a string message");
            return None;
        }

        let msg_utf8 = msg.to_utf8(isolate).unwrap();
        let load_ctx = match curr_ctx_scope.get_private_data_mut::<ExecutionCtx>(0) {
            Some(r_c) => r_c,
            None => {
                isolate.raise_exception_str("Called 'log' function out of context");
                return None;
            }
        };
        load_ctx.log(msg_utf8.as_str());
        None
    });

    redis.add_native_function(isolate, "call", |args, isolate, curr_ctx_scope| {
        if args.len() < 1 {
            isolate.raise_exception_str("Wrong number of arguments to 'call' function");
            return None;
        }

        let command = args.get(0);
        if !command.is_string() {
            isolate.raise_exception_str("First argument to 'command' must be a string");
            return None;
        }

        let command_utf8 = command.to_utf8(isolate).unwrap();
        let execution_ctx: &ExecutionCtx = curr_ctx_scope.get_private_data_mut(0).unwrap();

        let mut commands_args_str = Vec::new();
        for i in 1..args.len() {
            commands_args_str.push(args.get(i).to_utf8(isolate).unwrap());
        }

        let command_args_rust_str = commands_args_str
            .iter()
            .map(|v| v.as_str())
            .collect::<Vec<&str>>();

        let res = if execution_ctx.is_background() {
            let lock_ctx = execution_ctx.lock();
            Ok(lock_ctx.call(command_utf8.as_str(), &command_args_rust_str))
        } else {
            execution_ctx.call(command_utf8.as_str(), &command_args_rust_str)
        };
        let res = match res {
            Ok(r) => r,
            Err(e) => {
                isolate.raise_exception_str(&e);
                return None;
            }
        };

        call_result_to_js_object(isolate, curr_ctx_scope, res)
    });

    let mut globals = isolate.new_object_template();
    globals.add_object(isolate, "redis", &redis);

    return globals;
}

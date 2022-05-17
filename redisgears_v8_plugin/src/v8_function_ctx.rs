use redisgears_plugin_api::redisgears_plugin_api::{
    function_ctx::FunctionCallResult, function_ctx::FunctionCtx, run_function_ctx::RunFunctionCtx,
};

use v8_rs::v8::{
    isolate::V8Isolate, v8_context::V8Context, v8_context_scope::V8ContextScope,
    v8_promise::V8PromiseState, v8_value::V8LocalValue, v8_value::V8PersistValue,
};

use std::sync::Arc;

use crate::v8_native_functions::{BackgroundExecutionCtx, ExecutionCtx};

pub struct V8InternalFunction {
    ctx: Arc<V8Context>,
    isolate: Arc<V8Isolate>,
    persisted_function: V8PersistValue,
}

use std::str;

fn send_reply(
    isolate: &V8Isolate,
    ctx_scope: &V8ContextScope,
    execution_ctx: &ExecutionCtx,
    val: V8LocalValue,
) {
    if val.is_long() {
        execution_ctx.reply_with_long(val.get_long());
    } else if val.is_number() {
        execution_ctx.reply_with_double(val.get_number());
    } else if val.is_string() {
        execution_ctx.reply_with_bulk_string(val.to_utf8(isolate).unwrap().as_str());
    } else if val.is_array() {
        let arr = val.as_array();
        execution_ctx.reply_with_array(arr.len());
        for i in 0..arr.len() {
            let val = arr.get(ctx_scope, i);
            send_reply(isolate, ctx_scope, execution_ctx, val);
        }
    } else if val.is_object() {
        let res = val.as_object();
        let keys = res.get_property_names(ctx_scope);
        execution_ctx.reply_with_array(keys.len() * 2);
        for i in 0..keys.len() {
            let key = keys.get(ctx_scope, i);
            let obj = res.get(ctx_scope, &key);
            send_reply(isolate, ctx_scope, execution_ctx, key);
            send_reply(isolate, ctx_scope, execution_ctx, obj);
        }
    }
}

impl V8InternalFunction {
    fn call(&self, mut execution_ctx: ExecutionCtx) -> FunctionCallResult {
        let _isolate_scope = self.isolate.enter();
        let _handlers_scope = self.isolate.new_handlers_scope();
        let ctx_scope = self.ctx.enter();
        let trycatch = self.isolate.new_try_catch();

        // set private content
        self.ctx.set_private_data(0, Some(&execution_ctx));

        let res = {
            let args = match &mut execution_ctx {
                ExecutionCtx::Run(r) => {
                    let mut args = Vec::new();
                    while let Some(a) = r.next_arg() {
                        let arg = match str::from_utf8(a) {
                            Ok(s) => s,
                            Err(_) => {
                                execution_ctx
                                    .reply_with_error("Can not convert argument to string");
                                return FunctionCallResult::Done;
                            }
                        };
                        args.push(self.isolate.new_string(arg).to_value());
                    }
                    Some(args)
                }
                ExecutionCtx::BackgroundRun(b) => Some(
                    b.args
                        .iter()
                        .map(|v| self.isolate.new_string(v).to_value())
                        .collect::<Vec<V8LocalValue>>(),
                ),
                _ => None,
            };

            let args_ref = args.as_ref().map_or(None, |v| {
                let s = v.iter().map(|v| v).collect::<Vec<&V8LocalValue>>();
                Some(s)
            });

            let res = self
                .persisted_function
                .as_local(self.isolate.as_ref())
                .call(
                    &ctx_scope,
                    args_ref.as_ref().map_or(None, |v| Some(v.as_slice())),
                );
            res
        };

        self.ctx.set_private_data::<ExecutionCtx>(0, None);

        match res {
            Some(r) => {
                if r.is_promise() {
                    let res = r.as_promise();
                    if res.state() == V8PromiseState::Fulfilled
                        || res.state() == V8PromiseState::Rejected
                    {
                        let r = res.get_result();
                        if res.state() == V8PromiseState::Fulfilled {
                            send_reply(&self.isolate, &ctx_scope, &execution_ctx, r);
                        } else {
                            let r = r.to_utf8(&self.isolate).unwrap();
                            execution_ctx.reply_with_error(r.as_str());
                        }
                    } else {
                        let resolve = ctx_scope.new_native_function(|args, isolate, _context| {
                            let reply = args.get(0);
                            let reply = reply.to_utf8(isolate).unwrap();
                            execution_ctx.reply_with_bulk_string(reply.as_str());
                            None
                        });
                        let reject = ctx_scope.new_native_function(|args, isolate, _ctx_scope| {
                            let reply = args.get(0);
                            let reply = reply.to_utf8(isolate).unwrap();
                            execution_ctx.reply_with_error(reply.as_str());
                            None
                        });
                        res.then(&ctx_scope, &resolve, &reject);
                        return FunctionCallResult::Hold;
                    }
                } else {
                    send_reply(&self.isolate, &ctx_scope, &execution_ctx, r);
                }
            }
            None => {
                let error_utf8 = trycatch.get_exception().to_utf8(&self.isolate).unwrap();
                execution_ctx.reply_with_error(error_utf8.as_str());
            }
        }
        FunctionCallResult::Done
    }
}

pub struct V8Function {
    inner_function: Arc<V8InternalFunction>,
    is_async: bool,
}

impl V8Function {
    pub(crate) fn new(
        ctx: &Arc<V8Context>,
        isolate: &Arc<V8Isolate>,
        persisted_function: V8PersistValue,
        is_async: bool,
    ) -> V8Function {
        V8Function {
            inner_function: Arc::new(V8InternalFunction {
                ctx: Arc::clone(ctx),
                isolate: Arc::clone(isolate),
                persisted_function: persisted_function,
            }),
            is_async: is_async,
        }
    }
}

impl FunctionCtx for V8Function {
    fn call(&self, run_ctx: &mut dyn RunFunctionCtx) -> FunctionCallResult {
        if self.is_async {
            let inner_function = Arc::clone(&self.inner_function);
            // if we are going to the background we must consume all the arguments
            let mut args = Vec::new();
            while let Some(a) = run_ctx.next_arg() {
                let arg = match str::from_utf8(a) {
                    Ok(s) => s,
                    Err(_) => {
                        run_ctx.reply_with_error("Can not convert argument to string");
                        return FunctionCallResult::Done;
                    }
                };
                args.push(arg.to_string());
            }
            run_ctx.go_to_backgrond(Box::new(move |run_ctx| {
                let execution_ctx = ExecutionCtx::BackgroundRun(BackgroundExecutionCtx {
                    bg_execution_ctx: run_ctx.as_ref(),
                    args: args,
                });
                inner_function.call(execution_ctx);
            }));
            FunctionCallResult::Hold
        } else {
            let execution_ctx = ExecutionCtx::Run(run_ctx);
            self.inner_function.call(execution_ctx)
        }
    }
}

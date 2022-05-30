use redisgears_plugin_api::redisgears_plugin_api::{
    function_ctx::FunctionCtxInterface, run_function_ctx::RunFunctionCtxInterface,
    FunctionCallResult,
    run_function_ctx::ReplyCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface,
};

use v8_rs::v8::{
    isolate::V8Isolate, v8_context::V8Context, v8_context_scope::V8ContextScope,
    v8_promise::V8PromiseState, v8_value::V8LocalValue, v8_value::V8PersistValue,
};

use crate::v8_native_functions::{
    RedisClient,
    get_backgrounnd_client,
};

use std::cell::RefCell;
use std::sync::Arc;

use std::str;

struct BackgroundClientHolder {
    c: Option<Box<dyn ReplyCtxInterface>>
}

impl BackgroundClientHolder {
    fn unblock(&mut self) {
        self.c = None;
    }
}

pub struct V8InternalFunction {
    persisted_client: V8PersistValue,
    persisted_function: V8PersistValue,
    ctx: Arc<V8Context>,
    isolate: Arc<V8Isolate>,
}

fn send_reply(
    isolate: &V8Isolate,
    ctx_scope: &V8ContextScope,
    client: &dyn ReplyCtxInterface,
    val: V8LocalValue,
) {
    if val.is_long() {
        client.reply_with_long(val.get_long());
    } else if val.is_number() {
        client.reply_with_double(val.get_number());
    } else if val.is_string() {
        client.reply_with_bulk_string(val.to_utf8(isolate).unwrap().as_str());
    } else if val.is_array() {
        let arr = val.as_array();
        client.reply_with_array(arr.len());
        for i in 0..arr.len() {
            let val = arr.get(ctx_scope, i);
            send_reply(isolate, ctx_scope, client, val);
        }
    } else if val.is_object() {
        let res = val.as_object();
        let keys = res.get_property_names(ctx_scope);
        client.reply_with_array(keys.len() * 2);
        for i in 0..keys.len() {
            let key = keys.get(ctx_scope, i);
            let obj = res.get(ctx_scope, &key);
            send_reply(isolate, ctx_scope, client, key);
            send_reply(isolate, ctx_scope, client, obj);
        }
    }
}

impl V8InternalFunction {
    fn call_async(&self, command_args: Vec<String>, bg_client: Box<dyn ReplyCtxInterface>, redis_background_client: Box<dyn BackgroundRunFunctionCtxInterface>) -> FunctionCallResult {
        let _isolate_scope = self.isolate.enter();
        let _handlers_scope = self.isolate.new_handlers_scope();
        let ctx_scope = self.ctx.enter();
        let trycatch = self.isolate.new_try_catch();

        let res = {
            let r_client = get_backgrounnd_client(&self.isolate, &self.ctx, &ctx_scope, redis_background_client);
            let args ={
                let mut args = Vec::new();
                args.push(r_client.to_value());
                for arg in command_args.iter() {
                    args.push(self.isolate.new_string(arg).to_value());
                }
                Some(args)
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

        match res {
            Some(r) => {
                if r.is_promise() {
                    let res = r.as_promise();
                    if res.state() == V8PromiseState::Fulfilled
                        || res.state() == V8PromiseState::Rejected
                    {
                        let r = res.get_result();
                        if res.state() == V8PromiseState::Fulfilled {
                            send_reply(&self.isolate, &ctx_scope, bg_client.as_ref(), r);
                        } else {
                            let r = r.to_utf8(&self.isolate).unwrap();
                            bg_client.reply_with_error(r.as_str());
                        }
                    } else {
                        let bg_execution_ctx = BackgroundClientHolder{c:Some(bg_client)};
                        let execution_ctx_resolve = Arc::new(RefCell::new(bg_execution_ctx));
                        let execution_ctx_reject = Arc::clone(&execution_ctx_resolve);
                        let resolve =
                            ctx_scope.new_native_function(move |args, isolate, _context| {
                                let reply = args.get(0);
                                let reply = reply.to_utf8(isolate).unwrap();
                                let mut execution_ctx = execution_ctx_resolve.borrow_mut();
                                execution_ctx.c.as_ref().unwrap().reply_with_bulk_string(reply.as_str());
                                execution_ctx.unblock();
                                None
                            });
                        let reject =
                            ctx_scope.new_native_function(move |args, isolate, _ctx_scope| {
                                let reply = args.get(0);
                                let reply = reply.to_utf8(isolate).unwrap();
                                let mut execution_ctx = execution_ctx_reject.borrow_mut();
                                execution_ctx.c.as_ref().unwrap().reply_with_error(reply.as_str());
                                execution_ctx.unblock();
                                None
                            });
                        res.then(&ctx_scope, &resolve, &reject);
                        return FunctionCallResult::Hold;
                    }
                } else {
                    send_reply(&self.isolate, &ctx_scope, bg_client.as_ref(), r);
                }
            }
            None => {
                let error_utf8 = trycatch.get_exception().to_utf8(&self.isolate).unwrap();
                bg_client.reply_with_error(error_utf8.as_str());
            }
        }
        FunctionCallResult::Done
    }

    fn call_sync(&self, run_ctx: &mut dyn RunFunctionCtxInterface) -> FunctionCallResult {

        let _isolate_scope = self.isolate.enter();
        let _handlers_scope = self.isolate.new_handlers_scope();
        let ctx_scope = self.ctx.enter();
        let trycatch = self.isolate.new_try_catch();

        let res = {
            let args ={
                let mut args = Vec::new();
                args.push(self.persisted_client.as_local(&self.isolate));
                while let Some(a) = run_ctx.next_arg() {
                    let arg = match str::from_utf8(a) {
                        Ok(s) => s,
                        Err(_) => {
                            run_ctx.reply_with_error("Can not convert argument to string");
                            return FunctionCallResult::Done;
                        }
                    };
                    args.push(self.isolate.new_string(arg).to_value());
                }
                Some(args)
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

        match res {
            Some(r) => {
                if r.is_promise() {
                    let res = r.as_promise();
                    if res.state() == V8PromiseState::Fulfilled
                        || res.state() == V8PromiseState::Rejected
                    {
                        let r = res.get_result();
                        if res.state() == V8PromiseState::Fulfilled {
                            send_reply(&self.isolate, &ctx_scope, run_ctx.as_client(), r);
                        } else {
                            let r = r.to_utf8(&self.isolate).unwrap();
                            run_ctx.reply_with_error(r.as_str());
                        }
                    } else {
                        let bg_execution_ctx = BackgroundClientHolder{c:Some(run_ctx.get_background_client())};
                        let execution_ctx_resolve = Arc::new(RefCell::new(bg_execution_ctx));
                        let execution_ctx_reject = Arc::clone(&execution_ctx_resolve);
                        let resolve =
                            ctx_scope.new_native_function(move |args, isolate, _context| {
                                let reply = args.get(0);
                                let reply = reply.to_utf8(isolate).unwrap();
                                let mut execution_ctx = execution_ctx_resolve.borrow_mut();
                                execution_ctx.c.as_ref().unwrap().reply_with_bulk_string(reply.as_str());
                                execution_ctx.unblock();
                                None
                            });
                        let reject =
                            ctx_scope.new_native_function(move |args, isolate, _ctx_scope| {
                                let reply = args.get(0);
                                let reply = reply.to_utf8(isolate).unwrap();
                                let mut execution_ctx = execution_ctx_reject.borrow_mut();
                                execution_ctx.c.as_ref().unwrap().reply_with_error(reply.as_str());
                                execution_ctx.unblock();
                                None
                            });
                        res.then(&ctx_scope, &resolve, &reject);
                        return FunctionCallResult::Hold;
                    }
                } else {
                    send_reply(&self.isolate, &ctx_scope, run_ctx.as_client(), r);
                }
            }
            None => {
                let error_utf8 = trycatch.get_exception().to_utf8(&self.isolate).unwrap();
                run_ctx.reply_with_error(error_utf8.as_str());
            }
        }
        FunctionCallResult::Done
    }
}

pub struct V8Function {
    inner_function: Arc<V8InternalFunction>,
    client: Arc<RefCell<RedisClient>>,
    is_async: bool,
}

impl V8Function {
    pub(crate) fn new(
        ctx: &Arc<V8Context>,
        isolate: &Arc<V8Isolate>,
        persisted_function: V8PersistValue,
        persisted_client: V8PersistValue,
        client: &Arc<RefCell<RedisClient>>,
        is_async: bool,
    ) -> V8Function {
        V8Function {
            inner_function: Arc::new(V8InternalFunction {
                ctx: Arc::clone(ctx),
                isolate: Arc::clone(isolate),
                persisted_function: persisted_function,
                persisted_client: persisted_client,
            }),
            client: Arc::clone(client),
            is_async: is_async,
        }
    }
}

impl FunctionCtxInterface for V8Function {
    fn call(&self, run_ctx: &mut dyn RunFunctionCtxInterface) -> FunctionCallResult {
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
            let bg_client = run_ctx.get_background_client();
            let bg_redis_client = run_ctx.get_redis_client().get_background_redis_client();
            run_ctx.run_on_backgrond(Box::new(move || {
                inner_function.call_async(args, bg_client, bg_redis_client);
            }));
            FunctionCallResult::Done
        } else {
            let redis_client = run_ctx.get_redis_client();
            self.client.borrow_mut().set_client(redis_client);
            self.inner_function.call_sync(run_ctx);
            self.client.borrow_mut().make_invalid();
            FunctionCallResult::Done
        }
    }
}

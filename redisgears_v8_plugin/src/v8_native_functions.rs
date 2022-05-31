use redisgears_plugin_api::redisgears_plugin_api::{
    load_library_ctx::LoadLibraryCtxInterface, run_function_ctx::BackgroundRunFunctionCtxInterface,
    run_function_ctx::RedisClientCtxInterface, CallResult, GearsApiError,
};

use v8_rs::v8::{
    isolate::V8Isolate, v8_context_scope::V8ContextScope, v8_object::V8LocalObject,
    v8_value::V8LocalValue, v8_version,
};

use crate::v8_function_ctx::V8Function;
use crate::v8_script_ctx::V8ScriptCtx;
use crate::v8_stream_ctx::V8StreamCtx;

use std::cell::RefCell;
use std::str;
use std::sync::Arc;

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

pub(crate) struct RedisClient {
    client: Option<Box<dyn RedisClientCtxInterface>>,
}

impl RedisClient {
    pub(crate) fn new() -> RedisClient {
        RedisClient { client: None }
    }

    pub(crate) fn make_invalid(&mut self) {
        self.client = None;
    }

    pub(crate) fn set_client(&mut self, c: Box<dyn RedisClientCtxInterface>) {
        self.client = Some(c);
    }
}

pub(crate) fn get_backgrounnd_client(
    script_ctx: &Arc<V8ScriptCtx>,
    ctx_scope: &V8ContextScope,
    redis_background_client: Box<dyn BackgroundRunFunctionCtxInterface>,
) -> V8LocalObject {
    let bg_client = script_ctx.isolate.new_object();
    let redis_background_client = Arc::new(redis_background_client);
    let redis_background_client_ref = Arc::clone(&redis_background_client);
    let script_ctx_ref = Arc::clone(script_ctx);
    bg_client.set(
        ctx_scope,
        &script_ctx.isolate.new_string("block").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, ctx_scope| {
                if args.len() < 1 {
                    isolate.raise_exception_str("Wrong number of arguments to 'block' function");
                    return None;
                }
                let f = args.get(0);
                if !f.is_function() {
                    isolate.raise_exception_str("Argument to 'block' must be a function");
                    return None;
                }
                let redis_client = {
                    let _unlocker = isolate.new_unlocker();
                    redis_background_client_ref.lock()
                };
                let r_client = Arc::new(RefCell::new(RedisClient::new()));
                r_client.borrow_mut().set_client(redis_client);
                let c = get_redis_client(&script_ctx_ref, ctx_scope, &r_client);
                let res = f.call(ctx_scope, Some(&[&c.to_value()]));
                r_client.borrow_mut().make_invalid();
                res
            })
            .to_value(),
    );

    let redis_background_client_ref = Arc::clone(&redis_background_client);
    bg_client.set(
        ctx_scope,
        &script_ctx.isolate.new_string("log").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, _ctx_scope| {
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
                redis_background_client_ref.log(msg_utf8.as_str());
                None
            })
            .to_value(),
    );
    bg_client
}

pub(crate) fn get_redis_client(
    script_ctx: &Arc<V8ScriptCtx>,
    ctx_scope: &V8ContextScope,
    redis_client: &Arc<RefCell<RedisClient>>,
) -> V8LocalObject {
    let client = script_ctx.isolate.new_object();

    let redis_client_ref = Arc::clone(redis_client);
    client.set(
        ctx_scope,
        &script_ctx.isolate.new_string("call").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, ctx_scope| {
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

                let mut commands_args_str = Vec::new();
                for i in 1..args.len() {
                    commands_args_str.push(args.get(i).to_utf8(isolate).unwrap());
                }

                let command_args_rust_str = commands_args_str
                    .iter()
                    .map(|v| v.as_str())
                    .collect::<Vec<&str>>();

                let res = match redis_client_ref.borrow().client.as_ref() {
                    Some(c) => c.call(command_utf8.as_str(), &command_args_rust_str),
                    None => {
                        isolate.raise_exception_str("Used on invalid client");
                        return None;
                    }
                };

                call_result_to_js_object(isolate, ctx_scope, res)
            })
            .to_value(),
    );

    let redis_client_ref = Arc::clone(redis_client);
    client.set(
        ctx_scope,
        &script_ctx.isolate.new_string("log").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, _ctx_scope| {
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
                match redis_client_ref.borrow().client.as_ref() {
                    Some(r_c) => r_c.log(msg_utf8.as_str()),
                    None => {
                        isolate.raise_exception_str("Used on invalid client");
                        return None;
                    }
                };
                None
            })
            .to_value(),
    );

    let script_ctx_ref = Arc::clone(script_ctx);
    let redis_client_ref = Arc::clone(redis_client);
    client.set(
        ctx_scope,
        &script_ctx
            .isolate
            .new_string("run_on_background")
            .to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, ctx_scope| {
                if args.len() != 1 {
                    isolate.raise_exception_str(
                        "Wrong number of arguments to 'run_on_background' function",
                    );
                    return None;
                }

                let bg_redis_client = match redis_client_ref.borrow().client.as_ref() {
                    Some(c) => c.get_background_redis_client(),
                    None => {
                        isolate.raise_exception_str("Called 'run_on_background' out of context");
                        return None;
                    }
                };

                let f = args.get(0);
                if !f.is_async_function() {
                    isolate.raise_exception_str(
                        "First argument to 'run_on_background' must be an async function",
                    );
                    return None;
                }
                let f = f.persist(isolate);

                let new_script_ctx_ref = Arc::clone(&script_ctx_ref);
                let resolver = ctx_scope.new_resolver();
                let promise = resolver.get_promise();
                let resolver = resolver.to_value().persist(isolate);
                (script_ctx_ref.run_on_background)(Box::new(move || {
                    let _isolate_scope = new_script_ctx_ref.isolate.enter();
                    let _handlers_scope = new_script_ctx_ref.isolate.new_handlers_scope();
                    let ctx_scope = new_script_ctx_ref.ctx.enter();
                    let trycatch = new_script_ctx_ref.isolate.new_try_catch();

                    let background_client =
                        get_backgrounnd_client(&new_script_ctx_ref, &ctx_scope, bg_redis_client);
                    let res = f
                        .as_local(&new_script_ctx_ref.isolate)
                        .call(&ctx_scope, Some(&[&background_client.to_value()]));

                    let resolver = resolver.as_local(&new_script_ctx_ref.isolate).as_resolver();
                    match res {
                        Some(r) => resolver.resolve(&ctx_scope, &r),
                        None => {
                            let error_utf8 = trycatch.get_exception();
                            resolver.resolve(&ctx_scope, &error_utf8);
                        }
                    }
                }));
                Some(promise.to_value())
            })
            .to_value(),
    );

    client
}

pub(crate) fn initialize_globals(
    script_ctx: &Arc<V8ScriptCtx>,
    globals: &V8LocalObject,
    ctx_scope: &V8ContextScope,
) {
    let redis = script_ctx.isolate.new_object();

    let script_ctx_ref = Arc::clone(script_ctx);
    redis.set(ctx_scope,
        &script_ctx.isolate.new_string("register_stream_consumer").to_value(), 
        &ctx_scope.new_native_function(move|args, isolate, curr_ctx_scope| {
            if args.len() != 5 {
                isolate.raise_exception_str("Wrong number of arguments to 'register_stream_consumer' function");
                return None;
            }

            let consumer_name = args.get(0);
            if !consumer_name.is_string() {
                isolate.raise_exception_str("First argument to 'register_stream_consumer' must be a string representing the function name");
                return None;
            }
            let registration_name_utf8 = consumer_name.to_utf8(isolate).unwrap();

            let prefix = args.get(1);
            if !prefix.is_string() {
                isolate.raise_exception_str("Second argument to 'register_stream_consumer' must be a string representing the prefix");
                return None;
            }
            let prefix_utf8 = prefix.to_utf8(isolate).unwrap();

            let window = args.get(2);
            if !window.is_long() {
                isolate.raise_exception_str("Third argument to 'register_stream_consumer' must be a long representing the window size");
                return None;
            }
            let window = window.get_long();

            let trim = args.get(3);
            if !trim.is_boolean() {
                isolate.raise_exception_str("Dourth argument to 'register_stream_consumer' must be a boolean representing the trim option");
                return None;
            }
            let trim = trim.get_boolean();

            let function_callback = args.get(4);
            if !function_callback.is_function() {
                isolate.raise_exception_str("Fith argument to 'register_stream_consumer' must be a function");
                return None;
            }
            let persisted_function = function_callback.persist(isolate);

            let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface>(0);
            if load_ctx.is_none() {
                isolate.raise_exception_str("Called 'register_function' out of context");
                return None;
            }
            let load_ctx = load_ctx.unwrap();

            let v8_stream_ctx = V8StreamCtx::new(persisted_function, &script_ctx_ref, if function_callback.is_async_function() {true} else {false});
            let res = load_ctx.register_stream_consumer(registration_name_utf8.as_str(), prefix_utf8.as_str(), Box::new(v8_stream_ctx), window as usize, trim);
            if let Err(err) = res {
                match err {
                    GearsApiError::Msg(s) => isolate.raise_exception_str(&s),
                }
                return None;
            }
            None
    }).to_value());

    let script_ctx_ref = Arc::clone(script_ctx);
    redis.set(ctx_scope,
        &script_ctx.isolate.new_string("register_function").to_value(),
        &ctx_scope.new_native_function(move|args, isolate, curr_ctx_scope| {
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

            let load_ctx = curr_ctx_scope.get_private_data_mut::<&mut dyn LoadLibraryCtxInterface>(0);
            if load_ctx.is_none() {
                isolate.raise_exception_str("Called 'register_function' out of context");
                return None;
            }
            let load_ctx = load_ctx.unwrap();
            let c = Arc::new(RefCell::new(RedisClient::new()));
            let redis_client = get_redis_client(&script_ctx_ref, curr_ctx_scope, &c);

            let f = V8Function::new(
                &script_ctx_ref,
                persisted_function,
                redis_client.to_value().persist(isolate),
                &c,
                function_callback.is_async_function(),
            );

            let res = load_ctx.register_function(function_name_utf8.as_str(), Box::new(f));
            if let Err(err) = res {
                match err {
                    GearsApiError::Msg(s) => isolate.raise_exception_str(&s),
                }
                return None;
            }
            None
    }).to_value());

    redis.set(
        ctx_scope,
        &script_ctx.isolate.new_string("v8_version").to_value(),
        &ctx_scope
            .new_native_function(|_args, isolate, _curr_ctx_scope| {
                let v = v8_version();
                let v_v8_str = isolate.new_string(v);
                Some(v_v8_str.to_value())
            })
            .to_value(),
    );

    redis.set(
        ctx_scope,
        &script_ctx.isolate.new_string("log").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, curr_ctx_scope| {
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
                let load_ctx = match curr_ctx_scope
                    .get_private_data_mut::<&mut dyn LoadLibraryCtxInterface>(0)
                {
                    Some(r_c) => r_c,
                    None => {
                        isolate.raise_exception_str("Called 'log' function out of context");
                        return None;
                    }
                };
                load_ctx.log(msg_utf8.as_str());
                None
            })
            .to_value(),
    );

    globals.set(
        ctx_scope,
        &script_ctx.isolate.new_string("redis").to_value(),
        &redis.to_value(),
    );

    let script_ctx_ref = Arc::clone(script_ctx);
    globals.set(
        ctx_scope,
        &script_ctx.isolate.new_string("Promise").to_value(),
        &ctx_scope
            .new_native_function(move |args, isolate, curr_ctx_scope| {
                if args.len() != 1 {
                    isolate.raise_exception_str("Wrong number of arguments to 'Promise' function");
                    return None;
                }

                let function = args.get(0);
                if !function.is_function() || function.is_async_function() {
                    isolate.raise_exception_str("Bad argument to 'Promise' function");
                    return None;
                }

                let script_ctx_ref_resolve = Arc::clone(&script_ctx_ref);
                let script_ctx_ref_reject = Arc::clone(&script_ctx_ref);
                let resolver = curr_ctx_scope.new_resolver();
                let promise = resolver.get_promise();
                let resolver_resolve = Arc::new(resolver.to_value().persist(isolate));
                let resolver_reject = Arc::clone(&resolver_resolve);

                let resolve =
                    curr_ctx_scope.new_native_function(move |args, isolate, _curr_ctx_scope| {
                        if args.len() != 1 {
                            isolate.raise_exception_str(
                                "Wrong number of arguments to 'resolve' function",
                            );
                            return None;
                        }

                        let res = args.get(0).persist(isolate);
                        let new_script_ctx_ref_resolve = Arc::clone(&script_ctx_ref_resolve);
                        let resolver_resolve = Arc::clone(&resolver_resolve);
                        (script_ctx_ref_resolve.run_on_background)(Box::new(move || {
                            let _isolate_scope = new_script_ctx_ref_resolve.isolate.enter();
                            let _isolate_scope =
                                new_script_ctx_ref_resolve.isolate.new_handlers_scope();
                            let ctx_scope = new_script_ctx_ref_resolve.ctx.enter();
                            let _trycatch = new_script_ctx_ref_resolve.isolate.new_try_catch();
                            let res = res.as_local(&new_script_ctx_ref_resolve.isolate);
                            let resolver = resolver_resolve
                                .as_local(&new_script_ctx_ref_resolve.isolate)
                                .as_resolver();
                            resolver.resolve(&ctx_scope, &res);
                        }));
                        None
                    });

                let reject =
                    curr_ctx_scope.new_native_function(move |args, isolate, _curr_ctx_scope| {
                        if args.len() != 1 {
                            isolate.raise_exception_str(
                                "Wrong number of arguments to 'resolve' function",
                            );
                            return None;
                        }

                        let res = args.get(0).persist(isolate);
                        let new_script_ctx_ref_reject = Arc::clone(&script_ctx_ref_reject);
                        let resolver_reject = Arc::clone(&resolver_reject);
                        (script_ctx_ref_reject.run_on_background)(Box::new(move || {
                            let _isolate_scope = new_script_ctx_ref_reject.isolate.enter();
                            let _isolate_scope =
                                new_script_ctx_ref_reject.isolate.new_handlers_scope();
                            let ctx_scope = new_script_ctx_ref_reject.ctx.enter();
                            let _trycatch = new_script_ctx_ref_reject.isolate.new_try_catch();
                            let res = res.as_local(&new_script_ctx_ref_reject.isolate);
                            let resolver = resolver_reject
                                .as_local(&new_script_ctx_ref_reject.isolate)
                                .as_resolver();
                            resolver.reject(&ctx_scope, &res);
                        }));
                        None
                    });

                let _ = function.call(
                    curr_ctx_scope,
                    Some(&[&resolve.to_value(), &reject.to_value()]),
                );
                Some(promise.to_value())
            })
            .to_value(),
    );
}

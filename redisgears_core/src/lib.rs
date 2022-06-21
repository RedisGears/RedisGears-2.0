extern crate redis_module;

use redis_module::raw::RedisModule_GetDetachedThreadSafeContext;
use threadpool::ThreadPool;

use redis_module::{
    context::keys_cursor::KeysCursor, context::server_events::FlushSubevent,
    context::server_events::LoadingSubevent, context::server_events::ServerEventData,
    context::server_events::ServerRole, context::CallOptions, raw::KeyType::Stream, redis_command,
    redis_event_handler, redis_module, Context, InfoContext, NextArg, NotifyEvent, RedisError,
    RedisResult, RedisString, RedisValue, Status, ThreadSafeContext,
};

use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtxInterface, function_ctx::FunctionCtxInterface,
    keys_notifications_consumer_ctx::KeysNotificationsConsumerCtxInterface,
    load_library_ctx::LibraryCtxInterface, load_library_ctx::LoadLibraryCtxInterface,
    load_library_ctx::RegisteredKeys, stream_ctx::StreamCtxInterface, CallResult, GearsApiError,
};

use crate::run_ctx::RunCtx;

use libloading::{Library, Symbol};

use std::collections::HashMap;

use std::sync::Arc;

use crate::stream_reader::{ConsumerData, StreamReaderCtx};
use std::iter::Skip;
use std::vec::IntoIter;

use crate::compiled_library_api::CompiledLibraryAPI;
use crate::compiled_library_api::CompiledLibraryInternals;
use crate::keys_notifications::{KeysNotificationsCtx, NotificationCallback, NotificationConsumer};
use crate::keys_notifications_ctx::KeysNotificationsRunCtx;
use crate::stream_run_ctx::{GearsStreamConsumer, GearsStreamRecord};

use rdb::REDIS_GEARS_TYPE;

use std::cell::RefCell;

mod background_run_ctx;
mod background_run_scope_guard;
mod compiled_library_api;
mod keys_notifications;
mod keys_notifications_ctx;
mod rdb;
mod run_ctx;
mod stream_reader;
mod stream_run_ctx;

pub(crate) struct RefCellWrapper<T> {
    pub(crate) ref_cell: RefCell<T>,
}

unsafe impl<T> Sync for RefCellWrapper<T> {}
unsafe impl<T> Send for RefCellWrapper<T> {}

struct GearsLibraryMataData {
    name: String,
    engine: String,
    code: String,
}

struct GearsLibraryCtx {
    meta_data: GearsLibraryMataData,
    functions: HashMap<String, Box<dyn FunctionCtxInterface>>,
    stream_consumers:
        HashMap<String, Arc<RefCellWrapper<ConsumerData<GearsStreamRecord, GearsStreamConsumer>>>>,
    revert_stream_consumers: Vec<(String, GearsStreamConsumer, usize, bool)>,
    notifications_consumers: HashMap<String, Arc<RefCell<NotificationConsumer>>>,
    old_lib: Option<Box<GearsLibrary>>,
}

struct GearsLibrary {
    gears_lib_ctx: GearsLibraryCtx,
    _lib_ctx: Box<dyn LibraryCtxInterface>,
    compile_lib_internals: Arc<CompiledLibraryInternals>,
}

fn redis_value_to_call_reply(r: RedisValue) -> CallResult {
    match r {
        RedisValue::SimpleString(s) => CallResult::SimpleStr(s),
        RedisValue::SimpleStringStatic(s) => CallResult::SimpleStr(s.to_string()),
        RedisValue::BulkString(s) => CallResult::BulkStr(s.to_string()),
        RedisValue::BulkRedisString(s) => CallResult::BulkStr(s.try_as_str().unwrap().to_string()),
        RedisValue::Integer(i) => CallResult::Long(i),
        RedisValue::Float(f) => CallResult::Double(f),
        RedisValue::Array(a) => {
            let res = a
                .into_iter()
                .map(|v| redis_value_to_call_reply(v))
                .collect::<Vec<CallResult>>();
            CallResult::Array(res)
        }
        RedisValue::Null => CallResult::Null,
        _ => panic!("not yet implemented"),
    }
}

impl LoadLibraryCtxInterface for GearsLibraryCtx {
    fn register_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtxInterface>,
    ) -> Result<(), GearsApiError> {
        if self.functions.contains_key(name) {
            return Err(GearsApiError::Msg(format!(
                "Function {} already exists",
                name
            )));
        }
        self.functions.insert(name.to_string(), function_ctx);
        Ok(())
    }

    fn register_stream_consumer(
        &mut self,
        name: &str,
        prefix: &str,
        ctx: Box<dyn StreamCtxInterface>,
        window: usize,
        trim: bool,
    ) -> Result<(), GearsApiError> {
        if self.stream_consumers.contains_key(name) {
            return Err(GearsApiError::Msg(
                "Stream registration already exists".to_string(),
            ));
        }

        let stream_registration = if let Some(old_consumer) = self
            .old_lib
            .as_ref()
            .map_or(None, |v| v.gears_lib_ctx.stream_consumers.get(name))
        {
            let mut o_c = old_consumer.ref_cell.borrow_mut();
            if o_c.prefix != prefix {
                return Err(GearsApiError::Msg(
                    format!("Can not upgrade an existing consumer with different prefix, consumer: '{}', old_prefix: {}, new_prefix: {}.",
                    name, o_c.prefix, prefix)
                ));
            }
            let old_ctx = o_c.set_consumer(GearsStreamConsumer { ctx });
            let old_window = o_c.set_window(window);
            let old_trim = o_c.set_trim(trim);
            self.revert_stream_consumers
                .push((name.to_string(), old_ctx, old_window, old_trim));
            Arc::clone(old_consumer)
        } else {
            let globals = get_globals_mut();
            let stream_ctx = &mut globals.stream_ctx;
            let lib_name = self.meta_data.name.clone();
            let consumer_name = name.to_string();
            let consumer = stream_ctx.add_consumer(
                prefix,
                GearsStreamConsumer { ctx },
                window,
                trim,
                Some(Box::new(move |stream_name, ms, seq| {
                    redis_module::replicate(
                        get_ctx().ctx,
                        "_rg_internals.update_stream_last_read_id",
                        &[
                            &lib_name,
                            &consumer_name,
                            stream_name,
                            &ms.to_string(),
                            &seq.to_string(),
                        ],
                    );
                })),
            );
            if get_ctx().is_primary() {
                // trigger a key scan
                scan_key_space_for_streams();
            }
            consumer
        };

        self.stream_consumers
            .insert(name.to_string(), stream_registration);
        Ok(())
    }

    fn register_key_space_notification_consumer(
        &mut self,
        name: &str,
        key: RegisteredKeys,
        keys_notifications_consumer_ctx: Box<dyn KeysNotificationsConsumerCtxInterface>,
    ) -> Result<(), GearsApiError> {
        if self.notifications_consumers.contains_key(name) {
            return Err(GearsApiError::Msg(
                "Notification consumer already exists".to_string(),
            ));
        }

        let fire_event_callback: NotificationCallback =
            Box::new(move |event, key, done_callback| {
                let val = keys_notifications_consumer_ctx.on_notification_fired(
                    event,
                    key,
                    Box::new(KeysNotificationsRunCtx),
                );
                keys_notifications_consumer_ctx.post_command_notification(
                    val,
                    Box::new(KeysNotificationsRunCtx),
                    done_callback,
                )
            });

        let consumer = if let Some(old_notification_consumer) = self
            .old_lib
            .as_ref()
            .map_or(None, |v| v.gears_lib_ctx.notifications_consumers.get(name))
        {
            let mut o_c = old_notification_consumer.borrow_mut();
            let _old_consumer_callback = o_c.set_callback(fire_event_callback);
            // todo: add old consumer to some list for revert (if needed)
            Arc::clone(old_notification_consumer)
        } else {
            let globlas = get_globals_mut();
            let consumer = match key {
                RegisteredKeys::Key(k) => globlas
                    .notifications_ctx
                    .add_consumer_on_key(k, fire_event_callback),
                RegisteredKeys::Prefix(p) => globlas
                    .notifications_ctx
                    .add_consumer_on_prefix(p, fire_event_callback),
            };
            consumer
        };

        self.notifications_consumers
            .insert(name.to_string(), consumer);
        Ok(())
    }
}

struct GlobalCtx {
    libraries: HashMap<String, GearsLibrary>,
    backends: HashMap<String, Box<dyn BackendCtxInterface>>,
    redis_ctx: Context,
    authenticated_redis_ctx: Context,
    plugins: Vec<Library>,
    pool: ThreadPool,
    mgmt_pool: ThreadPool,
    stream_ctx: StreamReaderCtx<GearsStreamRecord, GearsStreamConsumer>,
    notifications_ctx: KeysNotificationsCtx,
}

static mut GLOBALS: Option<GlobalCtx> = None;

fn get_globals() -> &'static GlobalCtx {
    unsafe { GLOBALS.as_ref().unwrap() }
}

fn get_globals_mut() -> &'static mut GlobalCtx {
    unsafe { GLOBALS.as_mut().unwrap() }
}

pub fn get_ctx() -> &'static Context {
    &get_globals().redis_ctx
}

fn get_backends_mut() -> &'static mut HashMap<String, Box<dyn BackendCtxInterface>> {
    &mut get_globals_mut().backends
}

fn get_libraries() -> &'static HashMap<String, GearsLibrary> {
    &get_globals().libraries
}

fn get_libraries_mut() -> &'static mut HashMap<String, GearsLibrary> {
    &mut get_globals_mut().libraries
}

pub(crate) fn get_thread_pool() -> &'static ThreadPool {
    &get_globals().pool
}

pub(crate) fn call_redis_command(
    user: Option<&String>,
    command: &str,
    call_options: &CallOptions,
    args: &[&str],
) -> CallResult {
    let ctx = match user {
        Some(u) => {
            let ctx = &get_globals().authenticated_redis_ctx;
            if ctx.autenticate_user(u) == Status::Err {
                return CallResult::Error("Failed authenticating client".to_string());
            }
            ctx
        }
        None => get_ctx(),
    };
    let res = ctx.call_ext(command, call_options, args);
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

fn js_init(ctx: &Context, args: &Vec<RedisString>) -> Status {
    let mgmt_pool = ThreadPool::new(1);
    unsafe {
        let inner_ctx = RedisModule_GetDetachedThreadSafeContext.unwrap()(ctx.ctx);
        let inner_autenticated_ctx = RedisModule_GetDetachedThreadSafeContext.unwrap()(ctx.ctx);
        let mut global_ctx = GlobalCtx {
            libraries: HashMap::new(),
            redis_ctx: Context::new(inner_ctx),
            authenticated_redis_ctx: Context::new(inner_autenticated_ctx),
            backends: HashMap::new(),
            plugins: Vec::new(),
            pool: ThreadPool::new(1),
            mgmt_pool: mgmt_pool,
            stream_ctx: StreamReaderCtx::new(
                Box::new(|key, id, include_id| {
                    // read data from the stream
                    let ctx = get_ctx();
                    let stream_name = ctx.create_string(key);
                    let key = ctx.open_key(&stream_name);
                    let mut stream_iterator =
                        match key.get_stream_range_iterator(id, None, !include_id) {
                            Ok(s) => s,
                            Err(_) => {
                                return Err("Key does not exists on is not a stream".to_string())
                            }
                        };

                    Ok(match stream_iterator.next() {
                        Some(e) => Some(GearsStreamRecord { record: e }),
                        None => None,
                    })
                }),
                Box::new(|key_name, id| {
                    // trim the stream callback
                    let ctx = get_ctx();
                    let stream_name = ctx.create_string(key_name);
                    let key = ctx.open_key_writable(&stream_name);
                    let res = key.trim_stream_by_id(id, false);
                    if let Err(e) = res {
                        ctx.log_debug(&format!(
                            "Error occured when trimming stream (stream was probably deleted): {}",
                            e
                        ))
                    } else {
                        redis_module::replicate(
                            ctx.ctx,
                            "xtrim",
                            &[key_name, "MINID", &format!("{}-{}", id.ms, id.seq)],
                        );
                    }
                }),
            ),
            notifications_ctx: KeysNotificationsCtx::new(),
        };

        let v8_path = match args.into_iter().next() {
            Some(a) => a,
            None => {
                ctx.log_warning("Path to libredisgears_v8_plugin.so must be specified");
                return Status::Err;
            }
        }
        .try_as_str();
        let v8_path = match v8_path {
            Ok(a) => a,
            Err(_) => {
                ctx.log_warning("Path to libredisgears_v8_plugin.so must be specified");
                return Status::Err;
            }
        };
        let lib = match Library::new(v8_path) {
            Ok(l) => l,
            Err(e) => {
                ctx.log_warning(&format!("Failed loading '{}', {}", v8_path, e));
                return Status::Err;
            }
        };
        {
            let func: Symbol<unsafe fn() -> *mut dyn BackendCtxInterface> =
                lib.get(b"initialize_plugin").unwrap();
            let backend = Box::from_raw(func());
            let name = backend.get_name();
            ctx.log_notice(&format!("registering backend: {}", name));
            if global_ctx.backends.contains_key(name) {
                ctx.log_warning(&format!("Backend {} already exists", name));
                return Status::Err;
            }
            if let Err(e) = backend.initialize(
                &redis_module::ALLOC,
                Box::new(|msg| get_ctx().log_notice(msg)),
            ) {
                ctx.log_warning(&format!("Failed loading {} backend, {}", name, e.get_msg()));
                return Status::Err;
            }
            global_ctx.backends.insert(name.to_string(), backend);
        }
        global_ctx.plugins.push(lib);

        GLOBALS = Some(global_ctx);
    }
    Status::Ok
}

const fn js_info(_ctx: &InfoContext, _for_crash_report: bool) {}

fn function_call_command(
    ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let library_name = args.next_arg()?.try_as_str()?;
    let function_name = args.next_arg()?.try_as_str()?;
    let libraries = get_libraries();

    let lib = libraries.get(library_name);
    if lib.is_none() {
        return Err(RedisError::String(format!(
            "Unknown library {}",
            library_name
        )));
    }

    let lib = lib.unwrap();
    let function = lib.gears_lib_ctx.functions.get(function_name);
    if function.is_none() {
        return Err(RedisError::String(format!(
            "Unknown function {}",
            function_name
        )));
    }

    let function = function.unwrap();

    let args = args.collect::<Vec<redis_module::RedisString>>();
    let args_iter = args.iter();

    function.call(&mut RunCtx {
        ctx: ctx,
        iter: args_iter,
    });

    Ok(RedisValue::NoReply)
}

fn library_extract_matadata(code: &str) -> Result<GearsLibraryMataData, RedisError> {
    let shabeng = match code.split("\n").next() {
        Some(s) => s,
        None => return Err(RedisError::Str("could not extract library metadata")),
    };
    if !shabeng.starts_with("#!") {
        return Err(RedisError::Str("could not find #! syntax"));
    }

    let shabeng = shabeng.strip_prefix("#!").unwrap();
    let mut data = shabeng.split(" ");
    let engine = match data.next() {
        Some(s) => s,
        None => return Err(RedisError::Str("could not extract engine name")),
    };

    let name = loop {
        let d = match data.next() {
            Some(s) => s,
            None => return Err(RedisError::Str("Failed find 'name' property")),
        };
        let mut prop = d.split("=");
        let prop_name = match prop.next() {
            Some(s) => s,
            None => return Err(RedisError::Str("could not extract property name")),
        };
        let prop_val = match prop.next() {
            Some(s) => s,
            None => return Err(RedisError::Str("could not extract property value")),
        };
        if prop_name.to_lowercase() != "name" {
            return Err(RedisError::String(format!(
                "unknown property '{}'",
                prop_name
            )));
        }
        break prop_val;
    };

    Ok(GearsLibraryMataData {
        engine: engine.to_string(),
        name: name.to_string(),
        code: code.to_string(),
    })
}

fn function_del_command(
    ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let name = args
        .next()
        .map_or(Err(RedisError::Str("function name was not given")), |s| {
            s.try_as_str()
        })?;
    let libraries = get_libraries_mut();
    match libraries.remove(name) {
        Some(_) => {
            ctx.replicate_verbatim();
            Ok(RedisValue::SimpleStringStatic("OK"))
        }
        None => Err(RedisError::Str("library does not exists")),
    }
}

fn function_call_result_to_redis_result(res: CallResult) -> RedisValue {
    match res {
        CallResult::Long(l) => RedisValue::Integer(l),
        CallResult::BulkStr(s) => RedisValue::BulkString(s),
        CallResult::SimpleStr(s) => RedisValue::SimpleString(s),
        CallResult::Null => RedisValue::Null,
        CallResult::Double(d) => RedisValue::Float(d),
        CallResult::Error(s) => RedisValue::SimpleString(s),
        CallResult::Array(arr) => RedisValue::Array(
            arr.into_iter()
                .map(|v| function_call_result_to_redis_result(v))
                .collect::<Vec<RedisValue>>(),
        ),
    }
}

fn function_debug_command(
    _ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let backend_name = args.next_arg()?.try_as_str()?;
    let backend = get_backends_mut().get_mut(backend_name).map_or(
        Err(RedisError::String(format!(
            "Backend '{}' does not exists",
            backend_name
        ))),
        |v| Ok(v),
    )?;
    let mut has_errors = false;
    let args = args
        .map(|v| {
            let res = v.try_as_str();
            if res.is_err() {
                has_errors = true;
            }
            res
        })
        .collect::<Vec<Result<&str, RedisError>>>();
    if has_errors {
        return Err(RedisError::Str("Failed converting arguments to string"));
    }
    let args = args.into_iter().map(|v| v.unwrap()).collect::<Vec<&str>>();
    let res = backend.debug(args.as_slice());
    match res {
        Ok(res) => Ok(function_call_result_to_redis_result(res)),
        Err(e) => match e {
            GearsApiError::Msg(msg) => Err(RedisError::String(msg)),
        },
    }
}
fn function_list_command(
    _ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let mut with_code = false;
    let mut lib = None;
    let mut verbosity = 0;
    loop {
        let arg = args.next_arg();
        if arg.is_err() {
            break;
        }
        let arg = arg.unwrap();
        let arg_str = match arg.try_as_str() {
            Ok(arg) => arg,
            Err(_) => return Err(RedisError::Str("Binary option is not allowed")),
        };
        let arg_str = arg_str.to_lowercase();
        match arg_str.as_ref() {
            "withcode" => with_code = true,
            "verbose" => verbosity = verbosity + 1,
            "v" => verbosity = verbosity + 1,
            "vv" => verbosity = verbosity + 2,
            "vvv" => verbosity = verbosity + 3,
            "library" => {
                let lib_name = match args.next_arg() {
                    Ok(n) => match n.try_as_str() {
                        Ok(n) => n,
                        Err(_) => return Err(RedisError::Str("Library name is not a string")),
                    },
                    Err(_) => return Err(RedisError::Str("Library name was not given")),
                };
                lib = Some(lib_name);
            }
            _ => return Err(RedisError::String(format!("Unknown option '{}'", arg_str))),
        }
    }
    let libraries = get_libraries_mut();
    Ok(RedisValue::Array(
        libraries
            .values()
            .filter(|l| match lib {
                Some(lib_name) => {
                    if l.gears_lib_ctx.meta_data.name == lib_name {
                        true
                    } else {
                        false
                    }
                }
                None => true,
            })
            .map(|l| {
                let mut res = vec![
                    RedisValue::BulkString("engine".to_string()),
                    RedisValue::BulkString(l.gears_lib_ctx.meta_data.engine.to_string()),
                    RedisValue::BulkString("name".to_string()),
                    RedisValue::BulkString(l.gears_lib_ctx.meta_data.name.to_string()),
                    RedisValue::BulkString("pending_jobs".to_string()),
                    RedisValue::Integer(l.compile_lib_internals.pending_jobs() as i64),
                    RedisValue::BulkString("functions".to_string()),
                    RedisValue::Array(
                        l.gears_lib_ctx
                            .functions
                            .keys()
                            .map(|k| RedisValue::BulkString(k.to_string()))
                            .collect::<Vec<RedisValue>>(),
                    ),
                    RedisValue::BulkString("stream_consumers".to_string()),
                    RedisValue::Array(
                        l.gears_lib_ctx
                            .stream_consumers
                            .iter()
                            .map(|(k, v)| {
                                let v = v.ref_cell.borrow();
                                if verbosity > 0 {
                                    let mut res = vec![
                                        RedisValue::BulkString("name".to_string()),
                                        RedisValue::BulkString(k.to_string()),
                                        RedisValue::BulkString("prefix".to_string()),
                                        RedisValue::BulkString(v.prefix.to_string()),
                                        RedisValue::BulkString("window".to_string()),
                                        RedisValue::Integer(v.window as i64),
                                        RedisValue::BulkString("trim".to_string()),
                                        RedisValue::BulkString(
                                            (if v.trim { "enabled" } else { "disabled" })
                                                .to_string(),
                                        ),
                                        RedisValue::BulkString("num_streams".to_string()),
                                        RedisValue::Integer(v.consumed_streams.len() as i64),
                                    ];
                                    if verbosity > 1 {
                                        res.push(RedisValue::BulkString("streams".to_string()));
                                        res.push(RedisValue::Array(
                                            v.consumed_streams
                                                .iter()
                                                .map(|(s, v)| {
                                                    let v = v.ref_cell.borrow();
                                                    let mut res = Vec::new();
                                                    res.push(RedisValue::BulkString(
                                                        "name".to_string(),
                                                    ));
                                                    res.push(RedisValue::BulkString(s.to_string()));

                                                    res.push(RedisValue::BulkString(
                                                        "last_processed_time".to_string(),
                                                    ));
                                                    res.push(RedisValue::Integer(
                                                        v.last_processed_time as i64,
                                                    ));

                                                    res.push(RedisValue::BulkString(
                                                        "avg_processed_time".to_string(),
                                                    ));
                                                    res.push(RedisValue::Float(
                                                        v.total_processed_time as f64
                                                            / v.records_processed as f64,
                                                    ));

                                                    res.push(RedisValue::BulkString(
                                                        "last_lag".to_string(),
                                                    ));
                                                    res.push(RedisValue::Integer(
                                                        v.last_lag as i64,
                                                    ));

                                                    res.push(RedisValue::BulkString(
                                                        "avg_lag".to_string(),
                                                    ));
                                                    res.push(RedisValue::Float(
                                                        v.total_lag as f64
                                                            / v.records_processed as f64,
                                                    ));

                                                    res.push(RedisValue::BulkString(
                                                        "total_record_processed".to_string(),
                                                    ));
                                                    res.push(RedisValue::Integer(
                                                        v.records_processed as i64,
                                                    ));

                                                    res.push(RedisValue::BulkString(
                                                        "id_to_read_from".to_string(),
                                                    ));
                                                    match v.last_read_id {
                                                        Some(id) => {
                                                            res.push(RedisValue::BulkString(
                                                                format!("{}-{}", id.ms, id.seq),
                                                            ))
                                                        }
                                                        None => res.push(RedisValue::BulkString(
                                                            "None".to_string(),
                                                        )),
                                                    }
                                                    res.push(RedisValue::BulkString(
                                                        "last_error".to_string(),
                                                    ));
                                                    match &v.last_error {
                                                        Some(err) => res.push(
                                                            RedisValue::BulkString(err.to_string()),
                                                        ),
                                                        None => res.push(RedisValue::BulkString(
                                                            "None".to_string(),
                                                        )),
                                                    }
                                                    if verbosity > 2 {
                                                        res.push(RedisValue::BulkString(
                                                            "pending_ids".to_string(),
                                                        ));
                                                        let pending_ids = v
                                                            .pending_ids
                                                            .iter()
                                                            .map(|e| {
                                                                RedisValue::BulkString(format!(
                                                                    "{}-{}",
                                                                    e.ms, e.seq
                                                                ))
                                                            })
                                                            .collect::<Vec<RedisValue>>();
                                                        res.push(RedisValue::Array(pending_ids));
                                                    }
                                                    RedisValue::Array(res)
                                                })
                                                .collect::<Vec<RedisValue>>(),
                                        ));
                                    }
                                    RedisValue::Array(res)
                                } else {
                                    RedisValue::BulkString(k.to_string())
                                }
                            })
                            .collect::<Vec<RedisValue>>(),
                    ),
                    RedisValue::BulkString("notifications_consumers".to_string()),
                    RedisValue::Array(
                        l.gears_lib_ctx
                            .notifications_consumers
                            .iter()
                            .map(|(name, c)| {
                                if verbosity == 0 {
                                    RedisValue::BulkString(name.to_string())
                                } else {
                                    let stats = c.borrow().get_stats();
                                    RedisValue::Array(vec![
                                        RedisValue::BulkString("name".to_string()),
                                        RedisValue::BulkString(name.to_string()),
                                        RedisValue::BulkString("num_triggered".to_string()),
                                        RedisValue::Integer(stats.num_trigger as i64),
                                        RedisValue::BulkString("num_success".to_string()),
                                        RedisValue::Integer(stats.num_success as i64),
                                        RedisValue::BulkString("num_failed".to_string()),
                                        RedisValue::Integer(stats.num_failed as i64),
                                        RedisValue::BulkString("last_error".to_string()),
                                        RedisValue::BulkString(match stats.last_error {
                                            Some(s) => s,
                                            None => "None".to_string(),
                                        }),
                                    ])
                                }
                            })
                            .collect::<Vec<RedisValue>>(),
                    ),
                ];
                if with_code {
                    res.push(RedisValue::BulkString("code".to_string()));
                    res.push(RedisValue::BulkString(
                        l.gears_lib_ctx.meta_data.code.to_string(),
                    ));
                }
                RedisValue::Array(res)
            })
            .collect::<Vec<RedisValue>>(),
    ))
}

pub(crate) fn function_load_intrernal(code: &str, upgrade: bool) -> RedisResult {
    let meta_data = library_extract_matadata(code)?;
    let backend_name = meta_data.engine.as_str();
    let backend = get_backends_mut().get_mut(backend_name);
    if backend.is_none() {
        return Err(RedisError::String(format!(
            "Unknown backend {}",
            backend_name
        )));
    }
    let backend = backend.unwrap();
    let compile_lib_ctx = CompiledLibraryAPI::new();
    let compile_lib_internals = compile_lib_ctx.take_internals();
    let lib_ctx = backend.compile_library(code, Box::new(compile_lib_ctx));
    let lib_ctx = match lib_ctx {
        Err(e) => match e {
            GearsApiError::Msg(s) => {
                return Err(RedisError::String(format!(
                    "Failed library compilation {}",
                    s
                )))
            }
        },
        Ok(lib_ctx) => lib_ctx,
    };
    let libraries = get_libraries_mut();
    let old_lib = libraries.remove(&meta_data.name);
    if old_lib.is_some() && !upgrade {
        let err = Err(RedisError::String(format!(
            "Library {} already exists",
            &meta_data.name
        )));
        libraries.insert(meta_data.name, old_lib.unwrap());
        return err;
    }
    let mut gears_library = GearsLibraryCtx {
        meta_data: meta_data,
        functions: HashMap::new(),
        stream_consumers: HashMap::new(),
        notifications_consumers: HashMap::new(),
        revert_stream_consumers: Vec::new(),
        old_lib: old_lib.map_or(None, |v| Some(Box::new(v))),
    };
    let res = lib_ctx.load_library(&mut gears_library);
    if let Err(err) = res {
        let ret = match err {
            GearsApiError::Msg(s) => {
                let msg = format!("Failed loading library, {}", s);
                Err(RedisError::String(msg))
            }
        };
        if let Some(old_lib) = gears_library.old_lib.take() {
            for (name, old_ctx, old_window, old_trim) in gears_library.revert_stream_consumers {
                let stream_data = gears_library.stream_consumers.get(&name).unwrap();
                let mut s_d = stream_data.ref_cell.borrow_mut();
                s_d.set_consumer(old_ctx);
                s_d.set_window(old_window);
                s_d.set_trim(old_trim);
            }
            libraries.insert(gears_library.meta_data.name, *old_lib);
        }
        return ret;
    }
    if gears_library.functions.len() == 0
        && gears_library.stream_consumers.len() == 0
        && gears_library.notifications_consumers.len() == 0
    {
        if let Some(old_lib) = gears_library.old_lib.take() {
            for (name, old_ctx, old_window, old_trim) in gears_library.revert_stream_consumers {
                let stream_data = gears_library.stream_consumers.get(&name).unwrap();
                let mut s_d = stream_data.ref_cell.borrow_mut();
                s_d.set_consumer(old_ctx);
                s_d.set_window(old_window);
                s_d.set_trim(old_trim);
            }
            libraries.insert(gears_library.meta_data.name, *old_lib);
        }
        return Err(RedisError::Str(
            "No function nor registrations was registered",
        ));
    }
    gears_library.old_lib = None;
    libraries.insert(
        gears_library.meta_data.name.to_string(),
        GearsLibrary {
            gears_lib_ctx: gears_library,
            _lib_ctx: lib_ctx,
            compile_lib_internals: compile_lib_internals,
        },
    );
    Ok(RedisValue::SimpleStringStatic("OK"))
}

fn function_load_command(
    ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let mut upgrade = false;
    let lib_code = loop {
        let arg = args.next_arg();
        if arg.is_err() {
            break Err(RedisError::Str("Could not find library payload"));
        }
        let arg = arg.unwrap();
        let arg_str = match arg.try_as_str() {
            Ok(arg) => arg,
            Err(_) => break Ok(arg),
        };
        let arg_str = arg_str.to_lowercase();
        match arg_str.as_ref() {
            "upgrade" => upgrade = true,
            _ => break Ok(arg),
        }
    }?;
    let lib_code_slice = match lib_code.try_as_str() {
        Ok(s) => s,
        Err(_) => return Err(RedisError::Str("lib code must a valid string")),
    };
    match function_load_intrernal(lib_code_slice, upgrade) {
        Ok(r) => {
            ctx.replicate_verbatim();
            Ok(r)
        }
        Err(e) => Err(e),
    }
}

fn function_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let sub_command = args.next_arg()?.try_as_str()?.to_lowercase();
    match sub_command.as_ref() {
        "load" => function_load_command(ctx, args),
        "call" => function_call_command(ctx, args),
        "list" => function_list_command(ctx, args),
        "del" => function_del_command(ctx, args),
        "debug" => function_debug_command(ctx, args),
        _ => Err(RedisError::String(format!(
            "Unknown subcommand {}",
            sub_command
        ))),
    }
}

fn on_stream_touched(_ctx: &Context, _event_type: NotifyEvent, event: &str, key: &str) {
    if get_ctx().is_primary() {
        let stream_ctx = &mut get_globals_mut().stream_ctx;
        stream_ctx.on_stream_touched(event, key);
    }
}

fn generic_notification(_ctx: &Context, _event_type: NotifyEvent, event: &str, key: &str) {
    if event == "del" {
        let stream_ctx = &mut get_globals_mut().stream_ctx;
        stream_ctx.on_stream_deleted(event, key);
    }
}

fn key_space_notification(_ctx: &Context, _event_type: NotifyEvent, event: &str, key: &str) {
    get_globals().notifications_ctx.on_key_touched(event, key)
}

fn update_stream_last_read_id(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let library_name = args.next_arg()?.try_as_str()?;
    let stream_consumer = args.next_arg()?.try_as_str()?;
    let stream = args.next_arg()?.try_as_str()?;
    let ms = args.next_arg()?.try_as_str()?.parse::<u64>()?;
    let seq = args.next_arg()?.try_as_str()?.parse::<u64>()?;
    let library = get_libraries().get(library_name);
    if library.is_none() {
        return Err(RedisError::String(format!(
            "No such library '{}'",
            library_name
        )));
    }
    let library = library.unwrap();
    let consumer = library.gears_lib_ctx.stream_consumers.get(stream_consumer);
    if consumer.is_none() {
        return Err(RedisError::String(format!(
            "No such consumer '{}'",
            stream_consumer
        )));
    }
    let consumer = consumer.unwrap();
    get_globals_mut()
        .stream_ctx
        .update_stream_for_consumer(stream, consumer, ms, seq);
    ctx.replicate_verbatim();
    Ok(RedisValue::SimpleStringStatic("OK"))
}

fn scan_key_space_for_streams() {
    get_globals().mgmt_pool.execute(|| {
        let cursor = KeysCursor::new();
        let ctx = get_ctx();
        let thread_ctx = ThreadSafeContext::new();
        let mut _gaurd = Some(thread_ctx.lock());
        while cursor.scan(ctx, &|ctx, key_name, key| {
            let key_type = match key {
                Some(k) => k.key_type(),
                None => ctx.open_key(&key_name).key_type(),
            };
            if key_type == Stream {
                let key_name_str = key_name.try_as_str();
                match key_name_str {
                    Ok(key) => get_globals_mut()
                        .stream_ctx
                        .on_stream_touched("created", key),
                    Err(_) => {}
                }
            }
        }) {
            _gaurd = None; // will release the lock
            _gaurd = Some(thread_ctx.lock());
        }
    })
}

fn on_role_changed(ctx: &Context, event_data: ServerEventData) {
    match event_data {
        ServerEventData::RoleChangedEvent(role_changed) => {
            if let ServerRole::Primary = role_changed.role {
                ctx.log_notice(
                    "Role changed to primary, initializing key scan to search for streams.",
                );
                scan_key_space_for_streams();
            }
        }
        _ => panic!("got unexpected sub event"),
    }
}

fn on_loading_event(ctx: &Context, event_data: ServerEventData) {
    match event_data {
        ServerEventData::LoadingEvent(loading_sub_event) => {
            match loading_sub_event {
                LoadingSubevent::RdbStarted
                | LoadingSubevent::AofStarted
                | LoadingSubevent::ReplStarted => {
                    // clean the entire functions data
                    ctx.log_notice("Got a loading start event, clear the entire functions data.");
                    let globals = get_globals_mut();
                    globals.libraries.clear();
                    globals.stream_ctx.clear();
                }
                _ => {}
            }
        }
        _ => panic!("got unexpected sub event"),
    }
}

fn on_flush_event(ctx: &Context, event_data: ServerEventData) {
    match event_data {
        ServerEventData::FlushEvent(loading_sub_event) => match loading_sub_event {
            FlushSubevent::Started => {
                ctx.log_notice("Got a flush started event");
                let globals = get_globals_mut();
                for lib in globals.libraries.values() {
                    for consumer in lib.gears_lib_ctx.stream_consumers.values() {
                        let mut c = consumer.ref_cell.borrow_mut();
                        c.clear_streams_info();
                    }
                }
                globals.stream_ctx.clear_tracked_streams();
            }
            _ => {}
        },
        _ => panic!("got unexpected sub event"),
    }
}

// fn on_command_called(_ctx: &Context, _event_data: ServerEventData) {
//     // let res = ctx.call("ping", &[]);
//     // println!("{:?}", res);
// }

redis_module! {
    name: "redisgears_2",
    version: 999999,
    data_types: [REDIS_GEARS_TYPE],
    init: js_init,
    info: js_info,
    commands: [
        ["rg.function", function_command, "readonly", 0,0,0],
        ["_rg_internals.update_stream_last_read_id", update_stream_last_read_id, "readonly", 0,0,0],
    ],
    event_handlers: [
        [@STREAM: on_stream_touched],
        [@GENERIC: generic_notification],
        [@ALL @MISSED: key_space_notification],
    ],
    server_events: [
        [@RuleChanged: on_role_changed],
        [@Loading: on_loading_event],
        [@Flush: on_flush_event],
        // [@Command: on_command_called],
    ]
}

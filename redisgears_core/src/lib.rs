extern crate redis_module;

use threadpool::ThreadPool;

use redis_module::raw::RedisModule_GetDetachedThreadSafeContext;

use redis_module::{
    context::thread_safe::ContextGuard, redis_command, redis_module, BlockedClient, Context,
    InfoContext, NextArg, RedisError, RedisResult, RedisString, RedisValue, Status,
    ThreadSafeContext,
};

use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtx, function_ctx::FunctionCtx, load_library_ctx::LibraryCtx,
    load_library_ctx::LoadLibraryCtx, run_function_ctx::BackgroundRunFunctionCtx,
    run_function_ctx::BackgroundRunScopeGuard, run_function_ctx::RunFunctionCtx, CallResult,
    GearsApiError,
};

use libloading::{Library, Symbol};

use std::collections::HashMap;

use std::iter::Skip;
use std::slice::Iter;
use std::vec::IntoIter;

struct BackgroundRunScopeGuardCtx<'a> {
    _ctx_guard: ContextGuard,
    ctx: &'a Context,
}

struct BackgroundRunCtx {
    thread_ctx: ThreadSafeContext<BlockedClient>,
    ctx: Context,
}

struct RunCtx<'a> {
    ctx: &'a Context,
    iter: Iter<'a, redis_module::RedisString>,
}

struct GearsLibraryMataData {
    name: String,
    engine: String,
    code: String,
}

struct GearsLibraryCtx {
    functions: HashMap<String, Box<dyn FunctionCtx>>,
}

struct GearsLibrary {
    meta_data: GearsLibraryMataData,
    gears_lib_ctx: GearsLibraryCtx,
    lib_ctx: Box<dyn LibraryCtx>,
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
        _ => panic!("not yet implemented"),
    }
}

impl<'a> BackgroundRunScopeGuard for BackgroundRunScopeGuardCtx<'a> {
    fn call(&self, command: &str, args: &[&str]) -> CallResult {
        let res = self.ctx.call(command, args);
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
}

unsafe impl Send for BackgroundRunCtx {}

impl BackgroundRunFunctionCtx for BackgroundRunCtx {
    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }

    fn lock<'a>(&'a self) -> Box<dyn BackgroundRunScopeGuard + 'a> {
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

impl<'a> RunFunctionCtx for RunCtx<'a> {
    fn next_arg(&mut self) -> Option<&[u8]> {
        Some(self.iter.next()?.as_slice())
    }

    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }

    fn call(&self, command: &str, args: &[&str]) -> CallResult {
        let redis_ctx = get_ctx();
        let res = redis_ctx.call(command, args);
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

    fn go_to_backgrond(&self, func: Box<dyn FnOnce(Box<dyn BackgroundRunFunctionCtx>) + Send>) {
        let blocked_client = self.ctx.block_client();
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        let ctx = thread_ctx.get_ctx();
        let background_run_ctx = Box::new(BackgroundRunCtx {
            thread_ctx: thread_ctx,
            ctx: ctx,
        });
        get_thread_pool().execute(move || {
            func(background_run_ctx);
        });
    }
}

impl LoadLibraryCtx for GearsLibraryCtx {
    fn register_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtx>,
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

    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }
}

struct GlobalCtx {
    libraries: HashMap<String, GearsLibrary>,
    backends: HashMap<String, Box<dyn BackendCtx>>,
    redis_ctx: Context,
    plugins: Vec<Library>,
    pool: ThreadPool,
}

static mut GLOBALS: Option<GlobalCtx> = None;

fn get_globals() -> &'static GlobalCtx {
    unsafe { GLOBALS.as_ref().unwrap() }
}

fn get_globals_mut() -> &'static mut GlobalCtx {
    unsafe { GLOBALS.as_mut().unwrap() }
}

fn get_ctx() -> &'static Context {
    &get_globals().redis_ctx
}

fn get_backends() -> &'static HashMap<String, Box<dyn BackendCtx>> {
    &get_globals().backends
}

fn get_backends_mut() -> &'static mut HashMap<String, Box<dyn BackendCtx>> {
    &mut get_globals_mut().backends
}

fn get_libraries() -> &'static HashMap<String, GearsLibrary> {
    &get_globals().libraries
}

fn get_libraries_mut() -> &'static mut HashMap<String, GearsLibrary> {
    &mut get_globals_mut().libraries
}

fn get_thread_pool() -> &'static ThreadPool {
    &get_globals().pool
}

fn js_init(ctx: &Context, args: &Vec<RedisString>) -> Status {
    unsafe {
        let inner_ctx = RedisModule_GetDetachedThreadSafeContext.unwrap()(ctx.ctx);
        let mut global_ctx = GlobalCtx {
            libraries: HashMap::new(),
            redis_ctx: Context::new(inner_ctx),
            backends: HashMap::new(),
            plugins: Vec::new(),
            pool: ThreadPool::new(1),
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
        let lib = Library::new(v8_path).unwrap();
        {
            let func: Symbol<unsafe fn() -> *mut dyn BackendCtx> =
                lib.get(b"initialize_plugin").unwrap();
            let backend = Box::from_raw(func());
            let name = backend.get_name();
            ctx.log_notice(&format!("registering backend: {}", name));
            if global_ctx.backends.contains_key(name) {
                ctx.log_warning(&format!("Backend {} already exists", name));
                return Status::Err;
            }
            if let Err(e) = backend.initialize(&redis_module::ALLOC) {
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
    _ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let name = args
        .next()
        .map_or(Err(RedisError::Str("function name was not given")), |s| {
            s.try_as_str()
        })?;
    let libraries = get_libraries_mut();
    match libraries.remove(name) {
        Some(_) => Ok(RedisValue::SimpleStringStatic("OK")),
        None => Err(RedisError::Str("library does not exists")),
    }
}

fn function_list_command(
    _ctx: &Context,
    mut args: Skip<IntoIter<redis_module::RedisString>>,
) -> RedisResult {
    let mut with_code = false;
    let mut lib = None;
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
                    if l.meta_data.name == lib_name {
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
                    RedisValue::BulkString(l.meta_data.engine.to_string()),
                    RedisValue::BulkString("name".to_string()),
                    RedisValue::BulkString(l.meta_data.name.to_string()),
                    RedisValue::BulkString("functions".to_string()),
                    RedisValue::Array(
                        l.gears_lib_ctx
                            .functions
                            .keys()
                            .map(|k| RedisValue::BulkString(k.to_string()))
                            .collect::<Vec<RedisValue>>(),
                    ),
                ];
                if with_code {
                    res.push(RedisValue::BulkString("code".to_string()));
                    res.push(RedisValue::BulkString(l.meta_data.code.to_string()));
                }
                RedisValue::Array(res)
            })
            .collect::<Vec<RedisValue>>(),
    ))
}

fn function_load_command(
    _ctx: &Context,
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
    let meta_data = library_extract_matadata(lib_code_slice)?;
    let backend_name = meta_data.engine.as_str();
    let backend = get_backends().get(backend_name);
    if backend.is_none() {
        return Err(RedisError::String(format!(
            "Unknown backend {}",
            backend_name
        )));
    }
    let backend = backend.unwrap();
    let lib_ctx = backend.compile_library(lib_code_slice);
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
    let old_lib = libraries.get(&meta_data.name);
    if old_lib.is_some() && !upgrade {
        return Err(RedisError::String(format!(
            "Library {} already exists",
            &meta_data.name
        )));
    }
    let mut gears_library = GearsLibraryCtx {
        functions: HashMap::new(),
    };
    let res = lib_ctx.load_library(&mut gears_library);
    if let Err(err) = res {
        let ret = match err {
            GearsApiError::Msg(s) => {
                let msg = format!("Failed loading library, {}", s);
                Err(RedisError::String(msg))
            }
        };
        return ret;
    }
    if gears_library.functions.len() == 0 {
        return Err(RedisError::Str("No function was registered"));
    }
    libraries.insert(
        meta_data.name.to_string(),
        GearsLibrary {
            meta_data: meta_data,
            gears_lib_ctx: gears_library,
            lib_ctx: lib_ctx,
        },
    );
    Ok(RedisValue::SimpleStringStatic("OK"))
}

fn function_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let sub_command = args.next_arg()?.try_as_str()?.to_lowercase();
    match sub_command.as_ref() {
        "load" => function_load_command(ctx, args),
        "call" => function_call_command(ctx, args),
        "list" => function_list_command(ctx, args),
        "del" => function_del_command(ctx, args),
        _ => Err(RedisError::String(format!(
            "Unknown subcommand {}",
            sub_command
        ))),
    }
}

redis_module! {
    name: "redisgears_2",
    version: 999999,
    data_types: [],
    init: js_init,
    info: js_info,
    commands: [
        ["rg.function", function_command, "readonly", 0,0,0],
    ],
}

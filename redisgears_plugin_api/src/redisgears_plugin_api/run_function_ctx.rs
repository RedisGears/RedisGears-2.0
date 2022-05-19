use crate::redisgears_plugin_api::CallResult;

pub trait BackgroundRunScopeGuardInterface {
    fn call(&self, command: &str, args: &[&str]) -> CallResult;
}

pub trait BackgroundRunFunctionCtxInterface: Send {
    fn log(&self, msg: &str);
    fn lock<'a>(&'a self) -> Box<dyn BackgroundRunScopeGuardInterface + 'a>;
    fn reply_with_simple_string(&self, val: &str);
    fn reply_with_error(&self, val: &str);
    fn reply_with_long(&self, val: i64);
    fn reply_with_double(&self, val: f64);
    fn reply_with_bulk_string(&self, val: &str);
    fn reply_with_array(&self, size: usize);
}

pub trait RunFunctionCtxInterface {
    fn next_arg<'a>(&'a mut self) -> Option<&'a [u8]>;
    fn log(&self, msg: &str);
    fn call(&self, command: &str, args: &[&str]) -> CallResult;
    fn reply_with_simple_string(&self, val: &str);
    fn reply_with_error(&self, val: &str);
    fn reply_with_long(&self, val: i64);
    fn reply_with_double(&self, val: f64);
    fn reply_with_bulk_string(&self, val: &str);
    fn reply_with_array(&self, size: usize);
    fn get_background_ctx(&self) -> Box<dyn BackgroundRunFunctionCtxInterface>;
    fn run_on_backgrond(&self, func: Box<dyn FnOnce() + Send>);
}

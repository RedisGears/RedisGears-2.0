use crate::redisgears_plugin_api::CallResult;

pub trait RedisLogerCtxInterface {
    fn log(&self, msg: &str);
}

pub trait RedisClientCtxInterface: RedisLogerCtxInterface + Send + Sync {
    fn call(&self, command: &str, args: &[&str]) -> CallResult;
    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface>;
    fn as_redis_client(&self) -> &dyn RedisClientCtxInterface;
}

pub trait ReplyCtxInterface: Send + Sync {
    fn reply_with_simple_string(&self, val: &str);
    fn reply_with_error(&self, val: &str);
    fn reply_with_long(&self, val: i64);
    fn reply_with_double(&self, val: f64);
    fn reply_with_bulk_string(&self, val: &str);
    fn reply_with_array(&self, size: usize);
    fn as_client(&self) -> &dyn ReplyCtxInterface;
}

pub trait BackgroundRunFunctionCtxInterface: RedisLogerCtxInterface + Send + Sync {
    fn lock<'a>(&'a self) -> Box<dyn RedisClientCtxInterface>;
}

pub trait RunFunctionCtxInterface: ReplyCtxInterface {
    fn next_arg<'a>(&'a mut self) -> Option<&'a [u8]>;
    fn get_background_client(&self) -> Box<dyn ReplyCtxInterface>;
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface>;
}

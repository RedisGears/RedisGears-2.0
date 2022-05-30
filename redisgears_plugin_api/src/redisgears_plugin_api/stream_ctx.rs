use crate::redisgears_plugin_api::run_function_ctx::RedisClientCtxInterface;
use crate::redisgears_plugin_api::run_function_ctx::BackgroundRunFunctionCtxInterface;
use crate::redisgears_plugin_api::run_function_ctx::RedisLogerCtxInterface;

pub trait StreamProcessCtxInterface: RedisLogerCtxInterface {
    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface>;
    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface>;
    fn go_to_backgrond(
        &self,
        func: Box<dyn FnOnce() + Send>,
    );
}

pub trait StreamRecordInterface {
    fn get_id(&self) -> (u64, u64);
    fn fields<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a [u8], &'a [u8])> + 'a>;
}

pub enum StreamRecordAck {
    Ack,
    Nack(String),
}

pub trait StreamCtxInterface {
    fn process_record(
        &self,
        stream_name: &str,
        record: Box<dyn StreamRecordInterface + Send>,
        run_ctx: &dyn StreamProcessCtxInterface,
        ack_callback: Box<dyn FnOnce(StreamRecordAck) + Send>,
    ) -> Option<StreamRecordAck>;
}

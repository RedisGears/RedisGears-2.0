use crate::redisgears_plugin_api::CallResult;

pub trait BackgroundStreamScopeGuardInterface {
    fn call(&self, command: &str, args: &[&str]) -> CallResult;
}

pub trait BackgroundStreamProcessCtxInterface {
    fn log(&self, msg: &str);
    fn lock<'a>(&'a self) -> Box<dyn BackgroundStreamScopeGuardInterface + 'a>;
}

pub trait StreamProcessCtxInterface {
    fn log(&self, msg: &str);
    fn call(&self, command: &str, args: &[&str]) -> CallResult;
    fn go_to_backgrond(
        &self,
        func: Box<dyn FnOnce(Box<dyn BackgroundStreamProcessCtxInterface>) + Send>,
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

use redisgears_plugin_api::redisgears_plugin_api::{
    stream_ctx::BackgroundStreamProcessCtxInterface,
    stream_ctx::BackgroundStreamScopeGuardInterface, stream_ctx::StreamCtxInterface,
    stream_ctx::StreamProcessCtxInterface, stream_ctx::StreamRecordAck,
    stream_ctx::StreamRecordInterface, CallResult,
};

use redis_module::{
    context::thread_safe::ContextGuard, raw::RedisModuleStreamID, stream::StreamRecord,
    DetachedFromClient, RedisError, ThreadSafeContext,
};

use crate::{get_ctx, get_thread_pool, redis_value_to_call_reply};

use crate::stream_reader::{StreamConsumer, StreamReaderAck};

pub(crate) struct BackgroundStreamScopeGuard {
    _gaurd: ContextGuard,
}

impl BackgroundStreamScopeGuardInterface for BackgroundStreamScopeGuard {
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
}

pub(crate) struct BackgroundStreamProcessCtx {
    ctx: ThreadSafeContext<DetachedFromClient>,
}

impl BackgroundStreamProcessCtxInterface for BackgroundStreamProcessCtx {
    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }

    fn lock<'a>(&'a self) -> Box<dyn BackgroundStreamScopeGuardInterface + 'a> {
        let gaurd = self.ctx.lock();
        Box::new(BackgroundStreamScopeGuard { _gaurd: gaurd })
    }
}

pub(crate) struct StreamRunCtx;

impl StreamProcessCtxInterface for StreamRunCtx {
    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }

    fn call(&self, _command: &str, _args: &[&str]) -> CallResult {
        CallResult::Error("Not yet implemented".to_string())
    }

    fn go_to_backgrond(
        &self,
        func: Box<dyn FnOnce(Box<dyn BackgroundStreamProcessCtxInterface>) + Send>,
    ) {
        get_thread_pool().execute(move || {
            let ctx = ThreadSafeContext::new();
            func(Box::new(BackgroundStreamProcessCtx { ctx: ctx }));
        });
    }
}

pub(crate) struct GearsStreamRecord {
    pub(crate) record: StreamRecord,
}

unsafe impl Sync for GearsStreamRecord {}
unsafe impl Send for GearsStreamRecord {}

impl crate::stream_reader::StreamReaderRecord for GearsStreamRecord {
    fn get_id(&self) -> RedisModuleStreamID {
        self.record.id
    }
}

impl StreamRecordInterface for GearsStreamRecord {
    fn get_id(&self) -> (u64, u64) {
        (self.record.id.ms, self.record.id.seq)
    }

    fn fields<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a [u8], &'a [u8])> + 'a> {
        let res = self
            .record
            .fields
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect::<Vec<(&'a [u8], &'a [u8])>>();
        Box::new(res.into_iter())
    }
}

pub(crate) struct GearsStreamConsumer {
    pub(crate) ctx: Box<dyn StreamCtxInterface>,
}

impl StreamConsumer<GearsStreamRecord> for GearsStreamConsumer {
    fn new_data(
        &self,
        stream_name: &str,
        record: GearsStreamRecord,
        ack_callback: Box<dyn FnOnce(StreamReaderAck) + Send>,
    ) -> Option<StreamReaderAck> {
        let res = self.ctx.process_record(
            stream_name,
            Box::new(record),
            &mut StreamRunCtx,
            Box::new(|ack| {
                // here we must take the redis lock
                let ctx = ThreadSafeContext::new();
                let _gaurd = ctx.lock();
                ack_callback(match ack {
                    StreamRecordAck::Ack => StreamReaderAck::Ack,
                    StreamRecordAck::Nack(msg) => StreamReaderAck::Nack(msg),
                })
            }),
        );
        res.map_or(None, |r| {
            Some(match r {
                StreamRecordAck::Ack => StreamReaderAck::Ack,
                StreamRecordAck::Nack(msg) => StreamReaderAck::Nack(msg),
            })
        })
    }
}

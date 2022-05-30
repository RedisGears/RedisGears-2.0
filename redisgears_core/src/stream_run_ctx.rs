use redisgears_plugin_api::redisgears_plugin_api::{
    stream_ctx::StreamCtxInterface,
    stream_ctx::StreamProcessCtxInterface, stream_ctx::StreamRecordAck,
    stream_ctx::StreamRecordInterface,
    run_function_ctx::RedisLogerCtxInterface,
    run_function_ctx::RedisClientCtxInterface,
    run_function_ctx::BackgroundRunFunctionCtxInterface,

};

use redis_module::{
    raw::RedisModuleStreamID, stream::StreamRecord,
    ThreadSafeContext,
};

use crate::{get_ctx, get_thread_pool, run_ctx::RedisClient, background_run_ctx::BackgroundRunCtx};

use crate::stream_reader::{StreamConsumer, StreamReaderAck};

pub(crate) struct StreamRunCtx;

impl RedisLogerCtxInterface for StreamRunCtx {
    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }
}

impl StreamProcessCtxInterface for StreamRunCtx {
    

    fn get_redis_client(&self) -> Box<dyn RedisClientCtxInterface> {
        Box::new(RedisClient{})
    }

    fn get_background_redis_client(&self) -> Box<dyn BackgroundRunFunctionCtxInterface> {
        Box::new(BackgroundRunCtx {})
    }

    fn go_to_backgrond(
        &self,
        func: Box<dyn FnOnce() + Send>,
    ) {
        get_thread_pool().execute(move || {
            func();
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

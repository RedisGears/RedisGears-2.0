use v8_rs::v8::{
    isolate::V8Isolate, v8_context::V8Context, v8_value::V8LocalValue, v8_value::V8PersistValue,
};

use redisgears_plugin_api::redisgears_plugin_api::stream_ctx::{
    StreamCtxInterface, StreamProcessCtxInterface, StreamRecordAck, StreamRecordInterface,
};

use crate::v8_native_functions::ExecutionCtx;

use std::sync::Arc;

use std::str;

struct V8StreamCtxInternals {
    persisted_function: V8PersistValue,
    ctx: Arc<V8Context>,
    isolate: Arc<V8Isolate>,
}

pub struct V8StreamCtx {
    internals: Arc<V8StreamCtxInternals>,
    is_async: bool,
}

impl V8StreamCtx {
    pub(crate) fn new(
        ctx: &Arc<V8Context>,
        isolate: &Arc<V8Isolate>,
        persisted_function: V8PersistValue,
        is_async: bool,
    ) -> V8StreamCtx {
        V8StreamCtx {
            internals: Arc::new(V8StreamCtxInternals {
                ctx: Arc::clone(ctx),
                isolate: Arc::clone(isolate),
                persisted_function: persisted_function,
            }),
            is_async: is_async,
        }
    }
}

impl V8StreamCtxInternals {
    fn process_record_internal(
        &self,
        stream_name: &str,
        record: Box<dyn StreamRecordInterface>,
        execution_ctx: ExecutionCtx,
    ) -> Option<StreamRecordAck> {
        let _isolate_scope = self.isolate.enter();
        let _handlers_scope = self.isolate.new_handlers_scope();
        let ctx_scope = self.ctx.enter();
        let trycatch = self.isolate.new_try_catch();

        let id = record.get_id();
        let id_v8_arr = self.isolate.new_array(&[
            &self.isolate.new_long(id.0 as i64),
            &self.isolate.new_long(id.1 as i64),
        ]);
        let stream_name_v8_str = self.isolate.new_string(stream_name);

        let vals = record
            .fields()
            .map(|(f, v)| (str::from_utf8(f), str::from_utf8(v)))
            .filter(|(f, v)| {
                if f.is_err() || v.is_err() {
                    false
                } else {
                    true
                }
            })
            .map(|(f, v)| (f.unwrap(), v.unwrap()))
            .map(|(f, v)| {
                self.isolate
                    .new_array(&[
                        &self.isolate.new_string(f).to_value(),
                        &self.isolate.new_string(v).to_value(),
                    ])
                    .to_value()
            })
            .collect::<Vec<V8LocalValue>>();

        let val_v8_arr = self
            .isolate
            .new_array(&vals.iter().collect::<Vec<&V8LocalValue>>());

        let stream_data = self.isolate.new_object();
        stream_data.set(
            &ctx_scope,
            &self.isolate.new_string("id").to_value(),
            &id_v8_arr.to_value(),
        );
        stream_data.set(
            &ctx_scope,
            &self.isolate.new_string("stream_name").to_value(),
            &stream_name_v8_str.to_value(),
        );
        stream_data.set(
            &ctx_scope,
            &self.isolate.new_string("record").to_value(),
            &val_v8_arr.to_value(),
        );

        self.ctx.set_private_data(0, Some(&execution_ctx));
        let res = self
            .persisted_function
            .as_local(self.isolate.as_ref())
            .call(&ctx_scope, Some(&[&stream_data.to_value()]));
        self.ctx.set_private_data::<ExecutionCtx>(0, None);

        Some(match res {
            Some(_) => StreamRecordAck::Ack,
            None => {
                let error_utf8 = trycatch.get_exception().to_utf8(&self.isolate).unwrap();
                StreamRecordAck::Nack(error_utf8.as_str().to_string())
            }
        })
    }
}

impl StreamCtxInterface for V8StreamCtx {
    fn process_record(
        &self,
        stream_name: &str,
        record: Box<dyn StreamRecordInterface + Send>,
        run_ctx: &dyn StreamProcessCtxInterface,
        ack_callback: Box<dyn FnOnce(StreamRecordAck) + Send>,
    ) -> Option<StreamRecordAck> {
        if self.is_async {
            let internals = Arc::clone(&self.internals);
            let stream_name = stream_name.to_string();
            run_ctx.go_to_backgrond(Box::new(move |background_ctx| {
                let stream_run_ctx =
                    ExecutionCtx::BackgroundStreamProcessing(background_ctx.as_ref());
                let res = internals.process_record_internal(
                    &stream_name.to_string(),
                    record,
                    stream_run_ctx,
                );
                ack_callback(res.unwrap());
            }));
            None
        } else {
            let stream_run_ctx = ExecutionCtx::StreamProcessing(run_ctx);
            self.internals
                .process_record_internal(stream_name, record, stream_run_ctx)
        }
    }
}

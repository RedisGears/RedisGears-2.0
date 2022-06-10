use crate::{get_ctx, get_thread_pool};
use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::CompiledLibraryInterface;

pub(crate) struct CompiledLibraryAPI;

impl CompiledLibraryInterface for CompiledLibraryAPI {
    fn log(&self, msg: &str) {
        get_ctx().log_notice(msg);
    }

    fn run_on_background(&self, job: Box<dyn FnOnce() + Send>) {
        get_thread_pool().execute(job);
    }
}

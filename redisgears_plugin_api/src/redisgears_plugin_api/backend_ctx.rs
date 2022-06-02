use crate::redisgears_plugin_api::load_library_ctx::LibraryCtxInterface;
use crate::redisgears_plugin_api::CallResult;
use crate::redisgears_plugin_api::GearsApiError;
use std::alloc::GlobalAlloc;

pub trait BackendCtxInterface {
    fn get_name(&self) -> &'static str;
    fn initialize(
        &self,
        allocator: &'static dyn GlobalAlloc,
        log: Box<dyn Fn(&str) + 'static>,
    ) -> Result<(), GearsApiError>;
    fn compile_library(
        &mut self,
        code: &str,
        run_on_background: Box<dyn Fn(Box<dyn FnOnce() + Send>) + Send + Sync>,
        log: Box<dyn Fn(&str) + Send + Sync>,
    ) -> Result<Box<dyn LibraryCtxInterface>, GearsApiError>;
    fn debug(&mut self, args: &[&str]) -> Result<CallResult, GearsApiError>;
}

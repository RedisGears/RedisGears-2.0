use crate::redisgears_plugin_api::load_library_ctx::LibraryCtx;
use crate::redisgears_plugin_api::GearsApiError;
use std::alloc::GlobalAlloc;

pub trait BackendCtx {
    fn get_name(&self) -> &'static str;
    fn initialize(&self, allocator: &'static dyn GlobalAlloc) -> Result<(), GearsApiError>;
    fn compile_library(&self, code: &str) -> Result<Box<dyn LibraryCtx>, GearsApiError>;
}

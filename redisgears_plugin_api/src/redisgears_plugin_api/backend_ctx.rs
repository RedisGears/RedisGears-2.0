use crate::redisgears_plugin_api::load_library_ctx::LibraryCtxInterface;
use crate::redisgears_plugin_api::CallResult;
use crate::redisgears_plugin_api::GearsApiError;
use std::alloc::GlobalAlloc;

pub trait CompiledLibraryInterface {
    fn log(&self, msg: &str);
    fn run_on_background(&self, job: Box<dyn FnOnce() + Send>);
}

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
        compiled_library_api: Box<dyn CompiledLibraryInterface + Send + Sync>,
    ) -> Result<Box<dyn LibraryCtxInterface>, GearsApiError>;
    fn debug(&mut self, args: &[&str]) -> Result<CallResult, GearsApiError>;
}

use crate::redisgears_plugin_api::function_ctx::FunctionCtx;
use crate::redisgears_plugin_api::GearsApiError;

pub trait LibraryCtx {
    fn load_library(&self, load_library_ctx: &mut dyn LoadLibraryCtx) -> Result<(), GearsApiError>;
}

pub trait LoadLibraryCtx {
    fn register_function(
        &mut self,
        name: &str,
        function_ctx: Box<dyn FunctionCtx>,
    ) -> Result<(), GearsApiError>;
    fn log(&self, msg: &str);
}

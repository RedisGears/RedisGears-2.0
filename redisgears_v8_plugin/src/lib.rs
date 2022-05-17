use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::BackendCtx;

mod v8_backend;
mod v8_function_ctx;
mod v8_native_functions;
mod v8_script_ctx;

use crate::v8_backend::V8Backend;

#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn initialize_plugin() -> *mut dyn BackendCtx {
    Box::into_raw(Box::new(V8Backend))
}

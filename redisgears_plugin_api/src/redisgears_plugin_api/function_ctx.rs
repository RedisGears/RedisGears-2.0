use crate::redisgears_plugin_api::run_function_ctx::RunFunctionCtx;

pub enum FunctionCallResult {
    Done,
    Hold,
}

pub trait FunctionCtx {
    fn call(&self, run_ctx: &mut dyn RunFunctionCtx) -> FunctionCallResult;
}

pub mod backend_ctx;
pub mod function_ctx;
pub mod load_library_ctx;
pub mod run_function_ctx;

pub enum GearsApiError {
    Msg(String),
}

impl GearsApiError {
    pub fn get_msg(&self) -> &str {
        match self {
            GearsApiError::Msg(s) => &s,
        }
    }
}

pub enum CallResult {
    Error(String),
    SimpleStr(String),
    BulkStr(String),
    Long(i64),
    Double(f64),
    Array(Vec<CallResult>),
    Null,
}

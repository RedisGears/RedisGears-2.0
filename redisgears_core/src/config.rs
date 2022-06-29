use redis_module::context::configuration::{
    ConfigFlags, RedisConfigCtx, RedisNumberConfigCtx, RedisStringConfigCtx, RedisEnumConfigCtx,
};
use redis_module::RedisString;

use redis_module::context::Context;
use redis_module::RedisError;

use redisgears_plugin_api::redisgears_plugin_api::backend_ctx::LibraryFatalFailurePolicy;

pub(crate) struct ExecutionThreads {
    pub(crate) size: usize,
    flags: ConfigFlags,
}

impl ExecutionThreads {
    fn new() -> ExecutionThreads {
        ExecutionThreads {
            size: 1,
            flags: ConfigFlags::new().emmutable(),
        }
    }
}

impl RedisConfigCtx for ExecutionThreads {
    fn name(&self) -> &'static str {
        "execution-threads"
    }

    fn apply(&self, _ctx: &Context) -> Result<(), RedisError> {
        Ok(())
    }

    fn flags(&self) -> &ConfigFlags {
        &self.flags
    }
}

impl RedisNumberConfigCtx for ExecutionThreads {
    fn default(&self) -> i64 {
        1
    }

    fn min(&self) -> i64 {
        1
    }
    fn max(&self) -> i64 {
        32
    }

    fn get(&self, _name: &str) -> i64 {
        self.size as i64
    }

    fn set(&mut self, _name: &str, value: i64) -> Result<(), RedisError> {
        self.size = value as usize;
        Ok(())
    }
}

pub(crate) struct LibraryMaxMemory {
    pub(crate) size: usize,
    flags: ConfigFlags,
}

impl LibraryMaxMemory {
    fn new() -> LibraryMaxMemory {
        LibraryMaxMemory {
            size: 1024 * 1024 * 1024, // 1G
            flags: ConfigFlags::new().emmutable().memory(),
        }
    }
}

impl RedisConfigCtx for LibraryMaxMemory {
    fn name(&self) -> &'static str {
        "library-maxmemory"
    }

    fn apply(&self, _ctx: &Context) -> Result<(), RedisError> {
        Ok(())
    }

    fn flags(&self) -> &ConfigFlags {
        &self.flags
    }
}

impl RedisNumberConfigCtx for LibraryMaxMemory {
    fn default(&self) -> i64 {
        1024 * 1024 * 1024 // 1G
    }

    fn min(&self) -> i64 {
        16 * 1024 * 1024 // 16M
    }
    fn max(&self) -> i64 {
        2 * 1024 * 1024 * 1024 // 2G
    }

    fn get(&self, _name: &str) -> i64 {
        self.size as i64
    }

    fn set(&mut self, _name: &str, value: i64) -> Result<(), RedisError> {
        self.size = value as usize;
        Ok(())
    }
}

pub(crate) struct GearBoxAddress {
    pub(crate) address: String,
    flags: ConfigFlags,
}

impl GearBoxAddress {
    fn new() -> GearBoxAddress {
        GearBoxAddress {
            address: "http://localhost:3000".to_string(),
            flags: ConfigFlags::new(),
        }
    }
}

impl RedisConfigCtx for GearBoxAddress {
    fn name(&self) -> &'static str {
        "gearsbox-address"
    }

    fn apply(&self, _ctx: &Context) -> Result<(), RedisError> {
        Ok(())
    }

    fn flags(&self) -> &ConfigFlags {
        &self.flags
    }
}

impl RedisStringConfigCtx for GearBoxAddress {
    fn default(&self) -> Option<String> {
        Some("http://localhost:3000".to_string())
    }

    fn get(&self, _name: &str) -> RedisString {
        RedisString::create(std::ptr::null_mut(), &self.address)
    }

    fn set(&mut self, _name: &str, value: RedisString) -> Result<(), RedisError> {
        self.address = value.try_as_str().unwrap().to_string();
        Ok(())
    }
}

pub(crate) struct LibraryOnFatalFailurePolicy {
    pub(crate) policy:  LibraryFatalFailurePolicy,
    flags: ConfigFlags,
}

impl LibraryOnFatalFailurePolicy {
    fn new() -> LibraryOnFatalFailurePolicy {
        LibraryOnFatalFailurePolicy {
            policy: LibraryFatalFailurePolicy::Abort,
            flags: ConfigFlags::new(),
        }
    }
}

impl RedisConfigCtx for LibraryOnFatalFailurePolicy {
    fn name(&self) -> &'static str {
        "library-fatal_failure-policy"
    }

    fn apply(&self, _ctx: &Context) -> Result<(), RedisError> {
        Ok(())
    }

    fn flags(&self) -> &ConfigFlags {
        &self.flags
    }
}

impl RedisEnumConfigCtx for LibraryOnFatalFailurePolicy {
    fn default(&self) -> i32 {
        self.policy.clone() as i32
    }

    fn values(&self) -> Vec<(&str, i32)> {
        vec![
            ("abort", LibraryFatalFailurePolicy::Abort as i32),
            ("kill", LibraryFatalFailurePolicy::Kill as i32),
        ]
    }

    fn get(&self, _name: &str) -> i32 {
        self.policy.clone() as i32
    }

    fn set(&mut self, _name: &str, value: i32) -> Result<(), RedisError> {
        match value {
            x if x == LibraryFatalFailurePolicy::Abort as i32 => self.policy = LibraryFatalFailurePolicy::Abort,
            x if x == LibraryFatalFailurePolicy::Kill as i32 => self.policy = LibraryFatalFailurePolicy::Kill,
            _ => return Err(RedisError::Str("unsupported value were given")),
        }
        Ok(())
    }
}

pub(crate) struct LockRedisTimeout {
    pub(crate) size: u128,
    flags: ConfigFlags,
}

impl LockRedisTimeout {
    fn new() -> LockRedisTimeout {
        LockRedisTimeout {
            size: 1000,
            flags: ConfigFlags::new(),
        }
    }
}

impl RedisConfigCtx for LockRedisTimeout {
    fn name(&self) -> &'static str {
        "lock-redis-timeout"
    }

    fn apply(&self, _ctx: &Context) -> Result<(), RedisError> {
        Ok(())
    }

    fn flags(&self) -> &ConfigFlags {
        &self.flags
    }
}

impl RedisNumberConfigCtx for LockRedisTimeout {
    fn default(&self) -> i64 {
        500
    }

    fn min(&self) -> i64 {
        100
    }
    fn max(&self) -> i64 {
        1000000000
    }

    fn get(&self, _name: &str) -> i64 {
        self.size as i64
    }

    fn set(&mut self, _name: &str, value: i64) -> Result<(), RedisError> {
        self.size = value as u128;
        Ok(())
    }
}

pub(crate) struct Config {
    pub(crate) execution_threads: ExecutionThreads,
    pub(crate) library_maxmemory: LibraryMaxMemory,
    pub(crate) gears_box_address: GearBoxAddress,
    pub(crate) libraray_fatal_failure_policy: LibraryOnFatalFailurePolicy,
    pub(crate) lock_regis_timeout: LockRedisTimeout,
}

impl Config {
    pub(crate) fn new() -> Config {
        Config {
            execution_threads: ExecutionThreads::new(),
            library_maxmemory: LibraryMaxMemory::new(),
            gears_box_address: GearBoxAddress::new(),
            libraray_fatal_failure_policy: LibraryOnFatalFailurePolicy::new(),
            lock_regis_timeout: LockRedisTimeout::new(),
        }
    }
}

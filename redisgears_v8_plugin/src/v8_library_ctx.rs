use redisgears_plugin_api::redisgears_plugin_api::{
    GearsApiError,
    load_library_ctx::LibraryCtx,
    load_library_ctx::LoadLibraryCtx,
};

use v8_rs::v8::{
    isolate::V8Isolate,
    v8_context::V8Context,
    v8_module::V8PersistedModule,
    v8_promise::V8PromiseState,
};

use crate::v8_native_functions::{
    ExecutionCtx,
};

use serde_json::Value;
use std::io::Read;
use std::rc::Rc;
use std::path::PathBuf;

pub (crate) struct V8LibraryMataData {
    name: String,
    main_path: String,
    version: String,
    package_json: Value,
}

impl V8LibraryMataData {
    pub (crate) fn new(package_json_str: &str) -> Result<V8LibraryMataData, GearsApiError> {
        let package_value: Value = match serde_json::from_str(&package_json_str) {
            Ok(v) => v,
            Err(e) => return Err(GearsApiError::Msg(format!("Failed parsing 'package.json' file: {}", e))),
        };
        let package = match package_value.as_object() {
            Some(v) => v,
            None => return Err(GearsApiError::Msg(format!("Bad 'package.json' format"))),
        };
        let main_file_path = match package.get("main") {
            Some(v) => match v.as_str() {
                Some(v) => v,
                None => return Err(GearsApiError::Msg(format!("Bad 'package.json' format, main file value is not a string."))),
            }
            None => return Err(GearsApiError::Msg(format!("Bad 'package.json' format, no main file specified."))),
        };

        let name = match package.get("name") {
            Some(v) => match v.as_str() {
                Some(v) => v,
                None => return Err(GearsApiError::Msg(format!("Bad 'package.json' format, name value is not a string."))),
            }
            None => return Err(GearsApiError::Msg(format!("Bad 'package.json' format, no name specified."))),
        };

        let version = match package.get("version") {
            Some(v) => match v.as_str() {
                Some(v) => v,
                None => return Err(GearsApiError::Msg(format!("Bad 'package.json' format, version value is not a string."))),
            }
            None => return Err(GearsApiError::Msg(format!("Bad 'package.json' format, no version specified."))),
        };

        Ok(V8LibraryMataData{
            name: name.to_string(),
            version: version.to_string(),
            main_path: main_file_path.to_string(),
            package_json: package_value,
        })
    }

    pub (crate) fn get_name(& self) -> &str {
        &self.name
    }

    pub (crate) fn get_main_path(& self) -> &str {
        &self.main_path
    }

    pub (crate) fn get_main_dir(&self) -> Option<String> {
        let mut path = PathBuf::from(&self.main_path);
        path.pop();
        path.to_str().map_or(None, |s| Some(s.to_string()))
    }
}

pub (crate) struct V8LibraryDirMetaData {
    pub (crate) dir: Option<String>,
    pub (crate) file_name: String,
}

pub (crate) struct V8LibraryRawData {
    zip: zip::ZipArchive<std::io::Cursor<std::vec::Vec<u8>>>,
    raw_data: Vec<u8>,
}

impl V8LibraryRawData {
    pub (crate) fn new(blob: &[u8]) -> Result<V8LibraryRawData, GearsApiError> {
        let blob = blob.iter().map(|v| *v).collect::<Vec<u8>>();
        let reader = std::io::Cursor::new(blob.clone());
        let zip = zip::ZipArchive::new(reader);
        let zip = match zip {
            Ok(z) => z,
            Err(e) => return Err(GearsApiError::Msg(format!("Failed to read zip file: {}", e))),
        };

        Ok(V8LibraryRawData{
            zip: zip,
            raw_data: blob,
        })
    }

    pub (crate) fn read(&mut self, name: &str) -> Result<String, GearsApiError> {
        let file = self.zip.by_name(name);
        let mut content = match file {
            Ok(p) => p,
            Err(e) => return Err(GearsApiError::Msg(format!("'{}' file does not exists: {}", name, e))),
        };
        let mut content_str: String = String::new();
        if let Err(e) = content.read_to_string(&mut content_str) {
            return Err(GearsApiError::Msg(format!("Failed to read '{}' file: {}", name, e)));
        }
        Ok(content_str)
    }

    pub (crate) fn get_path_string(&mut self, meta_data: &V8LibraryDirMetaData) -> String {
        let path = match meta_data.dir.as_ref() {
            Some(s) => PathBuf::from(s).join(&meta_data.file_name),
            None => PathBuf::from(&meta_data.file_name),
        };
        path.to_str().unwrap().to_string()
    }

    fn file_exists(&mut self, path: &str) -> bool {
        self.zip.by_name(path).map_or(false, |_| true)
    }

    fn dir_exists(&mut self, path: &str) -> bool {
        self.zip.by_name(path).map_or(false, |s| s.is_dir())
    }

    fn path_to_meta_data(&mut self, path: &str) -> Option<V8LibraryDirMetaData> {
        let mut buf = PathBuf::from(path);
        let name = buf.file_name().unwrap().to_str().unwrap().to_string();
        buf.pop();
        let dir = buf.to_str().map_or(None, |s| Some(s.to_string()));
        return Some(V8LibraryDirMetaData{dir: dir, file_name: name})
    }

    pub (crate) fn get_path(&mut self, base_path: Option<&str>, path: &str) -> Option<V8LibraryDirMetaData> {
        if self.file_exists(path) {
            return self.path_to_meta_data(path);
        }

        if path.starts_with("./") {
            let path = path.strip_prefix("./").unwrap();
            let mut buf = match base_path {
                Some(s) => PathBuf::from(s).join(path),
                None => PathBuf::from(path),
            };
            let path = buf.to_str().unwrap();
            if self.file_exists(path) {
                return self.path_to_meta_data(path);
            }
            buf.set_extension("js");
            let path = buf.to_str().unwrap();
            if self.file_exists(path) {
                return self.path_to_meta_data(path);
            }
        }

        let buf = PathBuf::from("node_modules").join(path);
        let path = buf.to_str().unwrap();
        if self.file_exists(path) {
            return self.path_to_meta_data(path);
        }

        if self.dir_exists(path) {
            let js_buf = buf.join("index.js");
            let path = js_buf.to_str().unwrap();
            if self.file_exists(path) {
                return self.path_to_meta_data(path);
            } else {
                let mjs_buf = buf.join("index.mjs");
                let path = mjs_buf.to_str().unwrap();
                if self.file_exists(path) {
                    return self.path_to_meta_data(path);
                }
            }
        }

        // todo: open package.json and search for module section

        None
    }
}

pub (crate) struct V8LibraryCtx {
    matadata: V8LibraryMataData,
    raw_data: V8LibraryRawData,
    module: V8PersistedModule,
    ctx: Rc<V8Context>,
    isolate: Rc<V8Isolate>,
}

impl V8LibraryCtx {
    pub (crate) fn new(matadata: V8LibraryMataData, raw_data: V8LibraryRawData, isolate: V8Isolate, ctx: V8Context, module: V8PersistedModule) -> V8LibraryCtx {
        V8LibraryCtx{
            matadata: matadata,
            raw_data: raw_data,
            isolate: Rc::new(isolate),
            ctx: Rc::new(ctx),
            module: module,
        }
    }
}

impl LibraryCtx for V8LibraryCtx {

    fn load_library(&self, load_library_ctx: &mut dyn LoadLibraryCtx) -> Result<(), GearsApiError> {
        let _isolate_scope = self.isolate.enter();
        let _handlers_scope = self.isolate.new_handlers_scope();
        let ctx_scope = self.ctx.enter();
        let trycatch = self.isolate.new_try_catch();
        let execution_ctx = ExecutionCtx::Load(load_library_ctx);
        
        let local_module = self.module.to_local(&self.isolate);

        // set private content
        self.ctx.set_private_data(0, Some(&execution_ctx));
        self.ctx.set_private_data(1, Some(&self.ctx));
        self.ctx.set_private_data(2, Some(&self.isolate));
        self.ctx.set_private_data(3, Some(&self.raw_data));
        let curr_dir = self.matadata.get_main_dir();
        self.ctx.set_private_data(4, Some(&curr_dir));
        
        let res = local_module.evaluate(&ctx_scope);
        
        // reset private data
        self.ctx.set_private_data::<ExecutionCtx>(0, None);
        self.ctx.set_private_data::<ExecutionCtx>(1, None);
        self.ctx.set_private_data::<ExecutionCtx>(2, None);
        self.ctx.set_private_data::<ExecutionCtx>(3, None);
        self.ctx.set_private_data::<ExecutionCtx>(4, None);
        if res.is_none() {
            let error_utf8 = trycatch.get_exception().to_utf8(&self.isolate).unwrap();
            return Err(GearsApiError::Msg(format!("Failed evaluating module: {}", error_utf8.as_str())));
        }
        let res = res.unwrap();
        if res.is_promise() {
            let promise = res.as_promise();
            if promise.state() == V8PromiseState::Rejected {
                let error = promise.get_result();
                let error_utf8 = error.to_utf8(&self.isolate).unwrap();
                return Err(GearsApiError::Msg(format!("Failed evaluating module: {}", error_utf8.as_str())));
            }
        }
        Ok(())
    }
}

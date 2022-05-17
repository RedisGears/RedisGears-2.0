use redisgears_plugin_api::redisgears_plugin_api::{
    backend_ctx::BackendCtx, load_library_ctx::LibraryCtx, GearsApiError,
};

use crate::v8_script_ctx::V8ScriptCtx;

use v8_rs::v8::{isolate::V8Isolate, v8_init};

use crate::v8_native_functions::get_globals;

use std::alloc::{GlobalAlloc, Layout, System};
use std::str;

struct MyAllocator {
    allocator: Option<&'static dyn GlobalAlloc>,
}

unsafe impl GlobalAlloc for MyAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        match self.allocator.as_ref() {
            Some(a) => a.alloc(layout),
            None => System.alloc(layout),
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        match self.allocator.as_ref() {
            Some(a) => a.dealloc(ptr, layout),
            None => System.dealloc(ptr, layout),
        }
    }
}

#[global_allocator]
static mut GLOBAL: MyAllocator = MyAllocator { allocator: None };

pub(crate) struct V8Backend;

impl BackendCtx for V8Backend {
    fn get_name(&self) -> &'static str {
        "js"
    }

    fn initialize(&self, allocator: &'static dyn GlobalAlloc) -> Result<(), GearsApiError> {
        unsafe { GLOBAL.allocator = Some(allocator) }
        v8_init(); /* Initializing v8 */
        Ok(())
    }

    fn compile_library(&self, blob: &str) -> Result<Box<dyn LibraryCtx>, GearsApiError> {
        let isolate = V8Isolate::new();

        let (script, ctx) = {
            let isolate_scope = isolate.enter();
            let _handlers_scope = isolate.new_handlers_scope();

            let globals = get_globals(&isolate);

            let ctx = isolate_scope.new_context(Some(&globals));
            let ctx_scope = ctx.enter();

            let v8code_str = isolate.new_string(blob);

            let trycatch = isolate.new_try_catch();
            let script = match ctx_scope.compile(&v8code_str) {
                Some(s) => s,
                None => {
                    let error_utf8 = trycatch.get_exception().to_utf8(&isolate).unwrap();
                    return Err(GearsApiError::Msg(format!(
                        "Failed compiling code, {}",
                        error_utf8.as_str()
                    )));
                }
            };

            (script.persist(&isolate), ctx)
        };
        Ok(Box::new(V8ScriptCtx::new(isolate, ctx, script)))
    }
}
// }
// Err(_) => {
//     Err(GearsApiError::Msg("binary format not yet implemented".to_string()))
// zip module
// let mut raw_reader = V8LibraryRawData::new(blob)?;
// let package_json_str = raw_reader.read("package.json")?;
// let lib_meta_data = V8LibraryMataData::new(&package_json_str)?;
// let main_file_str = raw_reader.read(lib_meta_data.get_main_path())?;

// // now we can run the code
// let isolate = V8Isolate::new();
// let (persisted_module, ctx) = {
//     let isolate_scope = isolate.enter();
//     let _handlers_scope = isolate.new_handlers_scope();

//     let globals = get_globals(&isolate);

//     let ctx = isolate_scope.new_context(Some(&globals));
//     let ctx_scope = ctx.enter();

//     let v8module_main_name = isolate.new_string(lib_meta_data.get_main_path());
//     let v8module_main_code = isolate.new_string(&main_file_str);

//     let trycatch = isolate.new_try_catch();
//     let module = match ctx_scope.compile_as_module(&v8module_main_name, &v8module_main_code, true) {
//         Some(m) => m,
//         None => {
//             let error_utf8 = trycatch.get_exception().to_utf8(&isolate).unwrap();
//             return Err(GearsApiError::Msg(format!("Failed compiling module '{}': {}", lib_meta_data.get_main_path(), error_utf8.as_str())));
//         }
//     };

//     let mut module_map = HashMap::new();

//     ctx_scope.set_private_data(0, Some(&raw_reader));
//     ctx_scope.set_private_data::<HashMap<i64, Option<String>>>(1, Some(&module_map));

//     module_map.insert(module.get_identity_hash(), lib_meta_data.get_main_dir().map_or(None, |s|Some(s.to_string())));

//     if !module.initialize(&ctx_scope, |isolate, ctx_scope, module_name, identity_hash|{
//         let raw_reader: &mut V8LibraryRawData = ctx_scope.get_private_data_mut(0).unwrap();
//         let module_map: &mut HashMap<i64, Option<String>> = ctx_scope.get_private_data_mut(1).unwrap();

//         let referrer_path = match module_map.get(&identity_hash) {
//             Some(res) => res,
//             None => {
//                 isolate.raise_exception_str(&format!("Can not find path for referred '{}'", identity_hash));
//                 return None;
//             }
//         };

//         let module_name_utf8 = module_name.to_value().to_utf8(&isolate).unwrap();
//         let module_path_str = module_name_utf8.as_str();

//         let path_meta_data = raw_reader.get_path(referrer_path.as_ref().map_or(None, |s|Some(s)), module_path_str);
//         if path_meta_data.is_none() {
//             isolate.raise_exception_str(&format!("Cannot find module '{}'", module_path_str));
//             return None;
//         }

//         let path_meta_data = path_meta_data.unwrap();

//         let final_path = raw_reader.get_path_string(&path_meta_data);
//         let file_str = match raw_reader.read(&final_path) {
//             Ok(v) => v,
//             Err(e) => {
//                 match e {
//                     GearsApiError::Msg(msg) => isolate.raise_exception_str(&format!("Failed compiling module '{}': {}", module_name_utf8.as_str(), msg)),
//                 }
//                 return None
//             }
//         };
//         let final_path_v8_str = isolate.new_string(&final_path);
//         let v8module_file_code = isolate.new_string(&file_str);
//         let new_module = ctx_scope.compile_as_module(&final_path_v8_str, &v8module_file_code, true);

//         if let Some(new_module) = new_module {
//             module_map.insert(new_module.get_identity_hash(), path_meta_data.dir);
//             Some(new_module)
//         } else {
//             None
//         }
//     }) {
//         let error_utf8 = trycatch.get_exception().to_utf8(&isolate).unwrap();
//         return Err(GearsApiError::Msg(format!("Failed initializing module '{}': {}", lib_meta_data.get_main_path(), error_utf8.as_str())));
//     }
//     ctx_scope.set_private_data::<&V8LibraryRawData>(0, None);
//     ctx_scope.set_private_data::<HashMap<i64, String>>(1, None);
//     (module.persist(&isolate), ctx)
// };

// Ok(Box::new(V8LibraryCtx::new(lib_meta_data, raw_reader, isolate, ctx, persisted_module)))
// }
// }

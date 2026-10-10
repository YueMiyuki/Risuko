#![cfg(target_os = "android")]

use std::collections::HashMap;
use std::os::raw::c_void;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use jni::errors::LogErrorAndDefault;
use jni::objects::{JClass, JObject, JString, Reference};
use jni::refs::Global;
use jni::sys::{jint, JNI_VERSION_1_6};
use jni::{jni_sig, jni_str, Env, EnvUnowned, JValue, JavaVM};

static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();
static ANDROID_APP: OnceLock<Global<JObject<'static>>> = OnceLock::new();
static NDK_CONTEXT_READY: OnceLock<()> = OnceLock::new();
static NDK_INIT: Mutex<()> = Mutex::new(());
static DIRECTORY_PICKERS: OnceLock<
    Mutex<HashMap<String, tokio::sync::oneshot::Sender<Option<String>>>>,
> = OnceLock::new();
static DIRECTORY_PICKER_COUNTER: AtomicU64 = AtomicU64::new(1);

#[no_mangle]
pub extern "system" fn JNI_OnLoad(vm: *mut jni::sys::JavaVM, _: *mut c_void) -> jint {
    // SAFETY: Android gives us a process-lifetime `JavaVM*`. In jni 0.22,
    // `JavaVM::from_raw` initializes the crate-wide singleton and returns an
    // owned handle directly (it no longer returns a `Result`)
    let vm_raw = vm.cast::<c_void>();
    let vm = unsafe { JavaVM::from_raw(vm) };
    let _ = JAVA_VM.set(vm);
    if let Err(err) = init_ndk_context(vm_raw) {
        tracing::warn!("[Risuko] ndk_context init in JNI_OnLoad failed: {err}");
    }
    JNI_VERSION_1_6
}

pub fn ensure_ndk_context() -> Result<(), String> {
    if NDK_CONTEXT_READY.get().is_some() {
        return Ok(());
    }
    let vm = JAVA_VM
        .get()
        .ok_or_else(|| "JavaVM not captured (JNI_OnLoad didn't run?)".to_string())?;
    init_ndk_context(vm.get_raw().cast())
}

fn init_ndk_context(java_vm: *mut c_void) -> Result<(), String> {
    let _guard = NDK_INIT
        .lock()
        .map_err(|e| format!("NDK init lock poisoned: {e}"))?;
    if NDK_CONTEXT_READY.get().is_some() {
        return Ok(());
    }
    let vm = JAVA_VM
        .get()
        .ok_or_else(|| "JavaVM not captured (JNI_OnLoad didn't run?)".to_string())?;
    let (app_global, ctx_ptr) = vm
        .attach_current_thread(|env| -> jni::errors::Result<_> {
            let app = current_application(env)?;
            let app_global = env.new_global_ref(app)?;
            let ctx_ptr = app_global.as_raw().cast::<c_void>();
            Ok((app_global, ctx_ptr))
        })
        .map_err(|e: jni::errors::Error| format!("init_ndk_context: {e}"))?;
    if ANDROID_APP.set(app_global).is_err() {
        return Err("ANDROID_APP already set".to_string());
    }
    // SAFETY: pointers are valid for the process lifetime; called exactly once
    unsafe {
        ndk_context::initialize_android_context(java_vm, ctx_ptr);
    }
    if NDK_CONTEXT_READY.set(()).is_err() {
        return Err("NDK_CONTEXT_READY already set".to_string());
    }
    Ok(())
}

pub fn stage_share_path(path: &str) -> Result<String, String> {
    if !path.starts_with("content://") {
        return Ok(path.to_string());
    }
    let vm = JAVA_VM
        .get()
        .ok_or_else(|| "JavaVM not captured (JNI_OnLoad didn't run?)".to_string())?;
    let staged = vm
        .attach_current_thread(|env| -> jni::errors::Result<Option<String>> {
            let activity = main_activity_class(env)?;
            let uri = env.new_string(path)?;
            let value = env
                .call_static_method(
                    &activity,
                    jni_str!("stageContentUri"),
                    jni_sig!("(Ljava/lang/String;)Ljava/lang/String;"),
                    &[JValue::Object(&uri)],
                )?
                .l()?;
            if value.is_null() {
                return Ok(None);
            }
            let value_str = JString::cast_local(env, value)?;
            Ok(Some(value_str.try_to_string(env)?))
        })
        .map_err(|e: jni::errors::Error| format!("MainActivity.stageContentUri: {e}"))?;
    staged.ok_or_else(|| format!("failed to stage content URI: {path}"))
}

#[no_mangle]
pub extern "system" fn Java_app_risuko_mobile_MainActivity_nativeOnDirectoryPicked<'local>(
    mut env: EnvUnowned<'local>,
    _activity: JObject<'local>,
    request_id: JString<'local>,
    uri: JString<'local>,
) {
    env.with_env(|env| -> jni::errors::Result<()> {
        let request_id = request_id.try_to_string(env).unwrap_or_default();
        if request_id.is_empty() {
            return Ok(());
        }
        let uri = uri.try_to_string(env).ok();
        if let Ok(mut pending) = directory_pickers().lock() {
            if let Some(tx) = pending.remove(&request_id) {
                let _ = tx.send(uri);
            }
        }
        Ok(())
    })
    .resolve::<LogErrorAndDefault>();
}

fn directory_pickers(
) -> &'static Mutex<HashMap<String, tokio::sync::oneshot::Sender<Option<String>>>> {
    DIRECTORY_PICKERS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub async fn pick_directory() -> Result<Option<String>, String> {
    let request_id = format!(
        "{}-{}",
        std::process::id(),
        DIRECTORY_PICKER_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let (tx, rx) = tokio::sync::oneshot::channel();
    directory_pickers()
        .lock()
        .map_err(|e| format!("directory picker lock poisoned: {e}"))?
        .insert(request_id.clone(), tx);

    if let Err(err) = start_directory_picker(&request_id) {
        if let Ok(mut pending) = directory_pickers().lock() {
            pending.remove(&request_id);
        }
        return Err(err);
    }

    match tokio::time::timeout(Duration::from_secs(300), rx).await {
        Ok(Ok(uri)) => Ok(uri),
        Ok(Err(_)) => Err("Android directory picker was interrupted".to_string()),
        Err(_) => {
            if let Ok(mut pending) = directory_pickers().lock() {
                pending.remove(&request_id);
            }
            Err("Android directory picker timed out".to_string())
        }
    }
}

fn start_directory_picker(request_id: &str) -> Result<(), String> {
    let vm = JAVA_VM
        .get()
        .ok_or_else(|| "JavaVM not captured (JNI_OnLoad didn't run?)".to_string())?;
    let started = vm
        .attach_current_thread(|env| -> jni::errors::Result<bool> {
            let activity = main_activity_class(env)?;
            let request_id = env.new_string(request_id)?;
            env.call_static_method(
                &activity,
                jni_str!("pickDirectory"),
                jni_sig!("(Ljava/lang/String;)Z"),
                &[JValue::Object(&request_id)],
            )?
            .z()
        })
        .map_err(|e: jni::errors::Error| format!("MainActivity.pickDirectory: {e}"))?;
    if started {
        Ok(())
    } else {
        Err("Android activity is not ready".to_string())
    }
}

pub fn set_system_bars(dark_mode: bool) -> Result<(), String> {
    let vm = JAVA_VM
        .get()
        .ok_or_else(|| "JavaVM not captured (JNI_OnLoad didn't run?)".to_string())?;
    vm.attach_current_thread(|env| -> jni::errors::Result<()> {
        let activity = main_activity_class(env)?;
        env.call_static_method(
            &activity,
            jni_str!("setSystemBars"),
            jni_sig!("(Z)V"),
            &[JValue::Bool(dark_mode)],
        )?;
        Ok(())
    })
    .map_err(|e: jni::errors::Error| format!("MainActivity.setSystemBars: {e}"))
}

pub fn ensure_all_files_access() -> Result<bool, String> {
    let vm = JAVA_VM
        .get()
        .ok_or_else(|| "JavaVM not captured (JNI_OnLoad didn't run?)".to_string())?;
    vm.attach_current_thread(|env| -> jni::errors::Result<bool> {
        let activity = main_activity_class(env)?;
        env.call_static_method(
            &activity,
            jni_str!("requestAllFilesAccess"),
            jni_sig!("()Z"),
            &[],
        )?
        .z()
    })
    .map_err(|e: jni::errors::Error| format!("MainActivity.requestAllFilesAccess: {e}"))
}

pub fn show_download_notification(
    progress: u32,
    active_count: u32,
    detail: &str,
) -> Result<(), String> {
    let vm = JAVA_VM
        .get()
        .ok_or_else(|| "JavaVM not captured (JNI_OnLoad didn't run?)".to_string())?;
    vm.attach_current_thread(|env| -> jni::errors::Result<()> {
        let activity = main_activity_class(env)?;
        let detail = env.new_string(detail)?;
        env.call_static_method(
            &activity,
            jni_str!("showDownloadNotification"),
            jni_sig!("(IILjava/lang/String;)V"),
            &[
                JValue::Int(progress.min(100) as i32),
                JValue::Int(active_count.min(i32::MAX as u32) as i32),
                JValue::Object(&detail),
            ],
        )?;
        Ok(())
    })
    .map_err(|e: jni::errors::Error| format!("MainActivity.showDownloadNotification: {e}"))
}

pub fn hide_download_notification() -> Result<(), String> {
    let vm = JAVA_VM
        .get()
        .ok_or_else(|| "JavaVM not captured (JNI_OnLoad didn't run?)".to_string())?;
    vm.attach_current_thread(|env| -> jni::errors::Result<()> {
        let activity = main_activity_class(env)?;
        env.call_static_method(
            &activity,
            jni_str!("hideDownloadNotification"),
            jni_sig!("()V"),
            &[],
        )?;
        Ok(())
    })
    .map_err(|e: jni::errors::Error| format!("MainActivity.hideDownloadNotification: {e}"))
}

pub fn reveal_folder(path: &str) -> Result<(), String> {
    let vm = JAVA_VM
        .get()
        .ok_or_else(|| "JavaVM not captured (JNI_OnLoad didn't run?)".to_string())?;
    let outcome = vm
        .attach_current_thread(|env| -> jni::errors::Result<Option<String>> {
            let activity = main_activity_class(env)?;
            let path_str = env.new_string(path)?;
            let value = env
                .call_static_method(
                    &activity,
                    jni_str!("revealFolder"),
                    jni_sig!("(Ljava/lang/String;)Ljava/lang/String;"),
                    &[JValue::Object(&path_str)],
                )?
                .l()?;
            if value.is_null() {
                return Ok(None);
            }
            let value_str = JString::cast_local(env, value)?;
            Ok(Some(value_str.try_to_string(env)?))
        })
        .map_err(|e: jni::errors::Error| format!("MainActivity.revealFolder: {e}"))?;
    let outcome = outcome.ok_or_else(|| "revealFolder returned null".to_string())?;
    if outcome == "ok" {
        Ok(())
    } else {
        tracing::warn!("[Risuko] revealFolder({path}) -> {outcome}");
        Err(outcome)
    }
}

pub fn open_file(path: &str, mime: &str) -> Result<(), String> {
    let vm = JAVA_VM
        .get()
        .ok_or_else(|| "JavaVM not captured (JNI_OnLoad didn't run?)".to_string())?;
    let outcome = vm
        .attach_current_thread(|env| -> jni::errors::Result<Option<String>> {
            let activity = main_activity_class(env)?;
            let path_str = env.new_string(path)?;
            let mime_str = env.new_string(mime)?;
            let value = env
                .call_static_method(
                    &activity,
                    jni_str!("openFile"),
                    jni_sig!("(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;"),
                    &[JValue::Object(&path_str), JValue::Object(&mime_str)],
                )?
                .l()?;
            if value.is_null() {
                return Ok(None);
            }
            let value_str = JString::cast_local(env, value)?;
            Ok(Some(value_str.try_to_string(env)?))
        })
        .map_err(|e: jni::errors::Error| format!("MainActivity.openFile: {e}"))?;
    let outcome = outcome.ok_or_else(|| "openFile returned null".to_string())?;
    if outcome == "ok" {
        Ok(())
    } else {
        tracing::warn!("[Risuko] openFile({path}, {mime}) -> {outcome}");
        Err(outcome)
    }
}

fn main_activity_class<'local>(env: &mut Env<'local>) -> jni::errors::Result<JClass<'local>> {
    let app = current_application(env)?;
    let class_loader = env
        .call_method(
            &app,
            jni_str!("getClassLoader"),
            jni_sig!("()Ljava/lang/ClassLoader;"),
            &[],
        )?
        .l()?;
    let class_name = env.new_string("app.risuko.mobile.MainActivity")?;
    let activity = env
        .call_method(
            &class_loader,
            jni_str!("loadClass"),
            jni_sig!("(Ljava/lang/String;)Ljava/lang/Class;"),
            &[JValue::Object(&class_name)],
        )?
        .l()?;
    JClass::cast_local(env, activity)
}

fn current_application<'local>(env: &mut Env<'local>) -> jni::errors::Result<JObject<'local>> {
    let class = env.find_class(jni_str!("android/app/ActivityThread"))?;
    let thread = env
        .call_static_method(
            &class,
            jni_str!("currentActivityThread"),
            jni_sig!("()Landroid/app/ActivityThread;"),
            &[],
        )?
        .l()?;
    let app = env
        .call_method(
            &thread,
            jni_str!("getApplication"),
            jni_sig!("()Landroid/app/Application;"),
            &[],
        )?
        .l()?;
    Ok(app)
}

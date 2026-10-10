#[cfg(not(target_os = "android"))]
pub mod host;
#[cfg(not(target_os = "android"))]
pub mod paths;
#[cfg(not(target_os = "android"))]
pub mod time;

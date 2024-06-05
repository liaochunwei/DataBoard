// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod state;

use serde_json::{
    json, 
    Value as JsonValue,
    from_str as json_from_str, 
    to_value as json_from_struct,
};

use std::{
    format,
    sync::Mutex,
};
use crate::state::{
    Setting,
    Query,
    StateStore,
};

struct StateStoreWrapper(pub Mutex<StateStore>);

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}!", name)
}

#[tauri::command]
async fn databoard_loader(
    path: &str,
    state: tauri::State<'_, StateStoreWrapper>,
) -> Result<bool, ()> {
    Ok(state.0.lock().unwrap().read_csv(path))
}

#[tauri::command]
fn databoard_count(state: tauri::State<'_, StateStoreWrapper>) -> usize {
    state.0.lock().unwrap().count()
}
#[tauri::command]
fn databoard_columns(state: tauri::State<'_, StateStoreWrapper>) -> JsonValue {
    let ret = json_from_struct(state.0.lock().unwrap().columns()).unwrap();
    let result = json!({
        "columns": ret
    });
    result
}
#[tauri::command]
fn databoard_unique(name:String, state: tauri::State<'_, StateStoreWrapper>) -> JsonValue {
    let ret = state.0.lock().unwrap().column_unique(name);
    json_from_struct(&ret).unwrap()
}
#[tauri::command]
fn databoard_preview(count: usize, state: tauri::State<'_, StateStoreWrapper>) -> JsonValue {
    let mut data = state.0.lock().unwrap();
    let mut result = data.preview(count);

    let ret = data.to_string(&mut result);
    json_from_str(ret.as_str()).unwrap()
}

#[tauri::command]
async fn databoard_setting(
    setting: Setting,
    state: tauri::State<'_, StateStoreWrapper>,
) -> Result<bool, String> {
    let mut data = state.0.lock().unwrap();
    let ret = data.etl(setting.columns);
    match ret {
        Ok(_) => Ok(true),
        Err(e) => Err(format!("配置错误 {}", e.to_string())),
    }
}

#[tauri::command]
fn databoard_search(
    playload: Query,
    state: tauri::State<'_, StateStoreWrapper>,
) -> Result<JsonValue, String> {
    let mut data = state.0.lock().unwrap();
    let mut result = data.search(playload);
    let ret = data.to_string(&mut result);

    let columns = result.get_column_names();
    let records = json_from_str::<JsonValue>(ret.as_str()).unwrap();

    Ok(json! ({
        "columns": columns,
        "records": records,
    }))
}

#[tauri::command]
fn databoard_search_more(
    start: i32,
    state: tauri::State<'_, StateStoreWrapper>,
) -> Result<JsonValue, String> {
    let mut data = state.0.lock().unwrap();
    let mut result = data.records(start.into(), 100);
    let ret = data.to_string(&mut result);

    Ok(json_from_str(ret.as_str()).unwrap())
}
#[tauri::command]
fn databoard_search_save(
    path: &str,
    state: tauri::State<'_, StateStoreWrapper>,
) -> Result<bool, String> {
    let mut data = state.0.lock().unwrap();
    Ok(data.save_csv(path))
}

fn main() {
    env_logger::init();
    
    let state = StateStoreWrapper(Mutex::new(StateStore::default()));
    tauri::Builder::default()
        .manage(state)
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![
            greet,
            databoard_loader,
            databoard_count,
            databoard_columns,
            databoard_unique,
            databoard_preview,
            databoard_setting,
            databoard_search,
            databoard_search_more,
            databoard_search_save,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

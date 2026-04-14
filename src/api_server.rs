//! Servidor HTTP de la API de recuperación.
//! Puerto: configurable via api_addr en config.toml (recomendado: 127.0.0.1:15001)
//!
//! Endpoints:
//!   GET  /status                    — estado del nodo
//!   GET  /trash                     — lista papelera
//!   POST /trash/restore/:id         — restaura archivo de papelera
//!   GET  /weekly                    — lista backups semanales
//!   POST /weekly/restore            — restaura backup semanal
//!   GET  /monthly                   — lista backups mensuales
//!   POST /monthly/restore           — descifra y restaura backup mensual

use std::path::PathBuf;
use std::sync::Arc;
use axum::{
    Router,
    routing::{get, post},
    extract::{Path, State},
    response::Json,
    http::StatusCode,
};
use serde_json::{json, Value};

use crate::api::{
    self,
    RestoreWeeklyRequest,
    RestoreMonthlyRequest,
};

/// Estado compartido del servidor API.
/// Arc para poder clonarlo entre handlers de axum.
#[derive(Clone)]
pub struct ApiState {
    pub db_path: PathBuf,
    pub vault_root: PathBuf,
    pub trash_dir: PathBuf,
    pub weekly_path: PathBuf,
    pub monthly_path: PathBuf,
    pub node_id: String,
    pub vault_id: String,
    pub is_primary: bool,
}

pub type SharedState = Arc<ApiState>;

/// Arranca el servidor API en background.
pub async fn start(
    api_addr: &str,
    state: ApiState,
) -> anyhow::Result<()> {
    let shared = Arc::new(state);

    let app = Router::new()
        .route("/status",           get(handle_status))
        .route("/trash",            get(handle_list_trash))
        .route("/trash/restore/:id", post(handle_restore_trash))
        .route("/weekly",           get(handle_list_weekly))
        .route("/weekly/restore",   post(handle_restore_weekly))
        .route("/monthly",          get(handle_list_monthly))
        .route("/monthly/restore",  post(handle_restore_monthly))
        .with_state(shared);

    let listener = tokio::net::TcpListener::bind(api_addr).await?;
    tracing::info!("API de recuperación en http://{}", api_addr);

    axum::serve(listener, app).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn handle_status(
    State(state): State<SharedState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db_path = state.db_path.clone();
    let node_id = state.node_id.clone();
    let vault_id = state.vault_id.clone();
    let is_primary = state.is_primary;

    let result = tokio::task::spawn_blocking(move || {
        let conn = crate::db::open_db(&db_path)?;
        api::get_node_status(&conn, &node_id, &vault_id, is_primary)
    }).await;

    match result {
        Ok(Ok(status)) => Ok(Json(serde_json::to_value(status).unwrap())),
        Ok(Err(e)) => Err(internal_error(e.to_string())),
        Err(e) => Err(internal_error(e.to_string())),
    }
}

async fn handle_list_trash(
    State(state): State<SharedState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db_path = state.db_path.clone();

    let result = tokio::task::spawn_blocking(move || {
        let conn = crate::db::open_db(&db_path)?;
        api::list_trash(&conn)
    }).await;

    match result {
        Ok(Ok(items)) => Ok(Json(json!({ "items": items, "count": items.len() }))),
        Ok(Err(e)) => Err(internal_error(e.to_string())),
        Err(e) => Err(internal_error(e.to_string())),
    }
}

async fn handle_restore_trash(
    State(state): State<SharedState>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db_path = state.db_path.clone();
    let trash_dir = state.trash_dir.clone();
    let vault_root = state.vault_root.clone();

    let result = tokio::task::spawn_blocking(move || {
        let conn = crate::db::open_db(&db_path)?;
        api::restore_from_trash(&conn, id, &trash_dir, &vault_root)
    }).await;

    match result {
        Ok(Ok(r)) => Ok(Json(serde_json::to_value(r).unwrap())),
        Ok(Err(e)) => Err(internal_error(e.to_string())),
        Err(e) => Err(internal_error(e.to_string())),
    }
}

async fn handle_list_weekly(
    State(state): State<SharedState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let weekly_path = state.weekly_path.clone();

    let result = tokio::task::spawn_blocking(move || {
        api::list_weekly_backups(&weekly_path)
    }).await;

    match result {
        Ok(Ok(backups)) => Ok(Json(json!({ "backups": backups, "count": backups.len() }))),
        Ok(Err(e)) => Err(internal_error(e.to_string())),
        Err(e) => Err(internal_error(e.to_string())),
    }
}

async fn handle_restore_weekly(
    State(state): State<SharedState>,
    Json(req): Json<RestoreWeeklyRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let weekly_path = state.weekly_path.clone();
    let vault_root = state.vault_root.clone();

    let result = tokio::task::spawn_blocking(move || {
        api::restore_weekly_backup(&weekly_path, &vault_root, &req)
    }).await;

    match result {
        Ok(Ok(r)) => Ok(Json(serde_json::to_value(r).unwrap())),
        Ok(Err(e)) => Err(internal_error(e.to_string())),
        Err(e) => Err(internal_error(e.to_string())),
    }
}

async fn handle_list_monthly(
    State(state): State<SharedState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let monthly_path = state.monthly_path.clone();

    let result = tokio::task::spawn_blocking(move || {
        api::list_monthly_backups(&monthly_path)
    }).await;

    match result {
        Ok(Ok(backups)) => Ok(Json(json!({ "backups": backups, "count": backups.len() }))),
        Ok(Err(e)) => Err(internal_error(e.to_string())),
        Err(e) => Err(internal_error(e.to_string())),
    }
}

async fn handle_restore_monthly(
    State(state): State<SharedState>,
    Json(req): Json<RestoreMonthlyRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let monthly_path = state.monthly_path.clone();
    let vault_root = state.vault_root.clone();
    let vault_id = state.vault_id.clone();

    let result = tokio::task::spawn_blocking(move || {
        api::restore_monthly(&monthly_path, &vault_root, &vault_id, &req)
    }).await;

    match result {
        Ok(Ok(r)) => Ok(Json(serde_json::to_value(r).unwrap())),
        Ok(Err(e)) => Err(internal_error(e.to_string())),
        Err(e) => Err(internal_error(e.to_string())),
    }
}

// ---------------------------------------------------------------------------
// Utilidades
// ---------------------------------------------------------------------------

fn internal_error(msg: String) -> (StatusCode, Json<Value>) {
    tracing::error!("API error: {}", msg);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "ok": false, "error": msg })),
    )
}

async fn handle_list_conflicts(
    State(state): State<SharedState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db_path = state.db_path.clone();

    let result = tokio::task::spawn_blocking(move || {
        let conn = crate::db::open_db(&db_path)?;
        api::list_conflicts(&conn)
    }).await;

    match result {
        Ok(Ok(items)) => Ok(Json(json!({ "conflicts": items, "count": items.len() }))),
        Ok(Err(e)) => Err(internal_error(e.to_string())),
        Err(e) => Err(internal_error(e.to_string())),
    }
}

async fn handle_resolve_conflict(
    State(state): State<SharedState>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let db_path = state.db_path.clone();

    let result = tokio::task::spawn_blocking(move || {
        let conn = crate::db::open_db(&db_path)?;
        api::resolve_conflict(&conn, id)
    }).await;

    match result {
        Ok(Ok(true)) => Ok(Json(json!({ "ok": true, "message": "conflicto marcado como resuelto" }))),
        Ok(Ok(false)) => Ok(Json(json!({ "ok": false, "message": "conflicto no encontrado" }))),
        Ok(Err(e)) => Err(internal_error(e.to_string())),
        Err(e) => Err(internal_error(e.to_string())),
    }
}
mod api;
mod api_server;
mod backup;
mod config;
mod proto;
mod db;
mod scan;
mod util;
mod syncer;
mod net;
mod weekly;
mod monthly;

use anyhow::Context;
use crate::net::{read_msg, write_msg};
use proto::Msg;
use std::{path::PathBuf, time::Duration, sync::{Arc, atomic::{AtomicBool, Ordering}}};
use tokio::net::{TcpListener, TcpStream};

const MAX_MESSAGE_SIZE: usize = 32 * 1024 * 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let args: Vec<String> = std::env::args().collect();
    let config_file = args.get(1).map(|s| s.as_str()).unwrap_or("config.toml");
    let cfg = config::load(config_file).context(format!("load {}", config_file))?;

    validate_config(&cfg)?;

    let node_id = cfg.node_id.clone();
    let vault_root = PathBuf::from(&cfg.vault_path);
    let state_root = vault_root.join(&cfg.state_dir);
    let db_path = state_root.join("state.sqlite");
    let trash_dir = PathBuf::from(&cfg.trash_path);
    let weekly_path = PathBuf::from(&cfg.weekly_backup_path);
    let monthly_path = PathBuf::from(&cfg.monthly_archive_path);

    std::fs::create_dir_all(&state_root)?;
    std::fs::create_dir_all(&trash_dir)?;

    cleanup_orphan_tmp_files(&vault_root)?;

    let conn = db::open_db(&db_path)?;

    let backup_in_progress = Arc::new(AtomicBool::new(false));

    scan::scan_vault(
        &vault_root, &conn, &node_id, &cfg.state_dir,
        &trash_dir, cfg.is_primary_node, cfg.trash_retention_minutes,
    )?;
    tracing::info!("Escaneo inicial completo.");

    if cfg.is_primary_node {
        match weekly::should_run_weekly(&weekly_path, cfg.weekly_interval_seconds) {
            Ok(true) => {
                backup_in_progress.store(true, Ordering::SeqCst);
                tracing::info!("Backup semanal inicial — scan pausado.");
                if let Err(e) = weekly::run_weekly_backup(&vault_root, &weekly_path, &cfg.state_dir) {
                    tracing::warn!("backup semanal inicial falló: {:?}", e);
                }
                backup_in_progress.store(false, Ordering::SeqCst);
                tracing::info!("Backup semanal completado — scan reanudado.");
            }
            Ok(false) => tracing::info!("Backup semanal: no toca aún."),
            Err(e) => tracing::warn!("should_run_weekly falló: {:?}", e),
        }

        match monthly::find_weekly_to_promote(&weekly_path, cfg.monthly_interval_seconds) {
            Ok(Some(old)) => {
                backup_in_progress.store(true, Ordering::SeqCst);
                tracing::info!("Backup mensual inicial — scan pausado: {}", old.display());
                match monthly::run_monthly_archive(
                    &vault_root, &monthly_path, &cfg.state_dir,
                    &cfg.vault_id, &cfg.backup_passphrase,
                ) {
                    Ok(file) => {
                        tracing::info!("Backup mensual creado: {}", file.display());
                        if let Err(e) = monthly::cleanup_weekly_after_promotion(&weekly_path) {
                            tracing::warn!("cleanup semanal falló: {:?}", e);
                        }
                    }
                    Err(e) => tracing::warn!("backup mensual inicial falló: {:?}", e),
                }
                backup_in_progress.store(false, Ordering::SeqCst);
                tracing::info!("Backup mensual completado — scan reanudado.");
            }
            Ok(None) => {}
            Err(e) => tracing::warn!("find_weekly_to_promote falló: {:?}", e),
        }
    }

    let listen_addr = cfg.listen_addr.clone();
    let vault_id = cfg.vault_id.clone();
    let node_id_server = node_id.clone();
    let vault_root_server = vault_root.clone();
    let trash_dir_server = trash_dir.clone();
    let retention = cfg.trash_retention_minutes;
    let is_primary = cfg.is_primary_node;

    tokio::spawn(async move {
        if let Err(e) = run_server(
            &listen_addr, node_id_server, &vault_id,
            vault_root_server, trash_dir_server, retention, is_primary,
        ).await {
            tracing::error!("error en servidor sync: {:?}", e);
        }
    });

    let api_state = api_server::ApiState {
        db_path: db_path.clone(),
        vault_root: vault_root.clone(),
        trash_dir: trash_dir.clone(),
        weekly_path: weekly_path.clone(),
        monthly_path: monthly_path.clone(),
        node_id: cfg.node_id.clone(),
        vault_id: cfg.vault_id.clone(),
        is_primary: cfg.is_primary_node,
    };
    let api_addr = cfg.api_addr.clone();

    tokio::spawn(async move {
        if let Err(e) = api_server::start(&api_addr, api_state).await {
            tracing::error!("error en servidor API: {:?}", e);
        }
    });

    let trash_interval = Duration::from_secs(cfg.trash_scan_interval_seconds);
    let tombstone_interval = Duration::from_secs(cfg.tombstone_purge_interval_seconds);
    let backup_check_interval = Duration::from_secs(60);

    let mut last_trash_purge = std::time::Instant::now();
    let mut last_tombstone_purge = std::time::Instant::now();
    let mut last_backup_check = std::time::Instant::now();

    loop {
        tokio::time::sleep(Duration::from_secs(cfg.scan_interval_seconds)).await;

        if backup_in_progress.load(Ordering::SeqCst) {
            tracing::info!("Scan y flush pausados — backup en curso.");
            continue;
        }

        scan::scan_vault(
            &vault_root, &conn, &node_id, &cfg.state_dir,
            &trash_dir, cfg.is_primary_node, cfg.trash_retention_minutes,
        )?;

        if let Err(e) = syncer::flush_ops_to_peer(&cfg, &conn, &vault_root).await {
            tracing::warn!("flush_ops_to_peer falló: {:?}", e);
        }

        if cfg.is_primary_node && last_trash_purge.elapsed() >= trash_interval {
            if let Err(e) = backup::purge_expired_trash(&conn, &trash_dir) {
                tracing::warn!("purge_expired_trash falló: {:?}", e);
            }
            last_trash_purge = std::time::Instant::now();
        }

        if !cfg.is_primary_node && last_trash_purge.elapsed() >= trash_interval {
            if let Err(e) = backup::promote_expired_local_trash(&conn) {
                tracing::warn!("promote_expired_local_trash falló: {:?}", e);
            }
            last_trash_purge = std::time::Instant::now();
        }

        if cfg.is_primary_node && last_tombstone_purge.elapsed() >= tombstone_interval {
            if let Err(e) = backup::purge_old_tombstones(&conn) {
                tracing::warn!("purge_old_tombstones falló: {:?}", e);
            }
            last_tombstone_purge = std::time::Instant::now();
        }

        if cfg.is_primary_node && last_backup_check.elapsed() >= backup_check_interval {
            match weekly::should_run_weekly(&weekly_path, cfg.weekly_interval_seconds) {
                Ok(true) => {
                    backup_in_progress.store(true, Ordering::SeqCst);
                    tracing::info!("Iniciando backup semanal — scan pausado.");
                    if let Err(e) = weekly::run_weekly_backup(&vault_root, &weekly_path, &cfg.state_dir) {
                        tracing::warn!("backup semanal falló: {:?}", e);
                    }
                    backup_in_progress.store(false, Ordering::SeqCst);
                    tracing::info!("Backup semanal completado — scan reanudado.");
                }
                Ok(false) => {}
                Err(e) => tracing::warn!("should_run_weekly falló: {:?}", e),
            }

            match monthly::find_weekly_to_promote(&weekly_path, cfg.monthly_interval_seconds) {
                Ok(Some(old)) => {
                    backup_in_progress.store(true, Ordering::SeqCst);
                    tracing::info!("Iniciando backup mensual — scan pausado.");
                    match monthly::run_monthly_archive(
                        &vault_root, &monthly_path, &cfg.state_dir,
                        &cfg.vault_id, &cfg.backup_passphrase,
                    ) {
                        Ok(file) => {
                            tracing::info!("Backup mensual creado: {}", file.display());
                            if let Err(e) = monthly::cleanup_weekly_after_promotion(&weekly_path) {
                                tracing::warn!("cleanup semanal falló: {:?}", e);
                            }
                        }
                        Err(e) => tracing::warn!("backup mensual falló: {:?}", e),
                    }
                    backup_in_progress.store(false, Ordering::SeqCst);
                    tracing::info!("Backup mensual completado — scan reanudado.");
                }
                Ok(None) => {}
                Err(e) => tracing::warn!("find_weekly_to_promote falló: {:?}", e),
            }

            last_backup_check = std::time::Instant::now();
        }

        // Coalescing: colapsar ops redundantes del mismo archivo
        match db::ops_queue_coalesce(&conn) {
            Ok(n) if n > 0 => tracing::info!("ops_queue coalescing: {} ops redundantes eliminadas", n),
            Ok(_) => {}
            Err(e) => tracing::warn!("ops_queue_coalesce falló: {:?}", e),
        }

        // Trim: si se supera el límite, descartar las más antiguas
        match db::ops_queue_trim(&conn, cfg.ops_queue_max) {
            Ok(n) if n > 0 => tracing::warn!(
                "ops_queue superó el límite de {} — descartadas {} ops antiguas",
                cfg.ops_queue_max, n
            ),
            Ok(_) => {}
            Err(e) => tracing::warn!("ops_queue_trim falló: {:?}", e),
        }

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM ops_queue", [], |r| r.get(0)
        )?;
        tracing::info!("Scan completo. ops_queue={}/{}", count, cfg.ops_queue_max);
    }
}

fn validate_config(cfg: &config::Config) -> anyhow::Result<()> {
    if !std::path::Path::new(&cfg.vault_path).exists() {
        anyhow::bail!("vault_path no existe: '{}'", cfg.vault_path);
    }
    if cfg.node_id.trim().is_empty() {
        anyhow::bail!("node_id no puede estar vacío");
    }
    if cfg.vault_id.trim().is_empty() {
        anyhow::bail!("vault_id no puede estar vacío");
    }
    if cfg.scan_interval_seconds < 2 {
        anyhow::bail!("scan_interval_seconds mínimo es 2");
    }
    if cfg.is_primary_node
        && !cfg.monthly_archive_path.is_empty()
        && cfg.backup_passphrase.trim().is_empty()
    {
        anyhow::bail!("backup_passphrase no puede estar vacía con monthly_archive_path configurado");
    }
    if !cfg.peer_addr.contains(':') {
        anyhow::bail!("peer_addr formato incorrecto: '{}'", cfg.peer_addr);
    }
    if !cfg.api_addr.contains(':') {
        anyhow::bail!("api_addr formato incorrecto: '{}'", cfg.api_addr);
    }
    tracing::info!("Config OK — node_id={} vault_id={}", cfg.node_id, cfg.vault_id);
    Ok(())
}

fn cleanup_orphan_tmp_files(vault_root: &std::path::Path) -> anyhow::Result<()> {
    let mut count = 0usize;
    for entry in walkdir::WalkDir::new(vault_root).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("tmp_vsync") {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!("no se pudo limpiar tmp_vsync: {}: {:?}", path.display(), e);
            } else {
                count += 1;
            }
        }
    }
    if count > 0 {
        tracing::info!("Limpiados {} archivos .tmp_vsync huérfanos", count);
    }
    Ok(())
}

async fn run_server(
    listen_addr: &str,
    node_id: String,
    vault_id: &str,
    vault_root: PathBuf,
    trash_dir: PathBuf,
    retention_minutes: u64,
    is_primary: bool,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    tracing::info!("Servidor sync en {}", listen_addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        let vault_id = vault_id.to_string();
        let vault_root = vault_root.clone();
        let node_id = node_id.clone();
        let trash_dir = trash_dir.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_incoming(
                stream, &node_id, addr.to_string(), &vault_id,
                vault_root, trash_dir, retention_minutes, is_primary,
            ).await {
                tracing::warn!("error en sesión: {:?}", e);
            }
        });
    }
}

async fn handle_incoming(
    mut stream: TcpStream,
    node_id: &str,
    addr: String,
    vault_id: &str,
    vault_root: PathBuf,
    trash_dir: PathBuf,
    retention_minutes: u64,
    is_primary: bool,
) -> anyhow::Result<()> {
    let msg = read_msg(&mut stream).await?;

    match msg {
        Msg::Hello(h) => {
            if h.vault_id != vault_id {
                write_msg(&mut stream, &Msg::Error { message: "vault_id no coincide".into() }).await?;
                anyhow::bail!("vault_id no coincide desde {}", addr);
            }
            tracing::info!("Hello desde {} peer_id={} host={}", addr, h.peer_id, h.hostname);
            write_msg(&mut stream, &Msg::HelloAck {
                vault_id: vault_id.into(),
                peer_id: node_id.to_string(),
            }).await?;
        }
        _ => {
            write_msg(&mut stream, &Msg::Error { message: "se esperaba Hello".into() }).await?;
            anyhow::bail!("se esperaba Hello primero");
        }
    }

    loop {
        let msg = match read_msg(&mut stream).await {
            Ok(m) => m,
            Err(_) => break,
        };

        match msg {
            Msg::PushOp(op) => {
                let db_path = vault_root.join(".vaultsync/state.sqlite");
                let vault_root_clone = vault_root.clone();
                let op_clone = op.clone();
                let node_id_owned = node_id.to_string();

                let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    if let Ok(conn) = db::open_db(&db_path) {
                        db::advance_clock(&conn, &node_id_owned, op_clone.lamport_ts)?;

                        if (op_clone.op_type == "CREATE" || op_clone.op_type == "UPDATE")
                            && db::tombstone_exists(&conn, &op_clone.path)?
                        {
                            anyhow::bail!("rechazado por tombstone: {}", op_clone.path);
                        }

                        // Política de conflictos
                        if op_clone.op_type == "UPDATE" || op_clone.op_type == "CREATE" {
                            if let Some(local) = db::get_file_record(&conn, &op_clone.path)? {
                                // Hay versión local con lamport distinto — posible conflicto
                                if local.lamport_ts > 0
                                    && op_clone.lamport_ts > 0
                                    && local.lamport_ts != op_clone.lamport_ts
                                    && local.file_exists
                                {
                                    apply_conflict_policy(
                                        &conn,
                                        &vault_root_clone,
                                        &op_clone,
                                        local.lamport_ts,
                                    )?;
                                    return Ok(());
                                }
                            }
                        }
                    }
                    apply_file_op(&vault_root_clone, &op_clone)
                }).await;

                let inner = match result {
                    Ok(r) => r,
                    Err(e) => Err(anyhow::anyhow!("spawn_blocking: {:?}", e)),
                };

                let ok = inner.is_ok();
                let message = inner.err().map(|e| e.to_string()).unwrap_or_else(|| "aplicada".into());

                if ok {
                    tracing::info!("op [lamport={}] [from={}]: {} {}",
                        op.lamport_ts, op.origin_node_id, op.op_type, op.path);
                } else {
                    tracing::warn!("op rechazada: {}", message);
                }

                write_msg(&mut stream, &Msg::PushOpAck { op_id: op.op_id, ok, message }).await?;
            }

            Msg::TrashOp(op) if is_primary => {
                let db_path = vault_root.join(".vaultsync/state.sqlite");
                let trash_dir_clone = trash_dir.clone();
                let node_id_owned = node_id.to_string();
                let op_clone = op.clone();

                let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    if op_clone.data.len() > MAX_MESSAGE_SIZE {
                        anyhow::bail!("TrashOp demasiado grande: {} bytes", op_clone.data.len());
                    }
                    let conn = db::open_db(&db_path)?;
                    db::advance_clock(&conn, &node_id_owned, op_clone.lamport_ts)?;
                    backup::write_to_trash(&op_clone.data, &trash_dir_clone, &op_clone.trash_name)?;
                    db::trash_insert(&conn, &op_clone.original_path, &op_clone.trash_name,
                                     &op_clone.origin_node_id, retention_minutes)?;
                    Ok(())
                }).await;

                let inner = match result {
                    Ok(r) => r,
                    Err(e) => Err(anyhow::anyhow!("spawn_blocking: {:?}", e)),
                };

                let ok = inner.is_ok();
                let message = inner.err().map(|e| e.to_string()).unwrap_or_else(|| "recibido".into());

                write_msg(&mut stream, &Msg::TrashOpAck { op_id: op.op_id, ok, message }).await?;
            }

            other => {
                write_msg(&mut stream, &Msg::Error {
                    message: format!("mensaje inesperado: {:?}", other),
                }).await?;
                break;
            }
        }
    }

    Ok(())
}

/// Política de conflictos:
///   - Gana el Lamport más alto (last-write-wins)
///   - El perdedor va a .restauracion/ con metadatos
///   - Se registra en la tabla conflicts para que la API lo exponga
fn apply_conflict_policy(
    conn: &rusqlite::Connection,
    vault_root: &std::path::Path,
    op: &crate::proto::FileOp,
    local_lamport: u64,
) -> anyhow::Result<()> {
    let full_path = vault_root.join(&op.path);
    let restore_dir = vault_root.join(".restauracion");
    std::fs::create_dir_all(&restore_dir)?;

    // Nombre del archivo de conflicto con metadatos legibles
    let stem = std::path::Path::new(&op.path)
        .file_stem().unwrap_or_default().to_string_lossy();
    let ext = std::path::Path::new(&op.path)
        .extension().map(|e| format!(".{}", e.to_string_lossy())).unwrap_or_default();
    let date = chrono::Local::now().format("%Y-%m-%d_%H%M%S");

    let remote_wins = op.lamport_ts > local_lamport;

    let (winner_label, loser_label, loser_lamport, loser_node) = if remote_wins {
        ("remoto", &op.origin_node_id as &str, local_lamport, "local")
    } else {
        ("local", op.origin_node_id.as_str(), op.lamport_ts, op.origin_node_id.as_str())
    };

    let conflict_name = format!(
        "{} (conflicto {} {}){}", stem, loser_node, date, ext
    );
    let conflict_path = restore_dir.join(&conflict_name);

    if remote_wins {
        // Gana el remoto: guardamos la versión local en .restauracion/
        if full_path.exists() {
            std::fs::copy(&full_path, &conflict_path)
                .with_context(|| format!("guardar versión local en conflicto: {}", op.path))?;
        }
        // Aplicar la versión remota
        apply_file_op(vault_root, op)?;
        tracing::warn!(
            "CONFLICTO resuelto — gana {} [lamport={}], perdedor {} [lamport={}] → {}",
            winner_label, op.lamport_ts, loser_label, local_lamport, conflict_name
        );
    } else {
        // Gana el local: guardamos la versión remota en .restauracion/
        if let Some(data) = &op.data {
            std::fs::write(&conflict_path, data)
                .with_context(|| format!("guardar versión remota en conflicto: {}", op.path))?;
        }
        // No tocamos el archivo local — ya es el ganador
        tracing::warn!(
            "CONFLICTO resuelto — gana {} [lamport={}], perdedor {} [lamport={}] → {}",
            winner_label, local_lamport, loser_label, loser_lamport, conflict_name
        );
    }

    // Registrar en DB para que la API lo exponga
    db::conflict_insert(
        conn,
        &op.path,
        &conflict_name,
        local_lamport,
        op.lamport_ts,
        &op.origin_node_id,
    )?;

    Ok(())
}

fn apply_file_op(vault_root: &std::path::Path, op: &crate::proto::FileOp) -> anyhow::Result<()> {
    let full_path = vault_root.join(&op.path);
    match op.op_type.as_str() {
        "CREATE" | "UPDATE" => {
            let parent = full_path.parent()
                .ok_or_else(|| anyhow::anyhow!("sin parent para {}", full_path.display()))?;
            std::fs::create_dir_all(parent)?;
            let tmp_path = full_path.with_extension("tmp_vsync");
            std::fs::write(&tmp_path, op.data.as_deref().unwrap_or_default())?;
            std::fs::rename(&tmp_path, &full_path)?;
        }
        "DELETE" | "TRASH" => {
            if full_path.exists() { std::fs::remove_file(&full_path)?; }
        }
        "CREATE_DIR" => { std::fs::create_dir_all(&full_path)?; }
        "DELETE_DIR" => {
            if full_path.exists() { std::fs::remove_dir_all(&full_path)?; }
        }
        other => anyhow::bail!("op_type no soportado: {}", other),
    }
    Ok(())
}

use anyhow::Context as _;
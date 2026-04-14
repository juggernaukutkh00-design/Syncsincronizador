//! Lógica de la API de recuperación.
//! Los handlers de axum están en api_server.rs.
//! Este módulo contiene las operaciones puras de recuperación.

use std::path::{Path, PathBuf};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use crate::util::now_epoch;

// ---------------------------------------------------------------------------
// Tipos de respuesta JSON
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct NodeStatus {
    pub node_id: String,
    pub vault_id: String,
    pub is_primary_node: bool,
    pub ops_queue_count: i64,
    pub trash_count: i64,
    pub last_scan: String,
}

#[derive(Serialize)]
pub struct TrashItem {
    pub id: i64,
    pub original_path: String,
    pub trash_name: String,
    pub origin_node_id: String,
    pub deleted_at: String,
    pub expires_in_seconds: i64,
}

#[derive(Serialize)]
pub struct WeeklyBackup {
    pub name: String,
    pub path: String,
    pub file_count: usize,
    pub created_at: String,
}

#[derive(Serialize)]
pub struct MonthlyBackup {
    pub name: String,
    pub path: String,
    pub size_bytes: u64,
    pub has_sha256: bool,
}

#[derive(Deserialize)]
pub struct RestoreWeeklyRequest {
    pub backup_name: String,
    /// Si se especifica, solo restaura ese archivo concreto.
    /// Si es None, restaura todo el backup.
    pub file_path: Option<String>,
    /// Carpeta destino. Si es None, restaura al vault original.
    pub dest_override: Option<String>,
}

#[derive(Deserialize)]
pub struct RestoreMonthlyRequest {
    pub backup_name: String,
    pub passphrase: String,
    /// Carpeta destino. Si es None, usa una carpeta temporal segura.
    pub dest_override: Option<String>,
}

#[derive(Serialize)]
pub struct RestoreResult {
    pub ok: bool,
    pub message: String,
    pub files_restored: usize,
    pub dest_path: String,
}

// ---------------------------------------------------------------------------
// Estado del nodo
// ---------------------------------------------------------------------------

pub fn get_node_status(
    conn: &rusqlite::Connection,
    node_id: &str,
    vault_id: &str,
    is_primary: bool,
) -> anyhow::Result<NodeStatus> {
    let ops_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM ops_queue", [], |r| r.get(0)
    ).unwrap_or(0);

    let trash_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM trash", [], |r| r.get(0)
    ).unwrap_or(0);

    Ok(NodeStatus {
        node_id: node_id.to_string(),
        vault_id: vault_id.to_string(),
        is_primary_node: is_primary,
        ops_queue_count: ops_count,
        trash_count,
        last_scan: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
    })
}

// ---------------------------------------------------------------------------
// Papelera
// ---------------------------------------------------------------------------

pub fn list_trash(conn: &rusqlite::Connection) -> anyhow::Result<Vec<TrashItem>> {
    let mut stmt = conn.prepare(
        "SELECT id, original_path, trash_name, origin_node_id, deleted_at, expires_at
         FROM trash ORDER BY expires_at ASC"
    )?;

    let now = now_epoch();

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;

    let mut items = Vec::new();
    for row in rows {
        let (id, original_path, trash_name, origin_node_id, deleted_at, expires_at) = row?;
        items.push(TrashItem {
            id,
            original_path,
            trash_name,
            origin_node_id,
            deleted_at: format_epoch(deleted_at),
            expires_in_seconds: (expires_at - now).max(0),
        });
    }

    Ok(items)
}

/// Restaura un archivo de la papelera a su path original en el vault.
pub fn restore_from_trash(
    conn: &rusqlite::Connection,
    trash_id: i64,
    trash_dir: &Path,
    vault_root: &Path,
) -> anyhow::Result<RestoreResult> {
    // Buscar la entrada en la DB
    let result = conn.query_row(
        "SELECT original_path, trash_name FROM trash WHERE id = ?1",
        rusqlite::params![trash_id],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    );

    let (original_path, trash_name) = match result {
        Ok(r) => r,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            return Ok(RestoreResult {
                ok: false,
                message: format!("no se encontró entrada en papelera con id={}", trash_id),
                files_restored: 0,
                dest_path: String::new(),
            });
        }
        Err(e) => return Err(e.into()),
    };

    let trash_file = trash_dir.join(&trash_name);
    let dest = vault_root.join(&original_path);

    if !trash_file.exists() {
        return Ok(RestoreResult {
            ok: false,
            message: format!("archivo en papelera no encontrado en disco: {}", trash_name),
            files_restored: 0,
            dest_path: String::new(),
        });
    }

    // Crear directorios intermedios si hacen falta
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .context("crear directorios para restauración")?;
    }

    // Copiar de papelera al vault (no mover — la papelera sigue hasta que expira)
    std::fs::copy(&trash_file, &dest)
        .with_context(|| format!("restaurar {} desde papelera", original_path))?;

    tracing::info!("Restaurado desde papelera: {} → {}", trash_name, original_path);

    Ok(RestoreResult {
        ok: true,
        message: format!("restaurado correctamente: {}", original_path),
        files_restored: 1,
        dest_path: dest.to_string_lossy().to_string(),
    })
}

// ---------------------------------------------------------------------------
// Backups semanales
// ---------------------------------------------------------------------------

pub fn list_weekly_backups(weekly_path: &Path) -> anyhow::Result<Vec<WeeklyBackup>> {
    if !weekly_path.exists() {
        return Ok(vec![]);
    }

    let mut backups = Vec::new();

    let mut entries: Vec<_> = std::fs::read_dir(weekly_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    entries.sort_by_key(|e| e.file_name());
    entries.reverse(); // más reciente primero

    for entry in entries {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        // Contar archivos
        let file_count = walkdir::WalkDir::new(&path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .count();

        let metadata = entry.metadata()?;
        let created_at = metadata.modified()
            .ok()
            .and_then(|t| {
                let secs = t.duration_since(std::time::UNIX_EPOCH).ok()?.as_secs() as i64;
                Some(format_epoch(secs))
            })
            .unwrap_or_else(|| "desconocido".to_string());

        backups.push(WeeklyBackup {
            name,
            path: path.to_string_lossy().to_string(),
            file_count,
            created_at,
        });
    }

    Ok(backups)
}

/// Restaura un backup semanal completo o un archivo concreto.
pub fn restore_weekly_backup(
    weekly_path: &Path,
    vault_root: &Path,
    req: &RestoreWeeklyRequest,
) -> anyhow::Result<RestoreResult> {
    let backup_dir = weekly_path.join(&req.backup_name);

    if !backup_dir.exists() {
        return Ok(RestoreResult {
            ok: false,
            message: format!("backup semanal no encontrado: {}", req.backup_name),
            files_restored: 0,
            dest_path: String::new(),
        });
    }

    let dest_root = req.dest_override
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| vault_root.to_path_buf());

    // Si se especifica un archivo concreto
    if let Some(ref file_path) = req.file_path {
        let src = backup_dir.join(file_path);
        let dest = dest_root.join(file_path);

        if !src.exists() {
            return Ok(RestoreResult {
                ok: false,
                message: format!("archivo no encontrado en backup: {}", file_path),
                files_restored: 0,
                dest_path: String::new(),
            });
        }

        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::copy(&src, &dest)?;
        tracing::info!("Restaurado archivo semanal: {}", file_path);

        return Ok(RestoreResult {
            ok: true,
            message: format!("archivo restaurado: {}", file_path),
            files_restored: 1,
            dest_path: dest.to_string_lossy().to_string(),
        });
    }

    // Restauración completa
    let mut count = 0usize;

    for entry in walkdir::WalkDir::new(&backup_dir).into_iter().filter_map(|e| e.ok()) {
        let src = entry.path();

        let rel = match src.strip_prefix(&backup_dir) {
            Ok(r) => r,
            Err(_) => continue,
        };

        if rel.as_os_str().is_empty() { continue; }

        let dest = dest_root.join(rel);

        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dest)?;
            continue;
        }

        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::copy(src, &dest)?;
        count += 1;
    }

    tracing::info!("Restauración semanal completa: {} archivos desde {}", count, req.backup_name);

    Ok(RestoreResult {
        ok: true,
        message: format!("backup semanal restaurado: {} archivos", count),
        files_restored: count,
        dest_path: dest_root.to_string_lossy().to_string(),
    })
}

// ---------------------------------------------------------------------------
// Backups mensuales
// ---------------------------------------------------------------------------

pub fn list_monthly_backups(monthly_path: &Path) -> anyhow::Result<Vec<MonthlyBackup>> {
    if !monthly_path.exists() {
        return Ok(vec![]);
    }

    let mut backups = Vec::new();

    let mut entries: Vec<_> = std::fs::read_dir(monthly_path)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path().extension()
                .and_then(|ext| ext.to_str())
                == Some("vaultbackup")
        })
        .collect();

    entries.sort_by_key(|e| e.file_name());
    entries.reverse();

    for entry in entries {
        let path = entry.path();
        let name = path.file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let size_bytes = entry.metadata()
            .map(|m| m.len())
            .unwrap_or(0);

        let sha_path = path.with_extension("sha256");
        let has_sha256 = sha_path.exists();

        backups.push(MonthlyBackup {
            name,
            path: path.to_string_lossy().to_string(),
            size_bytes,
            has_sha256,
        });
    }

    Ok(backups)
}

/// Descifra y restaura un backup mensual.
pub fn restore_monthly(
    monthly_path: &Path,
    vault_root: &Path,
    vault_id: &str,
    req: &RestoreMonthlyRequest,
) -> anyhow::Result<RestoreResult> {
    let backup_file = monthly_path.join(format!("{}.vaultbackup", req.backup_name));

    if !backup_file.exists() {
        return Ok(RestoreResult {
            ok: false,
            message: format!("backup mensual no encontrado: {}", req.backup_name),
            files_restored: 0,
            dest_path: String::new(),
        });
    }

    // Destino: carpeta temporal segura o la especificada
    let dest_root = req.dest_override
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            vault_root.parent()
                .unwrap_or(vault_root)
                .join(format!("vault_restore_{}", chrono::Local::now().format("%Y%m%d_%H%M%S")))
        });

    match crate::monthly::restore_monthly_backup(
        &backup_file,
        &dest_root,
        vault_id,
        &req.passphrase,
    ) {
        Ok(count) => Ok(RestoreResult {
            ok: true,
            message: format!("backup mensual restaurado: {} archivos", count),
            files_restored: count,
            dest_path: dest_root.to_string_lossy().to_string(),
        }),
        Err(e) => Ok(RestoreResult {
            ok: false,
            message: format!("error en restauración: {}", e),
            files_restored: 0,
            dest_path: String::new(),
        }),
    }
}

// ---------------------------------------------------------------------------
// Utilidades
// ---------------------------------------------------------------------------

fn format_epoch(epoch: i64) -> String {
    use std::time::{UNIX_EPOCH, Duration};
    let d = UNIX_EPOCH + Duration::from_secs(epoch as u64);
    let dt: chrono::DateTime<chrono::Local> = d.into();
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

// ---------------------------------------------------------------------------
// Conflictos
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct ConflictItem {
    pub id: i64,
    pub path: String,
    pub conflict_file: String,
    pub local_lamport: u64,
    pub remote_lamport: u64,
    pub remote_node_id: String,
    pub detected_at: String,
    pub winner: String,
}

pub fn list_conflicts(conn: &rusqlite::Connection) -> anyhow::Result<Vec<ConflictItem>> {
    let entries = crate::db::conflict_list_unresolved(conn)?;
    let items = entries.into_iter().map(|e| {
        let winner = if e.remote_lamport > e.local_lamport {
            "remoto".to_string()
        } else {
            "local".to_string()
        };
        ConflictItem {
            id: e.id,
            path: e.path,
            conflict_file: e.conflict_file,
            local_lamport: e.local_lamport,
            remote_lamport: e.remote_lamport,
            remote_node_id: e.remote_node_id,
            detected_at: format_epoch(e.detected_at),
            winner,
        }
    }).collect();
    Ok(items)
}

pub fn resolve_conflict(conn: &rusqlite::Connection, id: i64) -> anyhow::Result<bool> {
    let affected = conn.execute(
        "UPDATE conflicts SET resolved = 1 WHERE id = ?1",
        rusqlite::params![id],
    )?;
    Ok(affected > 0)
}
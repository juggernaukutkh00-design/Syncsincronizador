use std::time::UNIX_EPOCH;
use walkdir::WalkDir;
use rusqlite::Connection;
use std::path::Path;
use crate::{backup, db, util::now_epoch};

fn should_ignore(path: &str, state_dir_name: &str) -> bool {
    let state_prefix = format!("{}/", state_dir_name);
    path.starts_with(&state_prefix)
        || path == state_dir_name
        || path == ".vaultsyncignore"
        || path == ".obsidian/workspace.json"
        || path == ".obsidian/workspace-mobile.json"
        || path.ends_with(".tmp")
        || path.ends_with(".lock")
        // Ignorar carpeta de conflictos — no sincronizar
        || path.starts_with(".restauracion/")
        || path == ".restauracion"
}

fn is_in_obsidian_trash(rel: &str) -> bool {
    rel.starts_with(".trash/") || rel == ".trash"
}

pub fn scan_vault(
    vault_root: &Path,
    conn: &Connection,
    node_id: &str,
    state_dir_name: &str,
    trash_dir: &Path,
    is_primary_node: bool,
    retention_minutes: u64,
) -> anyhow::Result<()> {
    let scan_ts = now_epoch();

    for entry in WalkDir::new(vault_root).into_iter().filter_map(|e| e.ok()) {
        let p = entry.path();

        let rel = match p.strip_prefix(vault_root) {
            Ok(r) => r.to_string_lossy().replace('\\', "/"),
            Err(_) => continue,
        };

        if rel.is_empty() { continue; }
        if should_ignore(&rel, state_dir_name) { continue; }

        if is_in_obsidian_trash(&rel) && entry.file_type().is_dir() {
            continue;
        }

        if is_in_obsidian_trash(&rel) && entry.file_type().is_file() {
            handle_obsidian_trash_file(
                conn, node_id, p, &rel, trash_dir,
                is_primary_node, retention_minutes,
            )?;
            continue;
        }

        if entry.file_type().is_dir() {
            let md = entry.metadata()?;
            let mtime = md.modified().ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64).unwrap_or(0);

            let prev = db::get_file_record(conn, &rel)?;
            db::upsert_file(conn, &rel, true, 0, mtime, None, "dir", scan_ts, false, 0)?;

            if prev.is_none() {
                let lamport_ts = db::tick_clock(conn, node_id)?;
                db::enqueue_op(conn, "local", node_id, lamport_ts, "CREATE_DIR", &rel, 0, mtime, None)?;
            }
            continue;
        }

        let md = entry.metadata()?;
        let size = md.len();
        let mtime = md.modified().ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64).unwrap_or(0);

        let prev = db::get_file_record(conn, &rel)?;

        match prev {
            None => {
                let lamport_ts = db::tick_clock(conn, node_id)?;
                db::upsert_file(conn, &rel, true, size, mtime, None, "unknown", scan_ts, false, lamport_ts)?;
                db::enqueue_op(conn, "local", node_id, lamport_ts, "CREATE", &rel, size, mtime, None)?;
            }
            Some(old) => {
                let changed = !old.file_exists || old.size != size || old.mtime != mtime;
                if changed {
                    let lamport_ts = db::tick_clock(conn, node_id)?;
                    db::upsert_file(conn, &rel, true, size, mtime, None, "unknown", scan_ts, false, lamport_ts)?;
                    db::enqueue_op(conn, "local", node_id, lamport_ts, "UPDATE", &rel, size, mtime, None)?;
                } else {
                    // Sin cambio — actualizar solo last_seen_scan manteniendo lamport_ts
                    db::upsert_file(conn, &rel, true, size, mtime, None, "unknown", scan_ts, false, old.lamport_ts)?;
                }
            }
        }
    }

    handle_missing_files(conn, node_id, scan_ts)?;
    Ok(())
}

fn handle_obsidian_trash_file(
    conn: &Connection,
    node_id: &str,
    full_path: &Path,
    rel: &str,
    trash_dir: &Path,
    is_primary_node: bool,
    retention_minutes: u64,
) -> anyhow::Result<()> {
    let original_path = rel.trim_start_matches(".trash/");
    tracing::info!("archivo en .trash detectado: {} (original: {})", rel, original_path);

    if is_primary_node {
        match backup::move_to_trash(full_path, trash_dir) {
            Ok(trash_name) => {
                db::trash_insert(conn, original_path, &trash_name, node_id, retention_minutes)?;
                tracing::info!("PC1 papelera {}min: {} → {}", retention_minutes, original_path, trash_name);
            }
            Err(e) => tracing::warn!("PC1 error moviendo a papelera {}: {:?}", original_path, e),
        }
    } else {
        match backup::move_to_trash(full_path, trash_dir) {
            Ok(trash_name) => {
                db::local_trash_insert(conn, original_path, &trash_name, retention_minutes)?;
                tracing::info!("PC2 local_trash: {} → {}", original_path, trash_name);
            }
            Err(e) => tracing::warn!("PC2 error moviendo a local_trash {}: {:?}", original_path, e),
        }
    }

    let lamport_ts = db::tick_clock(conn, node_id)?;
    db::enqueue_op(conn, "local", node_id, lamport_ts, "TRASH", original_path, 0, 0, None)?;
    Ok(())
}

fn handle_missing_files(
    conn: &Connection,
    node_id: &str,
    scan_ts: i64,
) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(
        "SELECT path, size, mtime, hash_sha256, hash_state FROM files
         WHERE file_exists = 1 AND last_seen_scan < ?1",
    )?;

    let rows = stmt.query_map(rusqlite::params![scan_ts], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)? as u64,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;

    let mut missing: Vec<(String, u64, i64, Option<String>, String)> = Vec::new();
    for row in rows { missing.push(row?); }

    for (path, size, mtime, hash_sha256, hash_state) in missing {
        conn.execute("DELETE FROM files WHERE path = ?1", rusqlite::params![path])?;
        let op_type = if hash_state == "dir" { "DELETE_DIR" } else { "DELETE" };
        let lamport_ts = db::tick_clock(conn, node_id)?;
        db::enqueue_op(conn, "local", node_id, lamport_ts, op_type, &path, size, mtime, hash_sha256.as_deref())?;
        tracing::warn!("archivo borrado directamente (sin papelera): {}", path);
    }

    Ok(())
}
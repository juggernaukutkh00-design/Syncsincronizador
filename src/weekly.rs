//! Copias de seguridad semanales.
//!
//! Comportamiento:
//!   - Snapshot completo del vault cada weekly_interval_seconds
//!   - Formato: carpeta con archivos tal cual
//!   - Máximo 4 copias — cuando llega la 5ª se borra la más antigua
//!   - Solo lo ejecuta PC1 (nodo principal)

use std::path::Path;
use anyhow::Context;
use walkdir::WalkDir;

const MAX_WEEKLY_COPIES: usize = 4;

fn backup_folder_name() -> String {
    chrono::Local::now().format("%Y-%m-%d_%H%M%S").to_string()
}

pub fn run_weekly_backup(
    vault_root: &Path,
    weekly_path: &Path,
    state_dir_name: &str,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(weekly_path)
        .context("crear directorio de backups semanales")?;

    let folder_name = backup_folder_name();
    let dest_root = weekly_path.join(&folder_name);
    std::fs::create_dir_all(&dest_root)?;

    tracing::info!("Iniciando backup semanal → {}", dest_root.display());

    let mut count = 0usize;
    let state_prefix = format!("{}/", state_dir_name);

    for entry in WalkDir::new(vault_root).into_iter().filter_map(|e| e.ok()) {
        let p = entry.path();

        let rel = match p.strip_prefix(vault_root) {
            Ok(r) => r.to_string_lossy().replace('\\', "/"),
            Err(_) => continue,
        };

        if rel.is_empty() { continue; }

        if rel.starts_with(&state_prefix)
            || rel == state_dir_name
            || rel.starts_with(".trash")
            || rel.ends_with(".tmp")
            || rel.ends_with(".lock")
            || rel.ends_with(".tmp_vsync")
        {
            continue;
        }

        if entry.file_type().is_dir() {
            std::fs::create_dir_all(dest_root.join(&rel))?;
            continue;
        }

        if entry.file_type().is_file() {
            let dest = dest_root.join(&rel);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::copy(p, &dest)
                .with_context(|| format!("copiar {} al backup", rel))?;
            count += 1;
        }
    }

    tracing::info!("Backup semanal completado: {} archivos → {}", count, folder_name);
    rotate_weekly_backups(weekly_path)?;
    Ok(())
}

fn rotate_weekly_backups(weekly_path: &Path) -> anyhow::Result<()> {
    let mut entries: Vec<_> = std::fs::read_dir(weekly_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    entries.sort_by_key(|e| e.file_name());

    if entries.len() > MAX_WEEKLY_COPIES {
        let to_delete = entries.len() - MAX_WEEKLY_COPIES;
        for entry in entries.iter().take(to_delete) {
            let path = entry.path();
            tracing::info!("Rotando backup semanal antiguo: {}", path.display());
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("borrar backup antiguo {}", path.display()))?;
        }
    }

    Ok(())
}

/// Comprueba si han pasado más de weekly_interval_seconds desde el último backup.
pub fn should_run_weekly(weekly_path: &Path, interval_seconds: u64) -> anyhow::Result<bool> {
    if !weekly_path.exists() {
        return Ok(true);
    }

    let mut entries: Vec<_> = std::fs::read_dir(weekly_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    if entries.is_empty() {
        return Ok(true);
    }

    entries.sort_by_key(|e| e.file_name());
    let latest = entries.last().unwrap();
    let metadata = latest.metadata()?;
    let modified = metadata.modified()?;
    let elapsed = modified.elapsed().unwrap_or_default();

    Ok(elapsed >= std::time::Duration::from_secs(interval_seconds))
}

#[allow(dead_code)]
pub fn list_weekly_backups(weekly_path: &Path) -> anyhow::Result<Vec<String>> {
    if !weekly_path.exists() {
        return Ok(vec![]);
    }

    let mut entries: Vec<String> = std::fs::read_dir(weekly_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    entries.sort();
    entries.reverse();
    Ok(entries)
}
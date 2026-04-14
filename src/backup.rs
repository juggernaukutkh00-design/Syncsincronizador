//! Sistema de papelera con borrado seguro.
//!
//! Flujo PC1 (principal):
//!   .trash/ detectado → papelera 30min → expira → borrado seguro → tombstone
//!
//! Flujo PC2 (secundario) online:
//!   .trash/ detectado → local_trash 30min → expira → pending_trash → TrashOp a PC1 → borrado seguro local
//!
//! Flujo PC2 offline:
//!   .trash/ detectado → local_trash 30min → expira → pending_trash → reconecta → TrashOp a PC1

use std::path::{Path, PathBuf};
use anyhow::Context;
use rand::Rng;

/// Genera un nombre aleatorio para el archivo en papelera.
pub fn random_trash_name() -> String {
    let mut rng = rand::thread_rng();
    let bytes: [u8; 8] = rng.gen();
    hex::encode(bytes)
}

/// Mueve un archivo al directorio de papelera con nombre aleatorio.
/// Devuelve el trash_name generado.
pub fn move_to_trash(file_path: &Path, trash_dir: &Path) -> anyhow::Result<String> {
    std::fs::create_dir_all(trash_dir).context("crear directorio de papelera")?;
    let trash_name = random_trash_name();
    let dest = trash_dir.join(&trash_name);
    std::fs::rename(file_path, &dest)
        .with_context(|| format!("mover {} a papelera", file_path.display()))?;
    Ok(trash_name)
}

/// Escribe bytes directamente en la papelera.
/// Usado por PC1 cuando recibe un TrashOp de PC2.
pub fn write_to_trash(data: &[u8], trash_dir: &Path, trash_name: &str) -> anyhow::Result<()> {
    std::fs::create_dir_all(trash_dir).context("crear directorio de papelera")?;
    let dest = trash_dir.join(trash_name);
    std::fs::write(&dest, data)
        .with_context(|| format!("escribir en papelera: {}", dest.display()))?;
    Ok(())
}

/// Borrado seguro en tres pasos:
///   1. Sobreescribir con bytes aleatorios
///   2. Cifrar con clave aleatoria que se descarta
///   3. Borrar del disco
pub fn secure_delete(file_path: &PathBuf) -> anyhow::Result<()> {
    if !file_path.exists() {
        return Ok(());
    }

    let size = std::fs::metadata(file_path)
        .with_context(|| format!("leer metadata de {}", file_path.display()))?
        .len() as usize;

    let mut rng = rand::thread_rng();

    // Paso 1: sobreescritura aleatoria
    let random_data: Vec<u8> = (0..size).map(|_| rng.gen::<u8>()).collect();
    std::fs::write(file_path, &random_data)
        .with_context(|| format!("sobreescritura de {}", file_path.display()))?;

    // Paso 2: cifrado XOR con clave aleatoria descartada
    let key: Vec<u8> = (0..size).map(|_| rng.gen::<u8>()).collect();
    let encrypted: Vec<u8> = random_data.iter().zip(key.iter()).map(|(b, k)| b ^ k).collect();
    std::fs::write(file_path, &encrypted)
        .with_context(|| format!("cifrado de {}", file_path.display()))?;

    // Paso 3: borrar
    std::fs::remove_file(file_path)
        .with_context(|| format!("borrar {}", file_path.display()))?;

    tracing::info!("borrado seguro completado: {}", file_path.display());
    Ok(())
}

/// Job de PC1: purga archivos expirados de la papelera principal.
/// Borrado seguro + tombstone para cada uno.
pub fn purge_expired_trash(
    conn: &rusqlite::Connection,
    trash_dir: &Path,
) -> anyhow::Result<()> {
    let expired = crate::db::trash_get_expired(conn)?;
    if expired.is_empty() { return Ok(()); }

    tracing::info!("{} archivos expirados en papelera PC1, borrando...", expired.len());

    for entry in expired {
        let file_path = trash_dir.join(&entry.trash_name);
        if let Err(e) = secure_delete(&file_path) {
            tracing::warn!("error en borrado seguro {}: {:?}", entry.trash_name, e);
        }
        // Registrar en tombstones para evitar resurrecciones
        crate::db::tombstone_insert(conn, &entry.original_path)?;
        crate::db::trash_delete(conn, entry.id)?;
        tracing::info!("eliminado definitivamente + tombstone: {}", entry.original_path);
    }
    Ok(())
}

/// Job de PC2: mueve archivos expirados de local_trash a pending_trash.
/// El archivo sigue en disco hasta que PC1 confirme recepción.
pub fn promote_expired_local_trash(
    conn: &rusqlite::Connection,
) -> anyhow::Result<()> {
    let expired = crate::db::local_trash_get_expired(conn)?;
    if expired.is_empty() { return Ok(()); }

    tracing::info!("{} archivos expirados en local_trash PC2, moviendo a pending_trash...", expired.len());

    for entry in expired {
        crate::db::pending_trash_insert(conn, &entry.original_path, &entry.trash_name)?;
        crate::db::local_trash_delete(conn, entry.id)?;
        tracing::info!("local_trash → pending_trash: {}", entry.original_path);
    }
    Ok(())
}

/// Job de limpieza de tombstones — ejecutar cada 1 día.
pub fn purge_old_tombstones(conn: &rusqlite::Connection) -> anyhow::Result<()> {
    let deleted = crate::db::tombstone_purge_old(conn)?;
    if deleted > 0 {
        tracing::info!("tombstones antiguos eliminados: {}", deleted);
    }
    Ok(())
}
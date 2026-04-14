//! Copias de seguridad mensuales.
//!
//! Formato: ZSTD + AES-256-GCM + SHA256
//! Estructura del archivo:
//!   [12 bytes nonce][datos cifrados + 16 bytes tag GCM]
//! Junto al archivo: .sha256 con el hash de verificación

use std::path::{Path, PathBuf};
use std::io::Write;
use anyhow::Context;
use walkdir::WalkDir;
use aes_gcm::{Aes256Gcm, Key, Nonce, aead::{Aead, KeyInit}};
use hkdf::Hkdf;
use sha2::{Sha256, Digest};
use rand::Rng;

fn derive_key(vault_id: &str, passphrase: &str) -> [u8; 32] {
    let ikm = format!("{}:{}", vault_id, passphrase);
    let hk = Hkdf::<Sha256>::new(None, ikm.as_bytes());
    let mut key = [0u8; 32];
    hk.expand(b"vault-syncd-monthly-backup-v1", &mut key)
        .expect("HKDF expand falló");
    key
}

fn pack_vault(vault_root: &Path, state_dir_name: &str) -> anyhow::Result<Vec<u8>> {
    let mut raw: Vec<u8> = Vec::new();
    let state_prefix = format!("{}/", state_dir_name);

    for entry in WalkDir::new(vault_root).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() { continue; }

        let p = entry.path();
        let rel = match p.strip_prefix(vault_root) {
            Ok(r) => r.to_string_lossy().replace('\\', "/"),
            Err(_) => continue,
        };

        if rel.is_empty()
            || rel.starts_with(&state_prefix)
            || rel == state_dir_name
            || rel.starts_with(".trash")
            || rel.ends_with(".tmp")
            || rel.ends_with(".lock")
            || rel.ends_with(".tmp_vsync")
        {
            continue;
        }

        let data = std::fs::read(p)
            .with_context(|| format!("leer {} para backup mensual", rel))?;

        let path_bytes = rel.as_bytes();
        raw.write_all(&(path_bytes.len() as u32).to_be_bytes())?;
        raw.write_all(path_bytes)?;
        raw.write_all(&(data.len() as u64).to_be_bytes())?;
        raw.write_all(&data)?;
    }

    let compressed = zstd::encode_all(raw.as_slice(), 3)
        .context("comprimir con ZSTD")?;

    tracing::info!(
        "Pack vault: {} bytes → {} bytes comprimido ({:.1}% reducción)",
        raw.len(), compressed.len(),
        (1.0 - compressed.len() as f64 / raw.len().max(1) as f64) * 100.0
    );

    Ok(compressed)
}

fn encrypt(data: &[u8], key: &[u8; 32]) -> anyhow::Result<Vec<u8>> {
    let mut rng = rand::thread_rng();
    let nonce_bytes: [u8; 12] = rng.gen();
    let nonce = Nonce::from_slice(&nonce_bytes);

    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let encrypted = cipher.encrypt(nonce, data)
        .map_err(|e| anyhow::anyhow!("cifrado AES-256-GCM falló: {:?}", e))?;

    let mut result = Vec::with_capacity(12 + encrypted.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&encrypted);
    Ok(result)
}

/// Descifra un archivo .vaultbackup y devuelve los datos descomprimidos.
/// Los datos descomprimidos tienen el formato pack_vault:
///   [4 bytes: longitud path][path UTF-8][8 bytes: tamaño][datos]
pub fn decrypt(encrypted_data: &[u8], key: &[u8; 32]) -> anyhow::Result<Vec<u8>> {
    if encrypted_data.len() < 12 {
        anyhow::bail!("archivo de backup demasiado pequeño para ser válido");
    }

    let nonce = Nonce::from_slice(&encrypted_data[..12]);
    let ciphertext = &encrypted_data[12..];

    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let compressed = cipher.decrypt(nonce, ciphertext)
        .map_err(|_| anyhow::anyhow!(
            "descifrado falló — passphrase incorrecta o archivo corrupto"
        ))?;

    let decompressed = zstd::decode_all(compressed.as_slice())
        .context("descomprimir ZSTD falló")?;

    Ok(decompressed)
}

/// Restaura los archivos de un backup mensual a una carpeta destino.
/// Primero descifra, luego extrae archivo por archivo.
pub fn restore_monthly_backup(
    backup_file: &Path,
    dest_root: &Path,
    vault_id: &str,
    passphrase: &str,
) -> anyhow::Result<usize> {
    // Verificar SHA256 antes de descifrar
    verify_monthly_backup(backup_file)?;

    let encrypted = std::fs::read(backup_file)
        .context("leer archivo de backup mensual")?;

    let key = derive_key(vault_id, passphrase);
    let raw = decrypt(&encrypted, &key)?;

    std::fs::create_dir_all(dest_root)?;

    // Extraer archivos del formato pack_vault
    let mut cursor = 0usize;
    let mut count = 0usize;

    while cursor < raw.len() {
        // Leer longitud del path
        if cursor + 4 > raw.len() { break; }
        let path_len = u32::from_be_bytes(raw[cursor..cursor+4].try_into()?) as usize;
        cursor += 4;

        // Leer path
        if cursor + path_len > raw.len() { break; }
        let path_str = std::str::from_utf8(&raw[cursor..cursor+path_len])
            .context("path inválido en backup")?;
        cursor += path_len;

        // Leer tamaño del archivo
        if cursor + 8 > raw.len() { break; }
        let file_size = u64::from_be_bytes(raw[cursor..cursor+8].try_into()?) as usize;
        cursor += 8;

        // Leer datos
        if cursor + file_size > raw.len() { break; }
        let file_data = &raw[cursor..cursor+file_size];
        cursor += file_size;

        // Escribir archivo en destino
        let dest_path = dest_root.join(path_str);
        if let Some(parent) = dest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dest_path, file_data)
            .with_context(|| format!("escribir {} en restauración", path_str))?;

        count += 1;
    }

    tracing::info!("Restauración mensual completada: {} archivos → {}", count, dest_root.display());
    Ok(count)
}

fn write_sha256_file(file_path: &Path) -> anyhow::Result<String> {
    let data = std::fs::read(file_path)?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    let hash = hex::encode(hasher.finalize());

    let sha_path = file_path.with_extension("sha256");
    std::fs::write(&sha_path, format!("{}  {}\n",
        hash,
        file_path.file_name().unwrap_or_default().to_string_lossy()
    ))?;

    Ok(hash)
}

pub fn verify_monthly_backup(backup_file: &Path) -> anyhow::Result<bool> {
    let sha_path = backup_file.with_extension("sha256");

    if !sha_path.exists() {
        anyhow::bail!("archivo .sha256 no encontrado para {}", backup_file.display());
    }

    let stored = std::fs::read_to_string(&sha_path)?;
    let stored_hash = stored.split_whitespace().next().unwrap_or("").to_string();

    let data = std::fs::read(backup_file)?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    let computed = hex::encode(hasher.finalize());

    let ok = stored_hash == computed;
    if ok {
        tracing::info!("Verificación SHA256 OK: {}", backup_file.display());
    } else {
        tracing::warn!("Verificación SHA256 FALLÓ: {}", backup_file.display());
    }

    Ok(ok)
}

fn monthly_name() -> String {
    chrono::Local::now().format("%Y-%m-%d_%H%M%S").to_string()
}

pub fn run_monthly_archive(
    vault_root: &Path,
    monthly_path: &Path,
    state_dir_name: &str,
    vault_id: &str,
    backup_passphrase: &str,
) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(monthly_path)
        .context("crear directorio de backups mensuales")?;

    let name = monthly_name();
    let backup_file = monthly_path.join(format!("{}.vaultbackup", name));

    tracing::info!("Iniciando backup mensual → {}", backup_file.display());

    let compressed = pack_vault(vault_root, state_dir_name)?;
    let key = derive_key(vault_id, backup_passphrase);
    let encrypted = encrypt(&compressed, &key)?;

    std::fs::write(&backup_file, &encrypted)
        .context("escribir archivo de backup mensual")?;

    let hash = write_sha256_file(&backup_file)?;

    tracing::info!(
        "Backup mensual completado: {} ({} bytes) SHA256={}...",
        backup_file.display(), encrypted.len(), &hash[..16]
    );

    Ok(backup_file)
}

pub fn find_weekly_to_promote(
    weekly_path: &Path,
    interval_seconds: u64,
) -> anyhow::Result<Option<PathBuf>> {
    if !weekly_path.exists() { return Ok(None); }

    let threshold = std::time::Duration::from_secs(interval_seconds);

    let mut entries: Vec<_> = std::fs::read_dir(weekly_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();

    entries.sort_by_key(|e| e.file_name());

    for entry in entries.iter() {
        let metadata = entry.metadata()?;
        let modified = metadata.modified()?;
        let elapsed = modified.elapsed().unwrap_or_default();
        if elapsed >= threshold {
            return Ok(Some(entry.path()));
        }
    }

    Ok(None)
}

pub fn cleanup_weekly_after_promotion(weekly_path: &Path) -> anyhow::Result<()> {
    if !weekly_path.exists() { return Ok(()); }

    for entry in std::fs::read_dir(weekly_path)?.filter_map(|e| e.ok()) {
        if entry.path().is_dir() {
            std::fs::remove_dir_all(entry.path())?;
            tracing::info!("Limpiado semanal tras promoción: {}", entry.path().display());
        }
    }

    Ok(())
}
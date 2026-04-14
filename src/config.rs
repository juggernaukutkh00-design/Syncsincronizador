use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub vault_id: String,
    pub listen_addr: String,
    pub vault_path: String,
    pub state_dir: String,
    pub ignore_file: String,
    pub node_id: String,
    pub peer_addr: String,
    pub peer_cert_pin_sha256_hex: String,
    pub version_retention_days: u32,
    pub backup_interval_days: u32,
    pub scan_interval_seconds: u64,

    // Papelera
    pub trash_path: String,
    pub trash_retention_minutes: u64,
    pub trash_scan_interval_seconds: u64,
    pub is_primary_node: bool,
    pub tombstone_purge_interval_seconds: u64,

    // Backups semanales
    pub weekly_backup_path: String,
    pub weekly_interval_seconds: u64,

    // Backups mensuales
    pub monthly_archive_path: String,
    pub backup_passphrase: String,
    pub monthly_interval_seconds: u64,

    // API de recuperación
    pub api_addr: String,

    // Cola de operaciones
    // Máximo de ops en cola antes de descartar las más antiguas.
    // Recomendado producción: 1000. Pruebas: 100.
    pub ops_queue_max: i64,
}

pub fn load(path: &str) -> anyhow::Result<Config> {
    let s = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&s)?)
}
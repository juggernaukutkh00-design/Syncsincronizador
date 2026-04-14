use anyhow::Context;
use rusqlite::{params, Connection};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct FileRecord {
    pub path: String,
    pub file_exists: bool,
    pub size: u64,
    pub mtime: i64,
    pub hash_sha256: Option<String>,
    pub hash_state: String,
    pub last_seen_scan: i64,
    pub ignored: bool,
    pub lamport_ts: u64,  // lamport del último cambio conocido
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct QueuedOp {
    pub id: i64,
    pub op_type: String,
    pub origin: String,
    pub origin_node_id: String,
    pub lamport_ts: u64,
    pub path: String,
    pub size: u64,
    pub mtime: i64,
    pub hash_sha256: Option<String>,
    pub enqueued_at: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TrashEntry {
    pub id: i64,
    pub original_path: String,
    pub trash_name: String,
    pub origin_node_id: String,
    pub deleted_at: i64,
    pub expires_at: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PendingTrash {
    pub id: i64,
    pub original_path: String,
    pub trash_name: String,
    pub enqueued_at: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ConflictEntry {
    pub id: i64,
    pub path: String,
    pub conflict_file: String,   // nombre del archivo en .restauracion/
    pub local_lamport: u64,
    pub remote_lamport: u64,
    pub remote_node_id: String,
    pub detected_at: i64,
    pub resolved: bool,
}

pub fn open_db(db_path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open(db_path).context("open sqlite db")?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    migrate(&conn)?;
    Ok(conn)
}

fn migrate(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            file_exists INTEGER NOT NULL,
            size INTEGER NOT NULL,
            mtime INTEGER NOT NULL,
            hash_sha256 TEXT,
            hash_state TEXT NOT NULL,
            last_seen_scan INTEGER NOT NULL,
            ignored INTEGER NOT NULL,
            lamport_ts INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS peer_state (
            peer_id TEXT PRIMARY KEY,
            last_sync INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tombstones (
            path TEXT PRIMARY KEY,
            deleted_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ops_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            op_type TEXT NOT NULL,
            origin TEXT NOT NULL,
            origin_node_id TEXT NOT NULL,
            lamport_ts INTEGER NOT NULL,
            path TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime INTEGER NOT NULL,
            hash_sha256 TEXT,
            enqueued_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS node_clock (
            node_id TEXT PRIMARY KEY,
            lamport_ts INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trash (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_path TEXT NOT NULL,
            trash_name TEXT NOT NULL UNIQUE,
            origin_node_id TEXT NOT NULL,
            deleted_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS local_trash (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_path TEXT NOT NULL,
            trash_name TEXT NOT NULL UNIQUE,
            deleted_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pending_trash (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_path TEXT NOT NULL,
            trash_name TEXT NOT NULL UNIQUE,
            enqueued_at INTEGER NOT NULL
        );

        -- Conflictos detectados durante sincronización.
        -- El archivo perdedor se guarda en .restauracion/ del vault.
        -- resolved = 0 mientras el usuario no lo gestione.
        CREATE TABLE IF NOT EXISTS conflicts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL,
            conflict_file TEXT NOT NULL,
            local_lamport INTEGER NOT NULL,
            remote_lamport INTEGER NOT NULL,
            remote_node_id TEXT NOT NULL,
            detected_at INTEGER NOT NULL,
            resolved INTEGER NOT NULL DEFAULT 0
        );
        "#,
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Lamport clock
// ---------------------------------------------------------------------------

pub fn get_clock(conn: &Connection, node_id: &str) -> anyhow::Result<u64> {
    let result = conn.query_row(
        "SELECT lamport_ts FROM node_clock WHERE node_id = ?1",
        params![node_id],
        |row| row.get::<_, i64>(0),
    );
    match result {
        Ok(v) => Ok(v as u64),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
        Err(e) => Err(e.into()),
    }
}

pub fn tick_clock(conn: &Connection, node_id: &str) -> anyhow::Result<u64> {
    let current = get_clock(conn, node_id)?;
    let next = current + 1;
    conn.execute(
        r#"INSERT INTO node_clock (node_id, lamport_ts) VALUES (?1, ?2)
           ON CONFLICT(node_id) DO UPDATE SET lamport_ts = excluded.lamport_ts"#,
        params![node_id, next as i64],
    )?;
    Ok(next)
}

pub fn advance_clock(conn: &Connection, node_id: &str, remote_ts: u64) -> anyhow::Result<u64> {
    let current = get_clock(conn, node_id)?;
    let next = current.max(remote_ts) + 1;
    conn.execute(
        r#"INSERT INTO node_clock (node_id, lamport_ts) VALUES (?1, ?2)
           ON CONFLICT(node_id) DO UPDATE SET lamport_ts = excluded.lamport_ts"#,
        params![node_id, next as i64],
    )?;
    Ok(next)
}

// ---------------------------------------------------------------------------
// Tombstones
// ---------------------------------------------------------------------------

pub fn tombstone_insert(conn: &Connection, path: &str) -> anyhow::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO tombstones (path, deleted_at) VALUES (?1, ?2)",
        params![path, crate::util::now_epoch()],
    )?;
    Ok(())
}

pub fn tombstone_exists(conn: &Connection, path: &str) -> anyhow::Result<bool> {
    let result = conn.query_row(
        "SELECT 1 FROM tombstones WHERE path = ?1",
        params![path],
        |_| Ok(true),
    );
    match result {
        Ok(v) => Ok(v),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

pub fn tombstone_purge_old(conn: &Connection) -> anyhow::Result<usize> {
    let cutoff = crate::util::now_epoch() - (24 * 60 * 60);
    let deleted = conn.execute(
        "DELETE FROM tombstones WHERE deleted_at < ?1",
        params![cutoff],
    )?;
    Ok(deleted)
}

// ---------------------------------------------------------------------------
// Archivos
// ---------------------------------------------------------------------------

pub fn upsert_file(
    conn: &Connection,
    path: &str,
    file_exists: bool,
    size: u64,
    mtime: i64,
    hash_sha256: Option<&str>,
    hash_state: &str,
    last_seen_scan: i64,
    ignored: bool,
    lamport_ts: u64,
) -> anyhow::Result<()> {
    conn.execute(
        r#"INSERT INTO files(path, file_exists, size, mtime, hash_sha256,
           hash_state, last_seen_scan, ignored, lamport_ts)
           VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
           ON CONFLICT(path) DO UPDATE SET
               file_exists=excluded.file_exists,
               size=excluded.size,
               mtime=excluded.mtime,
               hash_sha256=excluded.hash_sha256,
               hash_state=excluded.hash_state,
               last_seen_scan=excluded.last_seen_scan,
               ignored=excluded.ignored,
               lamport_ts=excluded.lamport_ts"#,
        params![path, file_exists as i32, size as i64, mtime, hash_sha256,
                hash_state, last_seen_scan, ignored as i32, lamport_ts as i64],
    )?;
    Ok(())
}

pub fn get_file_record(conn: &Connection, path: &str) -> anyhow::Result<Option<FileRecord>> {
    let mut stmt = conn.prepare(
        "SELECT path, file_exists, size, mtime, hash_sha256, hash_state,
                last_seen_scan, ignored, lamport_ts
         FROM files WHERE path = ?1",
    )?;
    let mut rows = stmt.query(params![path])?;
    if let Some(row) = rows.next()? {
        Ok(Some(FileRecord {
            path: row.get(0)?,
            file_exists: row.get::<_, i64>(1)? != 0,
            size: row.get::<_, i64>(2)? as u64,
            mtime: row.get(3)?,
            hash_sha256: row.get(4)?,
            hash_state: row.get(5)?,
            last_seen_scan: row.get(6)?,
            ignored: row.get::<_, i64>(7)? != 0,
            lamport_ts: row.get::<_, i64>(8)? as u64,
        }))
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Cola de operaciones
// ---------------------------------------------------------------------------

pub fn enqueue_op(
    conn: &Connection,
    origin: &str,
    origin_node_id: &str,
    lamport_ts: u64,
    op_type: &str,
    path: &str,
    size: u64,
    mtime: i64,
    hash_sha256: Option<&str>,
) -> anyhow::Result<()> {
    conn.execute(
        r#"INSERT INTO ops_queue (op_type, origin, origin_node_id, lamport_ts,
           path, size, mtime, hash_sha256, enqueued_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
        params![op_type, origin, origin_node_id, lamport_ts as i64,
                path, size as i64, mtime, hash_sha256, crate::util::now_epoch()],
    )?;
    Ok(())
}

pub fn get_pending_ops(conn: &Connection, limit: i64) -> anyhow::Result<Vec<QueuedOp>> {
    let mut stmt = conn.prepare(
        r#"SELECT id, op_type, origin, origin_node_id, lamport_ts,
           path, size, mtime, hash_sha256, enqueued_at
           FROM ops_queue ORDER BY lamport_ts ASC, origin_node_id ASC LIMIT ?1"#,
    )?;
    let rows = stmt.query_map(params![limit], |row| {
        Ok(QueuedOp {
            id: row.get(0)?,
            op_type: row.get(1)?,
            origin: row.get(2)?,
            origin_node_id: row.get(3)?,
            lamport_ts: row.get::<_, i64>(4)? as u64,
            path: row.get(5)?,
            size: row.get::<_, i64>(6)? as u64,
            mtime: row.get(7)?,
            hash_sha256: row.get(8)?,
            enqueued_at: row.get(9)?,
        })
    })?;
    let mut ops = Vec::new();
    for row in rows { ops.push(row?); }
    Ok(ops)
}

pub fn delete_op(conn: &Connection, op_id: i64) -> anyhow::Result<()> {
    conn.execute("DELETE FROM ops_queue WHERE id = ?1", params![op_id])?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Conflictos
// ---------------------------------------------------------------------------

pub fn conflict_insert(
    conn: &Connection,
    path: &str,
    conflict_file: &str,
    local_lamport: u64,
    remote_lamport: u64,
    remote_node_id: &str,
) -> anyhow::Result<()> {
    conn.execute(
        r#"INSERT INTO conflicts
           (path, conflict_file, local_lamport, remote_lamport, remote_node_id, detected_at, resolved)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)"#,
        params![path, conflict_file, local_lamport as i64, remote_lamport as i64,
                remote_node_id, crate::util::now_epoch()],
    )?;
    Ok(())
}

pub fn conflict_list_unresolved(conn: &Connection) -> anyhow::Result<Vec<ConflictEntry>> {
    let mut stmt = conn.prepare(
        r#"SELECT id, path, conflict_file, local_lamport, remote_lamport,
           remote_node_id, detected_at, resolved
           FROM conflicts WHERE resolved = 0 ORDER BY detected_at DESC"#,
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(ConflictEntry {
            id: row.get(0)?,
            path: row.get(1)?,
            conflict_file: row.get(2)?,
            local_lamport: row.get::<_, i64>(3)? as u64,
            remote_lamport: row.get::<_, i64>(4)? as u64,
            remote_node_id: row.get(5)?,
            detected_at: row.get(6)?,
            resolved: row.get::<_, i64>(7)? != 0,
        })
    })?;
    let mut entries = Vec::new();
    for row in rows { entries.push(row?); }
    Ok(entries)
}

pub fn conflict_resolve(conn: &Connection, id: i64) -> anyhow::Result<()> {
    conn.execute(
        "UPDATE conflicts SET resolved = 1 WHERE id = ?1",
        params![id],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Papelera PC1
// ---------------------------------------------------------------------------

pub fn trash_insert(
    conn: &Connection,
    original_path: &str,
    trash_name: &str,
    origin_node_id: &str,
    retention_minutes: u64,
) -> anyhow::Result<()> {
    let now = crate::util::now_epoch();
    let expires_at = now + (retention_minutes as i64 * 60);
    conn.execute(
        "INSERT INTO trash (original_path, trash_name, origin_node_id, deleted_at, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![original_path, trash_name, origin_node_id, now, expires_at],
    )?;
    Ok(())
}

pub fn trash_get_expired(conn: &Connection) -> anyhow::Result<Vec<TrashEntry>> {
    let now = crate::util::now_epoch();
    let mut stmt = conn.prepare(
        "SELECT id, original_path, trash_name, origin_node_id, deleted_at, expires_at
         FROM trash WHERE expires_at <= ?1",
    )?;
    let rows = stmt.query_map(params![now], |row| {
        Ok(TrashEntry {
            id: row.get(0)?,
            original_path: row.get(1)?,
            trash_name: row.get(2)?,
            origin_node_id: row.get(3)?,
            deleted_at: row.get(4)?,
            expires_at: row.get(5)?,
        })
    })?;
    let mut entries = Vec::new();
    for row in rows { entries.push(row?); }
    Ok(entries)
}

pub fn trash_delete(conn: &Connection, id: i64) -> anyhow::Result<()> {
    conn.execute("DELETE FROM trash WHERE id = ?1", params![id])?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Papelera local PC2
// ---------------------------------------------------------------------------

pub fn local_trash_insert(
    conn: &Connection,
    original_path: &str,
    trash_name: &str,
    retention_minutes: u64,
) -> anyhow::Result<()> {
    let now = crate::util::now_epoch();
    let expires_at = now + (retention_minutes as i64 * 60);
    conn.execute(
        "INSERT INTO local_trash (original_path, trash_name, deleted_at, expires_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![original_path, trash_name, now, expires_at],
    )?;
    Ok(())
}

pub fn local_trash_get_expired(conn: &Connection) -> anyhow::Result<Vec<TrashEntry>> {
    let now = crate::util::now_epoch();
    let mut stmt = conn.prepare(
        "SELECT id, original_path, trash_name, '' as origin, deleted_at, expires_at
         FROM local_trash WHERE expires_at <= ?1",
    )?;
    let rows = stmt.query_map(params![now], |row| {
        Ok(TrashEntry {
            id: row.get(0)?,
            original_path: row.get(1)?,
            trash_name: row.get(2)?,
            origin_node_id: row.get(3)?,
            deleted_at: row.get(4)?,
            expires_at: row.get(5)?,
        })
    })?;
    let mut entries = Vec::new();
    for row in rows { entries.push(row?); }
    Ok(entries)
}

pub fn local_trash_delete(conn: &Connection, id: i64) -> anyhow::Result<()> {
    conn.execute("DELETE FROM local_trash WHERE id = ?1", params![id])?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Pending trash PC2
// ---------------------------------------------------------------------------

pub fn pending_trash_insert(
    conn: &Connection,
    original_path: &str,
    trash_name: &str,
) -> anyhow::Result<()> {
    conn.execute(
        "INSERT INTO pending_trash (original_path, trash_name, enqueued_at)
         VALUES (?1, ?2, ?3)",
        params![original_path, trash_name, crate::util::now_epoch()],
    )?;
    Ok(())
}

pub fn pending_trash_get_all(conn: &Connection) -> anyhow::Result<Vec<PendingTrash>> {
    let mut stmt = conn.prepare(
        "SELECT id, original_path, trash_name, enqueued_at
         FROM pending_trash ORDER BY id ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(PendingTrash {
            id: row.get(0)?,
            original_path: row.get(1)?,
            trash_name: row.get(2)?,
            enqueued_at: row.get(3)?,
        })
    })?;
    let mut entries = Vec::new();
    for row in rows { entries.push(row?); }
    Ok(entries)
}

pub fn pending_trash_delete(conn: &Connection, id: i64) -> anyhow::Result<()> {
    conn.execute("DELETE FROM pending_trash WHERE id = ?1", params![id])?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Gestión de ops_queue — límite y coalescing
// ---------------------------------------------------------------------------

/// Devuelve el número actual de ops en cola.
pub fn ops_queue_count(conn: &Connection) -> anyhow::Result<i64> {
    let count = conn.query_row(
        "SELECT COUNT(*) FROM ops_queue", [], |r| r.get(0)
    )?;
    Ok(count)
}

/// Coalescing: si hay varias ops del mismo path, mantiene solo la más reciente.
/// Reduce la cola cuando un archivo cambia muchas veces offline.
/// Devuelve el número de ops eliminadas.
pub fn ops_queue_coalesce(conn: &Connection) -> anyhow::Result<usize> {
    // Para cada path con más de una op, borra todas menos la de mayor lamport_ts
    let deleted = conn.execute(
        r#"DELETE FROM ops_queue
           WHERE id NOT IN (
               SELECT MAX(id)
               FROM ops_queue
               GROUP BY path, op_type
           )
           AND op_type IN ('CREATE', 'UPDATE')"#,
        [],
    )?;
    Ok(deleted)
}

/// Limita la cola a max_ops entradas.
/// Si se supera el límite, borra las más antiguas (menor lamport_ts).
/// Devuelve el número de ops eliminadas.
pub fn ops_queue_trim(conn: &Connection, max_ops: i64) -> anyhow::Result<usize> {
    let count = ops_queue_count(conn)?;
    if count <= max_ops {
        return Ok(0);
    }

    let to_delete = count - max_ops;
    let deleted = conn.execute(
        r#"DELETE FROM ops_queue WHERE id IN (
               SELECT id FROM ops_queue
               ORDER BY lamport_ts ASC, id ASC
               LIMIT ?1
           )"#,
        params![to_delete],
    )?;

    Ok(deleted)
}
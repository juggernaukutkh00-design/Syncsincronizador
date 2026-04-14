use anyhow::Context;
use std::path::{Path, PathBuf};
use tokio::net::TcpStream;
use crate::net::{read_msg, write_msg};
use crate::{
    backup,
    config::Config,
    db,
    proto::{FileOp, Hello, Msg, TrashOp},
    util,
};

/// Límite máximo de tamaño de archivo para sincronización (100MB).
/// Archivos más grandes se ignoran con un warning.
const MAX_FILE_SIZE: u64 = 100 * 1024 * 1024;

/// Límite máximo de tamaño de TrashOp (100MB).
const MAX_TRASH_OP_SIZE: u64 = 100 * 1024 * 1024;

pub async fn flush_ops_to_peer(
    cfg: &Config,
    conn: &rusqlite::Connection,
    vault_root: &Path,
) -> anyhow::Result<()> {
    let ops: Vec<_> = db::get_pending_ops(conn, 100)?
        .into_iter()
        .filter(|op| op.origin != "remote")
        .collect();

    let pending_trash = if !cfg.is_primary_node {
        db::pending_trash_get_all(conn)?
    } else {
        vec![]
    };

    if ops.is_empty() && pending_trash.is_empty() {
        return Ok(());
    }

    let mut stream = TcpStream::connect(&cfg.peer_addr)
        .await
        .with_context(|| format!("connect to peer {}", cfg.peer_addr))?;

    let hello = Msg::Hello(Hello {
        vault_id: cfg.vault_id.clone(),
        peer_id: cfg.node_id.clone(),
        hostname: util::hostname(),
    });

    write_msg(&mut stream, &hello).await?;
    let resp = read_msg(&mut stream).await?;

    match resp {
        Msg::HelloAck { vault_id, .. } if vault_id == cfg.vault_id => {}
        Msg::Error { message } => anyhow::bail!("peer error durante hello: {}", message),
        other => anyhow::bail!("respuesta inesperada al hello: {:?}", other),
    }

    // Enviar ops normales
    for op in ops {
        let msg = match op.op_type.as_str() {
            "CREATE" | "UPDATE" => {
                let full_path = vault_root.join(&op.path);

                // Comprobar tamaño antes de leer — evita cargar archivos enormes en RAM
                let size = match std::fs::metadata(&full_path) {
                    Ok(m) => m.len(),
                    Err(_) => {
                        tracing::warn!("archivo no encontrado al enviar op {}: {}", op.id, op.path);
                        db::delete_op(conn, op.id)?;
                        continue;
                    }
                };

                if size > MAX_FILE_SIZE {
                    tracing::warn!(
                        "archivo demasiado grande para sincronizar ({} bytes > {} bytes): {}",
                        size, MAX_FILE_SIZE, op.path
                    );
                    db::delete_op(conn, op.id)?;
                    continue;
                }

                // Leer en spawn_blocking para no bloquear el runtime async
                let full_path_clone = full_path.clone();
                let data = match tokio::task::spawn_blocking(move || {
                    std::fs::read(&full_path_clone)
                }).await {
                    Ok(Ok(d)) => d,
                    Ok(Err(_)) | Err(_) => {
                        tracing::warn!("error leyendo archivo op {}: {}", op.id, op.path);
                        db::delete_op(conn, op.id)?;
                        continue;
                    }
                };

                Msg::PushOp(FileOp {
                    op_id: op.id,
                    op_type: op.op_type.clone(),
                    path: op.path.clone(),
                    size: op.size,
                    mtime: op.mtime,
                    data: Some(data),
                    lamport_ts: op.lamport_ts,
                    origin_node_id: op.origin_node_id.clone(),
                })
            }

            "TRASH" | "DELETE" | "CREATE_DIR" | "DELETE_DIR" => Msg::PushOp(FileOp {
                op_id: op.id,
                op_type: op.op_type.clone(),
                path: op.path.clone(),
                size: 0,
                mtime: 0,
                data: None,
                lamport_ts: op.lamport_ts,
                origin_node_id: op.origin_node_id.clone(),
            }),

            other => anyhow::bail!("op_type no soportado: {}", other),
        };

        write_msg(&mut stream, &msg).await?;
        let ack = read_msg(&mut stream).await?;

        match ack {
            Msg::PushOpAck { op_id, ok: true, .. } if op_id == op.id => {
                db::delete_op(conn, op.id)?;
                tracing::info!("op {} sincronizada [lamport={}] [from={}]: {}",
                    op.id, op.lamport_ts, op.origin_node_id, op.path);
            }
            Msg::PushOpAck { op_id, ok: false, message } if op_id == op.id => {
                tracing::warn!("peer rechazó op {}: {}", op.id, message);
                break;
            }
            other => anyhow::bail!("ack inesperado para op {}: {:?}", op.id, other),
        }
    }

    // PC2: enviar archivos de pending_trash a PC1
    let trash_dir = PathBuf::from(&cfg.trash_path);
    for (i, entry) in pending_trash.iter().enumerate() {
        let file_path = trash_dir.join(&entry.trash_name);

        // Comprobar tamaño antes de leer
        let size = match std::fs::metadata(&file_path) {
            Ok(m) => m.len(),
            Err(e) => {
                tracing::warn!("pending_trash no encontrado {}: {:?}", entry.trash_name, e);
                db::pending_trash_delete(conn, entry.id)?;
                continue;
            }
        };

        if size > MAX_TRASH_OP_SIZE {
            tracing::warn!(
                "pending_trash demasiado grande ({} bytes): {} — borrando sin enviar",
                size, entry.original_path
            );
            backup::secure_delete(&file_path)?;
            db::pending_trash_delete(conn, entry.id)?;
            continue;
        }

        // Leer en spawn_blocking
        let file_path_clone = file_path.clone();
        let data = match tokio::task::spawn_blocking(move || {
            std::fs::read(&file_path_clone)
        }).await {
            Ok(Ok(d)) => d,
            Ok(Err(e)) => {
                tracing::warn!("error leyendo pending_trash {}: {:?}", entry.trash_name, e);
                db::pending_trash_delete(conn, entry.id)?;
                continue;
            }
            Err(e) => {
                tracing::warn!("spawn_blocking falló para pending_trash: {:?}", e);
                continue;
            }
        };

        let lamport_ts = db::tick_clock(conn, &cfg.node_id)?;
        let msg = Msg::TrashOp(TrashOp {
            op_id: (1000000 + i) as i64,
            original_path: entry.original_path.clone(),
            trash_name: entry.trash_name.clone(),
            data,
            lamport_ts,
            origin_node_id: cfg.node_id.clone(),
        });

        write_msg(&mut stream, &msg).await?;
        let ack = read_msg(&mut stream).await?;

        match ack {
            Msg::TrashOpAck { ok: true, .. } => {
                backup::secure_delete(&file_path)?;
                db::pending_trash_delete(conn, entry.id)?;
                tracing::info!("pending_trash enviado y borrado: {}", entry.original_path);
            }
            Msg::TrashOpAck { ok: false, message, .. } => {
                tracing::warn!("PC1 rechazó TrashOp {}: {}", entry.original_path, message);
            }
            other => anyhow::bail!("ack inesperado para TrashOp: {:?}", other),
        }
    }

    Ok(())
}
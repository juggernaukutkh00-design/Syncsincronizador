use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub vault_id: String,
    pub hostname: String,
    pub peer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOp {
    pub op_id: i64,
    pub op_type: String,
    pub path: String,
    pub size: u64,
    pub mtime: i64,
    pub data: Option<Vec<u8>>,
    pub lamport_ts: u64,
    pub origin_node_id: String,
}

/// Op especial para enviar un archivo a la papelera del nodo principal.
/// PC2 lo usa cuando borra algo offline y luego reconecta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrashOp {
    pub op_id: i64,
    pub original_path: String,
    pub trash_name: String,   // nombre aleatorio con el que se guardó en pending_trash
    pub data: Vec<u8>,        // contenido del archivo
    pub lamport_ts: u64,
    pub origin_node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Msg {
    Hello(Hello),
    HelloAck {
        vault_id: String,
        peer_id: String,
    },

    PushOp(FileOp),
    PushOpAck {
        op_id: i64,
        ok: bool,
        message: String,
    },

    // PC2 envía archivo borrado a la papelera de PC1
    TrashOp(TrashOp),
    TrashOpAck {
        op_id: i64,
        ok: bool,
        message: String,
    },

    Error {
        message: String,
    },
}
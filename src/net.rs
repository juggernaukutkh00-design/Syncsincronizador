// src/net.rs

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::proto::Msg;

pub async fn write_msg<W: AsyncWriteExt + Unpin>(w: &mut W, msg: &Msg) -> anyhow::Result<()> {
    let bytes = serde_cbor::to_vec(msg)?;
    let len = (bytes.len() as u32).to_be_bytes();
    w.write_all(&len).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_msg<R: AsyncReadExt + Unpin>(r: &mut R) -> anyhow::Result<Msg> {
    let mut lenb = [0u8; 4];
    r.read_exact(&mut lenb).await?;
    let len = u32::from_be_bytes(lenb) as usize;

    if len > 32 * 1024 * 1024 {
        anyhow::bail!("message too large: {} bytes", len);
    }

    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(serde_cbor::from_slice(&buf)?)
}
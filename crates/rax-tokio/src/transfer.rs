use std::collections::HashMap;
use std::io;

use rax::NodeMessage;
use rax::attachment::{ChunkPart, MAX_ATTACHMENT_BYTES, MAX_TRANSFER_CHUNK_BYTES, TransferChunk};
use rax::id::TransferId;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::connection::Core;
use crate::error::SendError;

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error(
        "rax-tokio: an attachment of {size} bytes exceeds the {MAX_ATTACHMENT_BYTES}-byte ceiling"
    )]
    TooLarge { size: u64 },
    /// The receiver was told with a `failed` chunk, so it drops what it holds.
    #[error("rax-tokio: read attachment: {0}")]
    Read(#[source] io::Error),
    #[error("rax-tokio: attachment declared {declared} bytes, the reader gave {read}")]
    SizeMismatch { declared: u64, read: u64 },
    #[error(transparent)]
    Send(#[from] SendError),
}

pub(crate) async fn send<R>(
    core: &Core,
    mut reader: R,
    size: u64,
) -> Result<TransferId, TransferError>
where
    R: AsyncRead + Unpin,
{
    if size > MAX_ATTACHMENT_BYTES {
        return Err(TransferError::TooLarge { size });
    }
    let generation = core.generation();
    let transfer_id = TransferId(format!("t{:016x}", rand::random::<u64>()));
    let chunk = |seq, part| NodeMessage::Chunk {
        body: TransferChunk {
            transfer_id: transfer_id.clone(),
            seq,
            part,
        },
    };
    let limit = MAX_TRANSFER_CHUNK_BYTES as u64;
    let (mut seq, mut read) = (0u32, 0u64);
    let failure = loop {
        let mut data = Vec::new();
        let n = match (&mut reader).take(limit).read_to_end(&mut data).await {
            Ok(n) => n as u64,
            Err(err) => break TransferError::Read(err),
        };
        read += n;
        if read > size {
            break TransferError::SizeMismatch {
                declared: size,
                read,
            };
        }
        if n > 0 {
            core.send_in(generation, &chunk(seq, ChunkPart::Data(data)))
                .await?;
            seq += 1;
        }
        if n < limit {
            if read != size {
                break TransferError::SizeMismatch {
                    declared: size,
                    read,
                };
            }
            core.send_in(generation, &chunk(seq, ChunkPart::End))
                .await?;
            return Ok(transfer_id);
        }
    };
    core.send_in(
        generation,
        &chunk(seq, ChunkPart::Failed(failure.to_string())),
    )
    .await?;
    Err(failure)
}

pub(crate) enum Settled {
    Complete {
        transfer_id: TransferId,
        bytes: Vec<u8>,
    },
    Failed {
        transfer_id: TransferId,
        reason: String,
    },
}

enum Inbound {
    Receiving { next: u32, bytes: Vec<u8> },
    Failed,
}

#[derive(Default)]
pub(crate) struct Reassembly {
    transfers: HashMap<TransferId, Inbound>,
}

impl Reassembly {
    pub(crate) fn accept(&mut self, chunk: TransferChunk) -> Option<Settled> {
        let TransferChunk {
            transfer_id,
            seq,
            part,
        } = chunk;
        let terminal = !matches!(part, ChunkPart::Data(_));
        let inbound = self
            .transfers
            .entry(transfer_id.clone())
            .or_insert(Inbound::Receiving {
                next: 0,
                bytes: Vec::new(),
            });
        let Inbound::Receiving { next, bytes } = inbound else {
            if terminal {
                self.transfers.remove(&transfer_id);
            }
            return None;
        };
        let failure = if seq != *next {
            Some(format!("expected chunk {next}, received {seq}"))
        } else {
            match part {
                ChunkPart::Data(data) => {
                    let total = bytes.len() as u64 + data.len() as u64;
                    if data.len() > MAX_TRANSFER_CHUNK_BYTES {
                        Some(format!("chunk {seq} holds {} bytes", data.len()))
                    } else if total > MAX_ATTACHMENT_BYTES {
                        Some(format!("{total} bytes exceed the attachment ceiling"))
                    } else {
                        bytes.extend_from_slice(&data);
                        *next += 1;
                        None
                    }
                }
                ChunkPart::End => {
                    let bytes = std::mem::take(bytes);
                    self.transfers.remove(&transfer_id);
                    return Some(Settled::Complete { transfer_id, bytes });
                }
                ChunkPart::Failed(reason) => {
                    self.transfers.remove(&transfer_id);
                    return Some(Settled::Failed {
                        transfer_id,
                        reason,
                    });
                }
            }
        };
        let reason = failure?;
        if terminal {
            self.transfers.remove(&transfer_id);
        } else {
            self.transfers.insert(transfer_id.clone(), Inbound::Failed);
        }
        Some(Settled::Failed {
            transfer_id,
            reason,
        })
    }
}

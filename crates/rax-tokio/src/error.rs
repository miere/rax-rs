use rax::LinkError;

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("rax-tokio: the link is closed")]
    Closed,
    /// The peer started a fresh link, so whatever this message belonged to is gone on its side.
    #[error("rax-tokio: the link was reset before the message was sent")]
    LinkReset,
    #[error("rax-tokio: a message of {size} bytes cannot fit in a frame")]
    TooLarge { size: usize },
    #[error("rax-tokio: encode message: {0}")]
    Encode(#[from] rax::message::PayloadError),
    #[error(transparent)]
    Link(#[from] LinkError),
}

#[derive(Debug, thiserror::Error)]
pub enum CallError {
    #[error("rax-tokio: the peer faulted the call: {0}")]
    Fault(rax::Error),
    /// The reply can never arrive: the peer no longer holds the link the call was sent on.
    #[error("rax-tokio: the link was reset before the call was answered")]
    LinkReset,
    #[error("rax-tokio: the link closed before the call was answered")]
    Closed,
    /// The call was answered, but the bytes its reply announced did not arrive whole.
    #[error("rax-tokio: the transfer failed: {0}")]
    Transfer(String),
    #[error(transparent)]
    Unsendable(SendError),
}

impl From<SendError> for CallError {
    fn from(err: SendError) -> Self {
        match err {
            SendError::Closed => Self::Closed,
            SendError::LinkReset => Self::LinkReset,
            other => Self::Unsendable(other),
        }
    }
}

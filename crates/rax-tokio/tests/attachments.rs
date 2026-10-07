#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use rax::attachment::{AttachmentReceipt, MAX_ATTACHMENT_BYTES, ReceiptOutcome};
use rax_tokio::TransferError;
use rax_tokio::gateway::LinkEvent;
use rax_tokio::node::NodeEvent;
use support::*;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

#[tokio::test(flavor = "multi_thread")]
async fn a_ten_mebibyte_attachment_arrives_byte_identical() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut link_events,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let bytes: Vec<u8> = (0..10 * (1 << 20) + 12_345u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let sent = within(node.send_attachment(&bytes)).await.unwrap();
    let LinkEvent::Attachment {
        transfer_id,
        bytes: received,
    } = next_link(&mut link_events).await
    else {
        panic!("expected an attachment")
    };
    assert_eq!(transfer_id, sent);
    assert!(received == bytes, "the attachment was altered in transit");
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_gateway_tells_the_node_what_became_of_an_attachment() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        mut link_events,
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let sent = within(node.send_attachment(b"report")).await.unwrap();
    let LinkEvent::Attachment { transfer_id, .. } = next_link(&mut link_events).await else {
        panic!("expected an attachment")
    };
    let receipt = AttachmentReceipt {
        transfer_id,
        outcome: ReceiptOutcome::Failed,
        reason: Some("the upload was refused".into()),
    };
    link.receipt(receipt.clone()).await.unwrap();
    let NodeEvent::Receipt(received) = next(&mut node_events).await else {
        panic!("expected a receipt")
    };
    assert_eq!(received.transfer_id, sent);
    assert_eq!(received, receipt);
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_attachment_over_the_ceiling_is_refused_before_sending() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair { node, .. } = connect(&mut new_links, node_config(server.local_addr())).await;

    let refused = node
        .send_attachment_from(tokio::io::empty(), MAX_ATTACHMENT_BYTES + 1)
        .await;
    assert!(matches!(refused, Err(TransferError::TooLarge { .. })));
    node.close().await;
}

struct Broken;

impl AsyncRead for Broken {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::other("disk went away")))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reader_that_fails_tells_the_gateway_to_drop_the_transfer() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut link_events,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reader = (&b"the first few bytes"[..]).chain(Broken);
    let failed = node.send_attachment_from(reader, 1024).await;
    assert!(matches!(failed, Err(TransferError::Read(_))));
    let LinkEvent::TransferFailed { reason, .. } = next_link(&mut link_events).await else {
        panic!("expected a failed transfer")
    };
    assert!(reason.contains("disk went away"), "{reason}");
    node.close().await;
}

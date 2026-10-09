#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use rax::attachment::MAX_ATTACHMENT_BYTES;
use rax::id::RequestId;
use rax::local_file::ReadLocalFile;
use rax::{Error, ErrorKind, GatewayCall};
use rax_tokio::gateway::{GatewayLink, LinkEvent, LocalFile};
use rax_tokio::node::{self, NodeEvent, NodeEvents};
use rax_tokio::{CallError, TransferError};
use support::*;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::task::JoinHandle;

fn read(id: &str, max_bytes: Option<u64>) -> ReadLocalFile {
    ReadLocalFile {
        id: id.into(),
        max_bytes,
    }
}

fn start_read(link: &GatewayLink, read: ReadLocalFile) -> JoinHandle<Result<LocalFile, CallError>> {
    let link = link.clone();
    tokio::spawn(async move { link.read_local_file(read).await })
}

async fn next_read(events: &mut NodeEvents) -> (RequestId, ReadLocalFile) {
    let NodeEvent::Request {
        id,
        call: GatewayCall::ReadLocalFile(read),
    } = next(events).await
    else {
        panic!("expected local_file.read")
    };
    (id, read)
}

fn named(name: &str, mimetype: Option<&str>) -> node::LocalFile {
    node::LocalFile {
        name: Some(name.to_owned()),
        mimetype: mimetype.map(str::to_owned),
    }
}

fn pattern(len: u32) -> Vec<u8> {
    (0..len)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ten_mebibyte_file_arrives_byte_identical_with_its_name() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let bytes = pattern(10 * (1 << 20) + 12_345);
    let reading = start_read(&link, read("lf_report", None));
    let (id, asked) = next_read(&mut node_events).await;
    assert_eq!(asked.id.0, "lf_report");
    let described = named("report.pdf", Some("application/pdf"));
    within(node.serve_local_file(id, &asked, &bytes, described))
        .await
        .unwrap();

    let file = within(reading).await.unwrap().unwrap();
    assert_eq!(file.name.as_deref(), Some("report.pdf"));
    assert_eq!(file.mimetype.as_deref(), Some("application/pdf"));
    assert!(file.bytes == bytes, "the file was altered in transit");
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_over_the_readers_limit_is_refused_before_any_bytes() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reading = start_read(&link, read("lf_big", Some(10)));
    let (id, asked) = next_read(&mut node_events).await;
    let refused = node
        .serve_local_file(id, &asked, &[0; 11], Default::default())
        .await;
    assert!(matches!(
        refused,
        Err(TransferError::OverReadLimit {
            size: 11,
            max_bytes: 10
        })
    ));

    let Err(CallError::Fault(fault)) = within(reading).await.unwrap() else {
        panic!("expected a fault")
    };
    assert_eq!(fault.kind, ErrorKind::TooLarge);
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_over_the_ceiling_is_refused_before_any_bytes() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reading = start_read(&link, read("lf_huge", None));
    let (id, asked) = next_read(&mut node_events).await;
    let refused = node
        .serve_local_file_from(
            id,
            &asked,
            tokio::io::empty(),
            MAX_ATTACHMENT_BYTES + 1,
            Default::default(),
        )
        .await;
    assert!(matches!(refused, Err(TransferError::TooLarge { .. })));

    let Err(CallError::Fault(fault)) = within(reading).await.unwrap() else {
        panic!("expected a fault")
    };
    assert_eq!(fault.kind, ErrorKind::TooLarge);
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_declares_more_than_the_reader_asked_for_fails_the_read() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reading = start_read(&link, read("lf_big", Some(4)));
    let (id, _) = next_read(&mut node_events).await;
    let unlimited = read("lf_big", None);
    node.serve_local_file(id, &unlimited, b"too many bytes", Default::default())
        .await
        .unwrap();

    let Err(CallError::Transfer(reason)) = within(reading).await.unwrap() else {
        panic!("expected a failed transfer")
    };
    assert!(reason.contains("over the 4-byte limit"), "{reason}");
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_identifier_the_node_does_not_hold_is_faulted_not_found() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reading = start_read(&link, read("lf_guessed", None));
    let (id, _) = next_read(&mut node_events).await;
    node.fault(id, Error::new(ErrorKind::NotFound, "no such file"))
        .await
        .unwrap();

    let Err(CallError::Fault(fault)) = within(reading).await.unwrap() else {
        panic!("expected a fault")
    };
    assert_eq!(fault.kind, ErrorKind::NotFound);
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
async fn a_file_that_fails_mid_transfer_fails_the_read() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reading = start_read(&link, read("lf_report", None));
    let (id, asked) = next_read(&mut node_events).await;
    let source = (&b"the first few bytes"[..]).chain(Broken);
    let served = node
        .serve_local_file_from(id, &asked, source, 1024, Default::default())
        .await;
    assert!(matches!(served, Err(TransferError::Read(_))));

    let Err(CallError::Transfer(reason)) = within(reading).await.unwrap() else {
        panic!("expected a failed transfer")
    };
    assert!(reason.contains("disk went away"), "{reason}");
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_reads_each_get_their_own_bytes() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let first = start_read(&link, read("lf_a", None));
    let (first_id, first_read) = next_read(&mut node_events).await;
    let second = start_read(&link, read("lf_b", None));
    let (second_id, second_read) = next_read(&mut node_events).await;
    node.serve_local_file(second_id, &second_read, b"bravo", Default::default())
        .await
        .unwrap();
    node.serve_local_file(first_id, &first_read, b"alpha", Default::default())
        .await
        .unwrap();

    assert_eq!(within(first).await.unwrap().unwrap().bytes, b"alpha");
    assert_eq!(within(second).await.unwrap().unwrap().bytes, b"bravo");
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_fails_when_the_link_closes_under_it() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reading = start_read(&link, read("lf_report", None));
    next_read(&mut node_events).await;
    link.close().await;

    assert!(matches!(
        within(reading).await.unwrap(),
        Err(CallError::Closed)
    ));
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_and_an_attachment_on_one_link_do_not_cross() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        mut node_events,
        link,
        mut link_events,
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reading = start_read(&link, read("lf_report", None));
    let (id, asked) = next_read(&mut node_events).await;
    let pushed = within(node.send_attachment(b"pushed")).await.unwrap();
    node.serve_local_file(id, &asked, b"pulled", Default::default())
        .await
        .unwrap();

    let LinkEvent::Attachment { transfer_id, bytes } = next_link(&mut link_events).await else {
        panic!("expected an attachment")
    };
    assert_eq!(transfer_id, pushed);
    assert_eq!(bytes, b"pushed");
    assert_eq!(within(reading).await.unwrap().unwrap().bytes, b"pulled");
    node.close().await;
}

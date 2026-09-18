#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use rax::id::RequestId;
use rax::resource::ReadResource;
use rax::{Error, ErrorKind, NodeCall};
use rax_tokio::gateway::{LinkEvent, LinkEvents};
use rax_tokio::node::{NodeEvent, NodeHandle, Resource};
use rax_tokio::{CallError, TransferError};
use support::*;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::task::JoinHandle;

fn read(uri: &str, max_bytes: Option<u64>) -> ReadResource {
    ReadResource {
        uri: uri.to_owned(),
        max_bytes,
    }
}

fn start_read(node: &NodeHandle, read: ReadResource) -> JoinHandle<Result<Resource, CallError>> {
    let node = node.clone();
    tokio::spawn(async move { node.read_resource(read).await })
}

async fn next_read(events: &mut LinkEvents) -> (RequestId, ReadResource) {
    let LinkEvent::Request {
        id,
        call: NodeCall::ReadResource(read),
    } = next_link(events).await
    else {
        panic!("expected resource.read")
    };
    (id, read)
}

fn pattern(len: u32) -> Vec<u8> {
    (0..len)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resource_arrives_byte_identical_with_its_declared_type() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        link,
        mut link_events,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let bytes = pattern(9 * (1 << 20) + 4_321);
    let reading = start_read(&node, read("gateway://files/F1", None));
    let (id, asked) = next_read(&mut link_events).await;
    assert_eq!(asked.uri, "gateway://files/F1");
    let served = link
        .serve_resource(id, &asked, &bytes, Some("application/pdf".into()))
        .await
        .unwrap();

    let resource = within(reading).await.unwrap().unwrap();
    assert_eq!(resource.transfer_id, served);
    assert_eq!(resource.mimetype.as_deref(), Some("application/pdf"));
    assert!(
        resource.bytes == bytes,
        "the resource was altered in transit"
    );
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resource_over_the_readers_limit_is_refused_before_any_bytes() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        link,
        mut link_events,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reading = start_read(&node, read("gateway://files/F1", Some(10)));
    let (id, asked) = next_read(&mut link_events).await;
    let refused = link.serve_resource(id, &asked, &[0; 11], None).await;
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
async fn a_uri_the_gateway_never_sent_is_faulted_forbidden() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        link,
        mut link_events,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reading = start_read(&node, read("gateway://files/guessed", None));
    let (id, _) = next_read(&mut link_events).await;
    link.fault(id, Error::new(ErrorKind::Forbidden, "not yours"))
        .await
        .unwrap();

    let Err(CallError::Fault(fault)) = within(reading).await.unwrap() else {
        panic!("expected a fault")
    };
    assert_eq!(fault.kind, ErrorKind::Forbidden);
    node.close().await;
}

struct Broken;

impl AsyncRead for Broken {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::other("slack went away")))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_that_fails_mid_transfer_fails_the_read() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        link,
        mut link_events,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let reading = start_read(&node, read("gateway://files/F1", None));
    let (id, asked) = next_read(&mut link_events).await;
    let source = (&b"the first few bytes"[..]).chain(Broken);
    let served = link
        .serve_resource_from(id, &asked, source, 1024, None)
        .await;
    assert!(matches!(served, Err(TransferError::Read(_))));

    let Err(CallError::Transfer(reason)) = within(reading).await.unwrap() else {
        panic!("expected a failed transfer")
    };
    assert!(reason.contains("slack went away"), "{reason}");
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_reads_each_get_their_own_bytes() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let Pair {
        node,
        link,
        mut link_events,
        ..
    } = connect(&mut new_links, node_config(server.local_addr())).await;

    let first = start_read(&node, read("gateway://files/A", None));
    let (first_id, first_read) = next_read(&mut link_events).await;
    let second = start_read(&node, read("gateway://files/B", None));
    let (second_id, second_read) = next_read(&mut link_events).await;
    link.serve_resource(second_id, &second_read, b"bravo", None)
        .await
        .unwrap();
    link.serve_resource(first_id, &first_read, b"alpha", None)
        .await
        .unwrap();

    assert_eq!(within(first).await.unwrap().unwrap().bytes, b"alpha");
    assert_eq!(within(second).await.unwrap().unwrap().bytes, b"bravo");
    node.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_served_while_the_node_is_away_arrives_after_it_resumes() {
    let (server, mut new_links) = gateway(gateway_config()).await;
    let proxy = Proxy::start(server.local_addr()).await;
    let Pair {
        node,
        mut node_events,
        link,
        mut link_events,
    } = connect(&mut new_links, node_config(proxy.addr())).await;

    let reading = start_read(&node, read("gateway://files/F1", None));
    let (id, asked) = next_read(&mut link_events).await;
    proxy.sever();
    let bytes = pattern(3 * (1 << 20));
    let serving = tokio::spawn({
        let (link, bytes) = (link.clone(), bytes.clone());
        async move { link.serve_resource(id, &asked, &bytes, None).await }
    });

    loop {
        if let NodeEvent::Resumed = next(&mut node_events).await {
            break;
        }
    }
    within(serving).await.unwrap().unwrap();
    assert!(within(reading).await.unwrap().unwrap().bytes == bytes);
    node.close().await;
}

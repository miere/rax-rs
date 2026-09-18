use rax::frame::{Frame, ResumeOutcome};
use rax::link::{Link, LinkConfig, LinkError, NO_EPOCH, Received};
use serde_json::value::{RawValue, to_raw_value};

fn payload(text: &str) -> Box<RawValue> {
    to_raw_value(text).unwrap()
}

fn link(epoch: u64) -> Link {
    Link::new(LinkConfig {
        epoch,
        ack_threshold: 2,
        ..Default::default()
    })
}

fn consume(receiver: &mut Link, frame: Frame) -> Option<Frame> {
    match receiver.receive(frame).unwrap() {
        Received::Deliver { delivery, .. } => receiver.consumed(delivery),
        other => panic!("expected a delivery, got {other:?}"),
    }
}

#[test]
fn acknowledges_only_after_consumption_and_releases_the_buffer() {
    let (mut sender, mut receiver) = (link(1), link(1));
    let first = sender.send(payload("a")).unwrap();
    let second = sender.send(payload("b")).unwrap();
    assert_eq!(sender.pending().0, 2);

    assert!(consume(&mut receiver, first).is_none());
    let ack = consume(&mut receiver, second).expect("threshold of two reached");
    assert!(matches!(
        sender.receive(ack).unwrap(),
        Received::Acknowledged
    ));
    assert_eq!(sender.pending(), (0, 0));
}

#[test]
fn refuses_a_second_frame_while_one_is_being_consumed() {
    let (mut sender, mut receiver) = (link(1), link(1));
    let first = sender.send(payload("a")).unwrap();
    let second = sender.send(payload("b")).unwrap();
    let Received::Deliver { delivery, .. } = receiver.receive(first).unwrap() else {
        panic!()
    };
    assert_eq!(
        receiver.receive(second).unwrap_err(),
        LinkError::DeliveryOutstanding { pending: 1 }
    );
    let _ = receiver.consumed(delivery);
}

#[test]
fn a_gap_fails_the_link() {
    let (mut sender, mut receiver) = (link(1), link(1));
    let _lost = sender.send(payload("a")).unwrap();
    let second = sender.send(payload("b")).unwrap();
    assert_eq!(
        receiver.receive(second).unwrap_err(),
        LinkError::SequenceGap { want: 1, got: 2 }
    );
}

#[test]
fn window_is_bytes_but_never_blocks_an_empty_buffer() {
    let mut sender = Link::new(LinkConfig {
        window_bytes: 4,
        ..Default::default()
    });
    sender.send(payload("far larger than four bytes")).unwrap();
    assert!(matches!(
        sender.send(payload("x")),
        Err(LinkError::WindowFull { .. })
    ));
}

#[test]
fn resume_replays_unacknowledged_frames_and_drops_duplicates() {
    let (mut sender, mut receiver) = (link(7), link(7));
    let first = sender.send(payload("a")).unwrap();
    let _second = sender.send(payload("b")).unwrap();
    let _third = sender.send(payload("c")).unwrap();
    assert!(consume(&mut receiver, first).is_none());

    let Frame::Resume { last_seen, epoch } = receiver.resume_request() else {
        panic!()
    };
    let (answer, replay) = sender.accept_resume(last_seen, epoch);
    assert!(matches!(
        answer,
        Frame::Resumed {
            last_seen: 0,
            outcome: ResumeOutcome::Accepted,
            ..
        }
    ));
    assert_eq!(replay.len(), 2);

    let mut replay = replay.into_iter();
    let second = replay.next().unwrap();
    let _ = consume(&mut receiver, second.clone());
    assert!(matches!(
        receiver.receive(second).unwrap(),
        Received::Duplicate
    ));
}

#[test]
fn resume_across_epochs_is_refused() {
    let mut sender = link(2);
    sender.send(payload("a")).unwrap();
    let (answer, replay) = sender.accept_resume(0, 1);
    assert!(matches!(
        answer,
        Frame::Resumed {
            outcome: ResumeOutcome::Refused { .. },
            ..
        }
    ));
    assert!(replay.is_empty());
}

#[test]
fn resume_behind_a_released_frame_is_refused() {
    let (mut sender, mut receiver) = (link(1), link(1));
    let first = sender.send(payload("a")).unwrap();
    let second = sender.send(payload("b")).unwrap();
    let _ = consume(&mut receiver, first);
    let ack = consume(&mut receiver, second).unwrap();
    sender.receive(ack).unwrap();

    let (answer, _) = sender.accept_resume(0, 1);
    assert!(matches!(
        answer,
        Frame::Resumed {
            outcome: ResumeOutcome::Refused { .. },
            ..
        }
    ));
}

#[test]
fn an_ack_beyond_what_was_sent_fails_the_link() {
    let mut sender = link(1);
    let ack = Frame::Ack {
        ack: 3.try_into().unwrap(),
    };
    assert_eq!(
        sender.receive(ack).unwrap_err(),
        LinkError::AckBeyondSent { ack: 3, sent: 0 }
    );
}

#[test]
fn resume_replays_what_the_dialler_sent_and_the_acceptor_never_consumed() {
    let (mut node, mut gateway) = (link(5), link(5));
    let first = node.send(payload("a")).unwrap();
    let _lost = node.send(payload("b")).unwrap();
    assert!(consume(&mut gateway, first).is_none());

    let Frame::Resume { last_seen, epoch } = node.resume_request() else {
        panic!()
    };
    let (answer, _) = gateway.accept_resume(last_seen, epoch);
    let Frame::Resumed {
        last_seen: gateway_saw,
        outcome: ResumeOutcome::Accepted,
        ..
    } = answer
    else {
        panic!("expected an accepted resume, got {answer:?}")
    };
    assert_eq!(gateway_saw, 1);

    let replay = node.resumed(gateway_saw).unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(node.pending().0, 1, "frame 1 was consumed and is released");
    let ack = consume(&mut gateway, replay.into_iter().next().unwrap());
    assert!(matches!(ack, Some(Frame::Ack { ack }) if ack.get() == 2));
}

#[test]
fn a_dialler_with_nothing_to_resume_is_always_given_a_fresh_link() {
    let mut gateway = Link::new(LinkConfig::default());
    let (answer, replay) = gateway.accept_resume(0, NO_EPOCH);
    assert!(matches!(
        answer,
        Frame::Resumed {
            outcome: ResumeOutcome::Refused { .. },
            ..
        }
    ));
    assert!(replay.is_empty());
}

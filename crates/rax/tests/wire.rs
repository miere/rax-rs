use rax::activity::Activity;
use rax::attachment::{ChunkPart, TransferChunk};
use rax::credential::CredentialRenewal;
use rax::error::{ProviderFailure, RpcFailure};
use rax::event::StopReason;
use rax::frame::{Frame, FrameError, ResumeOutcome};
use rax::interaction::{DisplayAnswer, DisplayOutcome, SignInState};
use rax::local_file::{
    LOCAL_FILE_FORMAT, LocalFileTransfer, ReadLocalFile, replace_local_files, takes_local_files,
};
use rax::message::{from_payload, to_payload};
use rax::open::{Subject, Tagged, UnhandledReason};
use rax::resource::{ReadResource, ResourceTransfer};
use rax::session::{
    GatewayCapabilities, Initialize, Initialized, NewSession, NodeCapabilities, Prompt,
    PromptAccepted, SessionCreated, SessionDurability, ToolGate,
};
use rax::tool::{DeniedBy, ToolCallStatus, ToolCallUpdate, ToolKind};
use rax::*;
use serde_json::{Value, json};

fn assert_wire<T>(message: &T, expected: Value)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded = serde_json::to_value(message).unwrap();
    assert_eq!(encoded, expected);
    let decoded: T = serde_json::from_value(expected).unwrap();
    assert_eq!(&decoded, message);
}

fn request(id: &str, call: GatewayCall) -> GatewayMessage {
    GatewayMessage::Request {
        id: id.into(),
        call: call.into(),
    }
}

#[test]
fn initialize_negotiates_version_and_capabilities() {
    assert_wire(
        &request(
            "1",
            GatewayCall::Initialize(Initialize {
                protocol_version: 1,
                capabilities: GatewayCapabilities {
                    question: true,
                    plan: false,
                    sign_in: true,
                    resource_schemes: vec!["chat".into()],
                    readable_schemes: vec!["gateway".into()],
                    tools: None,
                },
            }),
        ),
        json!({"k": "req", "id": "1", "method": "initialize", "body": {"protocol_version": 1,
               "capabilities": {"question": true, "plan": false, "sign_in": true, "resource_schemes": ["chat"],
               "readable_schemes": ["gateway"]}}}),
    );
    assert_wire(
        &NodeMessage::Reply {
            id: "1".into(),
            reply: GatewayReply::Initialize(Initialized {
                protocol_version: 1,
                capabilities: NodeCapabilities {
                    interruptible: Some(true),
                    ..Default::default()
                },
                metadata: Default::default(),
            }),
        },
        json!({"k": "res", "id": "1", "method": "initialize", "body": {"protocol_version": 1,
               "capabilities": {"interruptible": true, "prompt": {"image": false, "audio": false, "embedded_resource": false},
               "tool_gate": "permission_prompts", "sessions": "ephemeral"}}}),
    );
}

#[test]
fn a_session_is_never_assumed_to_survive_a_restart() {
    let absent: NodeCapabilities = serde_json::from_value(json!({})).unwrap();
    assert_eq!(absent.sessions, SessionDurability::Ephemeral);
    let unknown: NodeCapabilities =
        serde_json::from_value(json!({"sessions": "durable_until_friday"})).unwrap();
    assert_eq!(unknown.sessions, SessionDurability::Ephemeral);
    let durable: NodeCapabilities = serde_json::from_value(json!({"sessions": "durable"})).unwrap();
    assert_eq!(durable.sessions, SessionDurability::Durable);
}

#[test]
fn a_prompt_for_a_session_the_node_lost_is_faulted_as_unknown() {
    assert_wire(
        &NodeMessage::Fault {
            id: "7".into(),
            error: Error::new(
                ErrorKind::UnknownSession,
                "session s1 did not survive a restart",
            ),
        },
        json!({"k": "fault", "id": "7", "error": {"message": "session s1 did not survive a restart",
               "kind": "unknown_session"}}),
    );
}

#[test]
fn a_prompt_for_a_session_with_a_turn_open_is_faulted_as_busy() {
    assert_wire(
        &NodeMessage::Fault {
            id: "8".into(),
            error: Error::new(ErrorKind::SessionBusy, "session s1 already has a turn open"),
        },
        json!({"k": "fault", "id": "8", "error": {"message": "session s1 already has a turn open",
               "kind": "session_busy"}}),
    );
}

#[test]
fn a_tool_gate_is_never_assumed_stronger_than_promised() {
    let absent: NodeCapabilities = serde_json::from_value(json!({})).unwrap();
    assert_eq!(absent.tool_gate, ToolGate::PermissionPrompts);
    let unknown: NodeCapabilities =
        serde_json::from_value(json!({"tool_gate": "every_call_on_tuesdays"})).unwrap();
    assert_eq!(unknown.tool_gate, ToolGate::PermissionPrompts);
    let every: NodeCapabilities =
        serde_json::from_value(json!({"tool_gate": "every_call"})).unwrap();
    assert_eq!(every.tool_gate, ToolGate::EveryCall);
}

#[test]
fn ui_context_travels_as_resource_links_the_protocol_never_interprets() {
    assert_wire(
        &request(
            "2",
            GatewayCall::NewSession(NewSession {
                context: vec![
                    ContentBlock::link("chat://workspace/channel/thread-42", "thread").into(),
                ],
                ..Default::default()
            }),
        ),
        json!({"k": "req", "id": "2", "method": "session.new", "body": {"context": [
               {"type": "resource_link", "uri": "chat://workspace/channel/thread-42", "name": "thread"}]}}),
    );
    assert_wire(
        &request(
            "3",
            GatewayCall::Prompt(Prompt {
                session_id: "s1".into(),
                content: vec![
                    ContentBlock::text("tidy this up").into(),
                    ContentBlock::link("docs://workspace/doc-17", "document").into(),
                ],
            }),
        ),
        json!({"k": "req", "id": "3", "method": "prompt", "body": {"session_id": "s1", "content": [
               {"type": "text", "text": "tidy this up"},
               {"type": "resource_link", "uri": "docs://workspace/doc-17", "name": "document"}]}}),
    );
}

#[test]
fn a_node_reports_what_it_cannot_follow_without_rejecting_the_prompt() {
    assert_wire(
        &NodeMessage::Reply {
            id: "3".into(),
            reply: GatewayReply::Prompt(PromptAccepted {
                unhandled: vec![Unhandled {
                    subject: Subject::Block { index: 1 },
                    reason: UnhandledReason::UnsupportedScheme,
                    message: Some("this node has no docs:// resolver".into()),
                }],
            }),
        },
        json!({"k": "res", "id": "3", "method": "prompt", "body": {"unhandled": [{"subject": "block", "index": 1,
               "reason": "unsupported_scheme", "message": "this node has no docs:// resolver"}]}}),
    );
}

#[test]
fn a_gateway_reports_what_it_cannot_render() {
    assert_wire(
        &GatewayMessage::Unhandled {
            stream: Some("3".into()),
            body: Unhandled {
                subject: Subject::Uri {
                    uri: "file:///home/agent/report.pdf".into(),
                },
                reason: UnhandledReason::Unreachable,
                message: None,
            },
        },
        json!({"k": "unhandled", "stream": "3", "body": {"subject": "uri",
               "uri": "file:///home/agent/report.pdf", "reason": "unreachable"}}),
    );
}

#[test]
fn unknown_tags_decode_as_unknown_and_survive_a_relay() {
    let block = json!({"type": "hologram", "fps": 60});
    let decoded: Open<ContentBlock> = serde_json::from_value(block.clone()).unwrap();
    assert!(matches!(&decoded, Open::Unknown { name, .. } if name == "hologram"));
    assert_eq!(serde_json::to_value(&decoded).unwrap(), block);

    let event = json!({"k": "evt", "stream": "3", "body": {"type": "confetti"}});
    let NodeMessage::Event { body, .. } = serde_json::from_value(event).unwrap() else {
        panic!("not an event")
    };
    assert!(matches!(body, Open::Unknown { name, .. } if name == "confetti"));

    let call = json!({"k": "req", "id": "9", "method": "session.fork", "body": {}});
    let GatewayMessage::Request { id, call } = serde_json::from_value(call).unwrap() else {
        panic!("not a request")
    };
    assert_eq!(id, "9".into());
    assert!(matches!(call, Open::Unknown { name, .. } if name == "session.fork"));

    let kind: Open<GatewayMessage> = serde_json::from_value(json!({"k": "gossip"})).unwrap();
    assert!(matches!(kind, Open::Unknown { name, .. } if name == "gossip"));
}

#[test]
fn a_known_tag_with_a_broken_body_is_an_error_not_an_unknown() {
    let broken = json!({"type": "resource_link", "name": "no uri"});
    assert!(serde_json::from_value::<Open<ContentBlock>>(broken).is_err());
}

#[test]
fn tool_calls_are_announced_then_ruled_on() {
    let announced: NodeMessage =
        serde_json::from_value(json!({"k": "evt", "stream": "3", "body": {
        "type": "tool_call", "tool_call": {"id": "tc1", "name": "Bash", "title": "rm -rf target",
        "kind": "execute", "input": {"command": "rm -rf target"}}}}))
        .unwrap();
    let NodeMessage::Event {
        body: Open::Known(Event::ToolCall { tool_call }),
        ..
    } = announced
    else {
        panic!("not a tool call")
    };
    assert_eq!(tool_call.kind, ToolKind::Execute);

    assert_wire(
        &GatewayMessage::Verdict {
            body: ToolVerdict {
                id: "tc1".into(),
                decision: Decision::Deny {
                    by: DeniedBy::Policy,
                    reason: Some("The admin does not allow shell commands here.".into()),
                },
            },
        },
        json!({"k": "verdict", "body": {"id": "tc1", "decision": "deny", "by": "policy",
               "reason": "The admin does not allow shell commands here."}}),
    );
    assert_wire(
        &GatewayMessage::Verdict {
            body: ToolVerdict {
                id: "tc2".into(),
                decision: Decision::Allow,
            },
        },
        json!({"k": "verdict", "body": {"id": "tc2", "decision": "allow"}}),
    );
    assert_wire(
        &NodeMessage::Event {
            stream: "3".into(),
            body: Event::ToolCallUpdate {
                tool_call_update: ToolCallUpdate {
                    id: "tc1".into(),
                    status: ToolCallStatus::Denied,
                    title: None,
                    content: vec![],
                    output: None,
                },
            }
            .into(),
        },
        json!({"k": "evt", "stream": "3", "body": {"type": "tool_call_update",
               "tool_call_update": {"id": "tc1", "status": "denied"}}}),
    );
}

#[test]
fn background_events_cannot_carry_questions() {
    let raw = json!({"k": "bg", "session_id": "s1",
                     "body": {"type": "question", "question": {"id": "q", "questions": []}}});
    let NodeMessage::Background { body, .. } = serde_json::from_value(raw).unwrap() else {
        panic!("not a background event")
    };
    assert!(matches!(body, Open::Unknown { name, .. } if name == "question"));
}

#[test]
fn replies_name_their_method_and_faults_keep_their_discriminant() {
    assert_wire(
        &NodeMessage::Reply {
            id: "4".into(),
            reply: GatewayReply::RenewCredential(CredentialRenewal::AlreadyRunning),
        },
        json!({"k": "res", "id": "4", "method": "credential.renew", "body": {"status": "already_running"}}),
    );
    assert_wire(
        &GatewayMessage::Reply {
            id: "n1".into(),
            reply: NodeReply::CredentialHealth,
        },
        json!({"k": "res", "id": "n1", "method": "credential.health"}),
    );
    assert_wire(
        &NodeMessage::Fault {
            id: "9".into(),
            error: Error::new(ErrorKind::Unsupported, "no session.fork here"),
        },
        json!({"k": "fault", "id": "9", "error": {"message": "no session.fork here", "kind": "unsupported"}}),
    );
    assert_wire(
        &Error::new(
            ErrorKind::Provider {
                provider: ProviderFailure {
                    kind: "rate_limited".into(),
                    provider: Some("anthropic".into()),
                    status_code: Some(429),
                    message: None,
                    retryable: true,
                },
            },
            "slow down",
        ),
        json!({"message": "slow down", "kind": "provider", "provider": {"kind": "rate_limited",
               "provider": "anthropic", "status_code": 429, "retryable": true}}),
    );
    assert_wire(
        &Error::new(
            ErrorKind::Rpc {
                rpc: RpcFailure {
                    method: Some("session/prompt".into()),
                    code: -32603,
                },
            },
            "boom",
        ),
        json!({"message": "boom", "kind": "rpc", "rpc": {"method": "session/prompt", "code": -32603}}),
    );
    let newer: Error =
        serde_json::from_value(json!({"message": "quota", "kind": "brand_new"})).unwrap();
    assert_eq!(newer, Error::new(ErrorKind::Unknown, "quota"));
}

#[test]
fn turn_events_stream_under_the_prompt_id() {
    assert_wire(
        &NodeMessage::Event {
            stream: "3".into(),
            body: Event::Message {
                content: ContentBlock::text("hello").into(),
            }
            .into(),
        },
        json!({"k": "evt", "stream": "3", "body": {"type": "message", "content": {"type": "text", "text": "hello"}}}),
    );
    assert_wire(
        &NodeMessage::Event {
            stream: "3".into(),
            body: Event::Complete {
                stop_reason: Some(StopReason::EndTurn),
            }
            .into(),
        },
        json!({"k": "evt", "stream": "3", "body": {"type": "complete", "stop_reason": "end_turn"}}),
    );
    assert_wire(
        &NodeMessage::End { stream: "3".into() },
        json!({"k": "end", "stream": "3"}),
    );
}

#[test]
fn answers_travel_keyed_by_the_prompt() {
    assert_wire(
        &GatewayMessage::Answer {
            body: DisplayAnswer {
                id: "q1".into(),
                outcome: DisplayOutcome::Answered,
                answers: [("colour".to_owned(), vec!["blue".to_owned()])].into(),
                choice: None,
                user_id: Some("U1".into()),
                note: None,
                code: None,
            },
        },
        json!({"k": "answer", "body": {"id": "q1", "outcome": "answered",
               "answers": {"colour": ["blue"]}, "user_id": "U1"}}),
    );
}

#[test]
fn a_local_file_read_is_answered_before_its_bytes() {
    assert_wire(
        &request(
            "g8",
            GatewayCall::ReadLocalFile(ReadLocalFile {
                id: "lf_9c2e".into(),
                max_bytes: Some(1024),
            }),
        ),
        json!({"k": "req", "id": "g8", "method": "local_file.read",
               "body": {"id": "lf_9c2e", "max_bytes": 1024}}),
    );
    assert_wire(
        &NodeMessage::Reply {
            id: "g8".into(),
            reply: GatewayReply::ReadLocalFile(LocalFileTransfer {
                transfer_id: "y".into(),
                size: 2,
                name: Some("report.pdf".into()),
                mimetype: None,
            }),
        },
        json!({"k": "res", "id": "g8", "method": "local_file.read",
               "body": {"transfer_id": "y", "size": 2, "name": "report.pdf"}}),
    );
}

#[test]
fn local_files_are_off_unless_declared() {
    let node: NodeCapabilities = serde_json::from_value(json!({})).unwrap();
    assert!(!node.local_files);
    let declared: NodeCapabilities = serde_json::from_value(json!({"local_files": true})).unwrap();
    assert!(declared.local_files);
}

fn attach_schema() -> Value {
    json!({"type": "object", "properties": {
        "file": {"type": "string", "format": LOCAL_FILE_FORMAT},
        "comment": {"type": "string"},
        "more": {"type": "array", "items": {"type": "object", "properties": {
            "path": {"type": "string", "format": "local-file"}}}}}})
}

#[test]
fn every_marked_argument_is_replaced_and_nothing_else() {
    assert!(takes_local_files(&attach_schema()));
    assert!(!takes_local_files(
        &json!({"type": "object", "properties": {"path": {"type": "string"}}})
    ));

    let mut arguments = json!({"file": "/work/a.pdf", "comment": "/work/a.pdf", "unlisted": "/x",
                               "more": [{"path": "b.png"}, {"path": 7}]});
    let mut seen = Vec::new();
    let replaced = replace_local_files(&attach_schema(), &mut arguments, &mut |path| {
        seen.push(path.to_owned());
        Ok::<_, String>(format!("lf_{}", seen.len()).into())
    });
    assert_eq!(replaced, Ok(()));
    assert_eq!(seen, ["/work/a.pdf", "b.png"]);
    assert_eq!(
        arguments,
        json!({"file": "lf_1", "comment": "/work/a.pdf", "unlisted": "/x",
               "more": [{"path": "lf_2"}, {"path": 7}]})
    );
}

#[test]
fn a_refused_path_stops_the_call_from_being_rewritten_further() {
    let mut arguments = json!({"file": "/etc/passwd", "more": [{"path": "b.png"}]});
    let mut asked = 0;
    let refused = replace_local_files(&attach_schema(), &mut arguments, &mut |path| {
        asked += 1;
        Err(format!("{path} is outside the working directory"))
    });
    assert_eq!(
        refused,
        Err("/etc/passwd is outside the working directory".to_owned())
    );
    assert_eq!(asked, 1);
}

#[test]
fn a_receipt_from_an_older_gateway_decodes_as_unknown() {
    let receipt: Open<GatewayMessage> = serde_json::from_value(
        json!({"k": "receipt", "body": {"transfer_id": "t1", "outcome": "delivered"}}),
    )
    .unwrap();
    assert!(matches!(receipt, Open::Unknown { name, .. } if name == "receipt"));
}

#[test]
fn transfer_chunks_encode_bytes_as_base64() {
    assert_wire(
        &NodeMessage::Chunk {
            body: TransferChunk {
                transfer_id: "x".into(),
                seq: 0,
                part: ChunkPart::Data(b"hi".to_vec()),
            },
        },
        json!({"k": "chunk", "body": {"transfer_id": "x", "seq": 0, "part": {"data": "aGk="}}}),
    );
    assert_wire(
        &NodeMessage::Chunk {
            body: TransferChunk {
                transfer_id: "x".into(),
                seq: 1,
                part: ChunkPart::End,
            },
        },
        json!({"k": "chunk", "body": {"transfer_id": "x", "seq": 1, "part": "end"}}),
    );
}

#[test]
fn a_resource_read_is_answered_before_its_bytes() {
    assert_wire(
        &NodeMessage::Request {
            id: "n3".into(),
            call: NodeCall::ReadResource(ReadResource {
                uri: "gateway://files/F1".into(),
                max_bytes: Some(1024),
            })
            .into(),
        },
        json!({"k": "req", "id": "n3", "method": "resource.read",
               "body": {"uri": "gateway://files/F1", "max_bytes": 1024}}),
    );
    assert_wire(
        &GatewayMessage::Reply {
            id: "n3".into(),
            reply: NodeReply::ReadResource(ResourceTransfer {
                transfer_id: "x".into(),
                size: 2,
                mimetype: Some("text/plain".into()),
            }),
        },
        json!({"k": "res", "id": "n3", "method": "resource.read",
               "body": {"transfer_id": "x", "size": 2, "mimetype": "text/plain"}}),
    );
    assert_wire(
        &GatewayMessage::Chunk {
            body: TransferChunk {
                transfer_id: "x".into(),
                seq: 0,
                part: ChunkPart::Data(b"hi".to_vec()),
            },
        },
        json!({"k": "chunk", "body": {"transfer_id": "x", "seq": 0, "part": {"data": "aGk="}}}),
    );
    for (kind, name) in [
        (ErrorKind::NotFound, "not_found"),
        (ErrorKind::Forbidden, "forbidden"),
        (ErrorKind::TooLarge, "too_large"),
    ] {
        assert_wire(
            &GatewayMessage::Fault {
                id: "n3".into(),
                error: Error::new(kind, "no"),
            },
            json!({"k": "fault", "id": "n3", "error": {"message": "no", "kind": name}}),
        );
    }
}

#[test]
fn a_node_cannot_send_a_gateway_call() {
    let raw = json!({"k": "req", "id": "1", "method": "prompt", "body": {"session_id": "s", "content": []}});
    let NodeMessage::Request { call, .. } = serde_json::from_value(raw).unwrap() else {
        panic!("not a request")
    };
    assert!(call.known().is_none());
}

#[test]
fn tag_name_lists_match_the_enums() {
    fn names<T: serde::Serialize>(values: &[T], tag: &str) -> Vec<String> {
        values
            .iter()
            .map(|value| {
                serde_json::to_value(value).unwrap()[tag]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }
    let link = ContentBlock::link("u", "n");
    assert_eq!(
        names(
            &[
                ContentBlock::text(""),
                ContentBlock::Image {
                    data: String::new(),
                    mime_type: String::new(),
                    uri: None
                },
                ContentBlock::Audio {
                    data: String::new(),
                    mime_type: String::new()
                },
                link,
                ContentBlock::Resource {
                    resource: rax::content::EmbeddedResource::Text {
                        uri: String::new(),
                        text: String::new(),
                        mime_type: None
                    }
                },
            ],
            "type"
        ),
        ContentBlock::NAMES
    );
    assert_eq!(
        names(
            &[
                GatewayCall::Initialize(Initialize {
                    protocol_version: 1,
                    capabilities: Default::default()
                }),
                GatewayCall::NewSession(Default::default()),
                GatewayCall::Prompt(Prompt {
                    session_id: "s".into(),
                    content: vec![]
                }),
                GatewayCall::Cancel(rax::session::SessionRef {
                    session_id: "s".into()
                }),
                GatewayCall::CloseSession(rax::session::SessionRef {
                    session_id: "s".into()
                }),
                GatewayCall::RenewCredential,
                GatewayCall::ReadLocalFile(ReadLocalFile {
                    id: "lf".into(),
                    max_bytes: None
                }),
                GatewayCall::Close,
            ],
            "method"
        ),
        GatewayCall::NAMES
    );
    assert_eq!(NodeCall::NAMES.last(), Some(&"activity"));
    assert!(GatewayMessage::NAMES.contains(&"rejected"));
    let event_names = Event::NAMES;
    for name in BackgroundEvent::NAMES {
        assert!(
            event_names.contains(name),
            "background event {name} is not a turn event"
        );
    }
}

#[test]
fn sign_in_terminal_states() {
    let terminal: Vec<_> = [
        SignInState::Working,
        SignInState::Ready,
        SignInState::Confirming,
        SignInState::Success,
        SignInState::Failed,
        SignInState::TimedOut,
        SignInState::Cancelled,
    ]
    .into_iter()
    .filter(|state| state.is_terminal())
    .collect();
    assert_eq!(
        terminal,
        [
            SignInState::Success,
            SignInState::Failed,
            SignInState::TimedOut,
            SignInState::Cancelled
        ]
    );
}

#[test]
fn frames_round_trip_their_payload_untouched() {
    let message = request("1", GatewayCall::Close);
    let frame = Frame::Message {
        seq: 1.try_into().unwrap(),
        ack: 0,
        payload: to_payload(&message).unwrap(),
    };
    let raw = frame.encode().unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&raw).unwrap(),
        json!({"v": 1, "k": "msg", "seq": 1, "p": {"k": "req", "id": "1", "method": "close"}})
    );
    let Frame::Message { payload, .. } = Frame::decode(&raw).unwrap() else {
        panic!("not a message")
    };
    assert_eq!(
        from_payload::<Open<GatewayMessage>>(&payload).unwrap(),
        Open::Known(message)
    );
}

#[test]
fn frames_refuse_what_the_delivery_layer_forbids() {
    let refused = |raw: Value| Frame::decode(&serde_json::to_vec(&raw).unwrap()).unwrap_err();
    assert!(matches!(
        refused(json!({"v": 2, "k": "ack", "ack": 1})),
        FrameError::VersionMismatch { peer: 2 }
    ));
    assert!(matches!(
        refused(json!({"v": 1, "k": "msg", "p": {}})),
        FrameError::Invalid { .. }
    ));
    assert!(matches!(
        refused(json!({"v": 1, "k": "ack", "seq": 4, "ack": 1})),
        FrameError::Invalid { .. }
    ));
    assert!(matches!(
        refused(json!({"v": 1, "k": "resumed", "r": {"last_seen": 1, "epoch": 1}})),
        FrameError::Invalid { .. }
    ));
    assert!(matches!(
        refused(json!({"v": 1, "k": "gossip"})),
        FrameError::Malformed(_)
    ));

    let resumed =
        Frame::decode(br#"{"v":1,"k":"resumed","r":{"last_seen":3,"epoch":2,"reason":"gone"}}"#)
            .unwrap();
    assert!(matches!(
        resumed,
        Frame::Resumed {
            outcome: ResumeOutcome::Refused { .. },
            ..
        }
    ));
}

#[test]
fn a_gateway_publishes_its_tools_at_initialize() {
    assert_wire(
        &request(
            "1",
            GatewayCall::Initialize(Initialize {
                protocol_version: 1,
                capabilities: GatewayCapabilities {
                    tools: Some(ToolGroup {
                        namespace: "murtaugh".into(),
                        tools: vec![ToolDef {
                            name: "slack_read_message".into(),
                            description: "Read a message from a Slack link.".into(),
                            input_schema: Some(json!({"type": "object"})),
                            kind: ToolKind::Read,
                        }],
                    }),
                    ..Default::default()
                },
            }),
        ),
        json!({"k": "req", "id": "1", "method": "initialize", "body": {"protocol_version": 1,
               "capabilities": {"question": false, "plan": false, "sign_in": false,
               "tools": {"namespace": "murtaugh", "tools": [{"name": "slack_read_message",
               "description": "Read a message from a Slack link.",
               "input_schema": {"type": "object"}, "kind": "read"}]}}}}),
    );
}

/// A gateway that predates the catalogue sends no `tools` key, and must not decode as one offering
/// an unnamed namespace.
#[test]
fn a_gateway_with_no_catalogue_offers_no_tools() {
    let raw = json!({"question": true, "plan": false, "sign_in": false});
    let capabilities: GatewayCapabilities = serde_json::from_value(raw).unwrap();
    assert_eq!(capabilities.tools, None);
}

/// The distinction the whole design rests on: a tool that ran and said no is an answer, so the
/// agent reads it; a call that never ran is a fault, so the node can say so in its own words.
#[test]
fn a_tool_that_ran_and_failed_is_still_a_reply() {
    assert_wire(
        &NodeMessage::Request {
            id: "n4".into(),
            call: NodeCall::CallTool(CallTool {
                session_id: "s1".into(),
                namespace: None,
                name: "slack_read_message".into(),
                arguments: Some(json!({"link": "https://slack.com/archives/C1/p1"})),
            })
            .into(),
        },
        json!({"k": "req", "id": "n4", "method": "tool.call",
               "body": {"session_id": "s1", "name": "slack_read_message",
               "arguments": {"link": "https://slack.com/archives/C1/p1"}}}),
    );
    assert_wire(
        &GatewayMessage::Reply {
            id: "n4".into(),
            reply: NodeReply::CallTool(ToolOutcome {
                content: "I am not in that channel, so I cannot read it.".into(),
                is_error: true,
                resources: Vec::new(),
            }),
        },
        json!({"k": "res", "id": "n4", "method": "tool.call",
               "body": {"content": "I am not in that channel, so I cannot read it.",
               "is_error": true}}),
    );
}

/// A tool that fetched something too big to answer with links it, for the node to read after.
#[test]
fn a_tool_can_answer_with_resources() {
    assert_wire(
        &GatewayMessage::Reply {
            id: "n5".into(),
            reply: NodeReply::CallTool(ToolOutcome {
                content: "Read Q3 report.pdf.".into(),
                is_error: false,
                resources: vec![ContentBlock::link("gateway://files/F1", "Q3 report.pdf").into()],
            }),
        },
        json!({"k": "res", "id": "n5", "method": "tool.call",
               "body": {"content": "Read Q3 report.pdf.", "is_error": false,
               "resources": [{"type": "resource_link", "uri": "gateway://files/F1",
                              "name": "Q3 report.pdf"}]}}),
    );
}

#[test]
fn tool_resources_are_off_unless_declared() {
    let node: NodeCapabilities = serde_json::from_value(json!({})).unwrap();
    assert!(!node.tool_resources);
    let declared: NodeCapabilities =
        serde_json::from_value(json!({"tool_resources": true})).unwrap();
    assert!(declared.tool_resources);
}

/// `forbidden` is the same answer a resource gets, and for the same reason: a node must not be able
/// to discover a tool by guessing at names the gateway never published.
#[test]
fn a_tool_the_gateway_never_published_is_forbidden() {
    assert_wire(
        &GatewayMessage::Fault {
            id: "n4".into(),
            error: Error::new(ErrorKind::Forbidden, "no such tool"),
        },
        json!({"k": "fault", "id": "n4", "error": {"message": "no such tool", "kind": "forbidden"}}),
    );
}

#[test]
fn a_tool_taking_no_arguments_omits_both_optional_fields() {
    assert_wire(
        &ToolDef {
            name: "speak".into(),
            description: "Say something out loud.".into(),
            input_schema: None,
            kind: ToolKind::Other,
        },
        json!({"name": "speak", "description": "Say something out loud.", "kind": "other"}),
    );
}

#[test]
fn a_node_declares_metadata_at_initialize_for_the_gateways_that_own_it() {
    let metadata = Metadata::from([(
        "murtaugh_access".to_owned(),
        json!({"policy": "allow_list", "people": ["U0ABC"]}),
    )]);
    assert_wire(
        &NodeMessage::Reply {
            id: "1".into(),
            reply: GatewayReply::Initialize(Initialized {
                protocol_version: 1,
                capabilities: NodeCapabilities::default(),
                metadata,
            }),
        },
        json!({"k": "res", "id": "1", "method": "initialize", "body": {"protocol_version": 1,
               "capabilities": {"prompt": {"image": false, "audio": false, "embedded_resource": false},
               "tool_gate": "permission_prompts", "sessions": "ephemeral"},
               "metadata": {"murtaugh_access": {"policy": "allow_list", "people": ["U0ABC"]}}}}),
    );
    let older: Initialized =
        serde_json::from_value(json!({"protocol_version": 1, "capabilities": {}})).unwrap();
    assert!(older.metadata.is_empty());
}

#[test]
fn a_metadata_update_replaces_the_whole_map() {
    assert_wire(
        &NodeMessage::Request {
            id: "n2".into(),
            call: NodeCall::UpdateMetadata(UpdateMetadata {
                metadata: Metadata::from([("murtaugh_access".to_owned(), json!("always_allow"))]),
            })
            .into(),
        },
        json!({"k": "req", "id": "n2", "method": "metadata.update",
               "body": {"metadata": {"murtaugh_access": "always_allow"}}}),
    );
    assert_wire(
        &GatewayMessage::Reply {
            id: "n2".into(),
            reply: NodeReply::UpdateMetadata,
        },
        json!({"k": "res", "id": "n2", "method": "metadata.update"}),
    );
}

#[test]
fn a_gateway_reports_the_metadata_keys_it_ignored() {
    assert_wire(
        &GatewayMessage::Unhandled {
            stream: None,
            body: Unhandled {
                subject: Subject::MetadataKey {
                    key: "murtaugh_colour".into(),
                },
                reason: UnhandledReason::UnsupportedType,
                message: None,
            },
        },
        json!({"k": "unhandled", "body": {"subject": "metadata_key", "key": "murtaugh_colour",
               "reason": "unsupported_type"}}),
    );
}

#[test]
fn a_gateway_rejects_metadata_it_owns_and_cannot_accept() {
    assert_wire(
        &GatewayMessage::Fault {
            id: "n2".into(),
            error: Error::new(ErrorKind::Rejected, "`people` must be a list"),
        },
        json!({"k": "fault", "id": "n2", "error": {"message": "`people` must be a list",
               "kind": "rejected"}}),
    );
    assert_wire(
        &GatewayMessage::Rejected {
            body: Rejection {
                key: Some("murtaugh_access".into()),
                message: "`people` must be a list".into(),
            },
        },
        json!({"k": "rejected", "body": {"key": "murtaugh_access",
               "message": "`people` must be a list"}}),
    );
}

#[test]
fn a_metadata_key_names_its_gateway_before_the_first_underscore() {
    use rax::metadata::{owned_by, owner};
    assert_eq!(owner("murtaugh_access"), Some(("murtaugh", "access")));
    assert_eq!(owner("murtaugh_tool_mode"), Some(("murtaugh", "tool_mode")));
    assert_eq!(owner("access"), None);
    assert_eq!(owner("_access"), None);
    assert_eq!(owner("murtaugh_"), None);
    let metadata = Metadata::from([
        ("murtaugh_access".to_owned(), json!("always_allow")),
        ("other_access".to_owned(), json!(1)),
        ("loose".to_owned(), json!(2)),
    ]);
    let owned: Vec<_> = owned_by(&metadata, "murtaugh").collect();
    assert_eq!(owned, [("access", &json!("always_allow"))]);
}

#[test]
fn a_session_opens_with_the_tool_groups_it_will_see() {
    assert_wire(
        &request(
            "2",
            GatewayCall::NewSession(NewSession {
                context: vec![],
                tool_groups: vec![
                    ToolGroup {
                        namespace: "slack".into(),
                        tools: vec![ToolDef {
                            name: "attach".into(),
                            description: "Attach a file to the thread.".into(),
                            input_schema: Some(json!({"type": "object"})),
                            kind: ToolKind::Edit,
                        }],
                    },
                    ToolGroup {
                        namespace: "openknowledge_x7k2".into(),
                        tools: vec![ToolDef {
                            name: "read".into(),
                            description: "Read the open document.".into(),
                            input_schema: None,
                            kind: ToolKind::Read,
                        }],
                    },
                ],
            }),
        ),
        json!({"k": "req", "id": "2", "method": "session.new", "body": {"tool_groups": [
               {"namespace": "slack", "tools": [{"name": "attach",
                "description": "Attach a file to the thread.", "input_schema": {"type": "object"},
                "kind": "edit"}]},
               {"namespace": "openknowledge_x7k2", "tools": [{"name": "read",
                "description": "Read the open document.", "kind": "read"}]}]}}),
    );
    // A gateway that lends nothing, or predates groups, sends no key at all.
    let bare: NewSession = serde_json::from_value(json!({})).unwrap();
    assert!(bare.tool_groups.is_empty());
}

/// A node that cannot publish a group still opens the session, and says which group it left out.
#[test]
fn a_node_reports_a_group_it_did_not_publish() {
    assert_wire(
        &NodeMessage::Reply {
            id: "2".into(),
            reply: GatewayReply::NewSession(SessionCreated {
                session_id: "s1".into(),
                unhandled: vec![Unhandled {
                    subject: Subject::ToolGroup {
                        namespace: "github".into(),
                    },
                    reason: UnhandledReason::Other,
                    message: Some("this machine already has a tool server named github".into()),
                }],
            }),
        },
        json!({"k": "res", "id": "2", "method": "session.new", "body": {"session_id": "s1",
               "unhandled": [{"subject": "tool_group", "namespace": "github", "reason": "other",
               "message": "this machine already has a tool server named github"}]}}),
    );
}

#[test]
fn a_tool_call_names_the_group_it_belongs_to() {
    assert_wire(
        &NodeMessage::Request {
            id: "n4".into(),
            call: NodeCall::CallTool(CallTool {
                session_id: "s1".into(),
                namespace: Some("slack".into()),
                name: "attach".into(),
                arguments: None,
            })
            .into(),
        },
        json!({"k": "req", "id": "n4", "method": "tool.call",
               "body": {"session_id": "s1", "namespace": "slack", "name": "attach"}}),
    );
}

/// The capability is left off the wire when false, so an older gateway sees exactly what it did.
#[test]
fn a_node_declares_it_takes_tools_per_session() {
    let declared = NodeCapabilities {
        tool_groups: true,
        ..Default::default()
    };
    let encoded = serde_json::to_value(&declared).unwrap();
    assert_eq!(encoded["tool_groups"], json!(true));
    let older: NodeCapabilities = serde_json::from_value(json!({})).unwrap();
    assert!(!older.tool_groups);
    assert!(
        serde_json::to_value(&older)
            .unwrap()
            .get("tool_groups")
            .is_none()
    );
}

#[test]
fn a_namespace_fits_a_tool_name_and_nothing_else() {
    assert!(ToolGroup::is_valid_namespace("slack"));
    assert!(ToolGroup::is_valid_namespace("openknowledge_x7k2"));
    assert!(ToolGroup::is_valid_namespace(&"a".repeat(32)));
    assert!(!ToolGroup::is_valid_namespace(&"a".repeat(33)));
    assert!(!ToolGroup::is_valid_namespace(""));
    assert!(!ToolGroup::is_valid_namespace("Slack"));
    assert!(!ToolGroup::is_valid_namespace("open-knowledge"));
    assert!(!ToolGroup::is_valid_namespace("slack.tools"));
}

#[test]
fn a_node_reports_its_activity() {
    assert_wire(
        &NodeMessage::Request {
            id: "n3".into(),
            call: NodeCall::Activity(Activity {
                turns: 1,
                background_tasks: 2,
            })
            .into(),
        },
        json!({"k": "req", "id": "n3", "method": "activity",
               "body": {"turns": 1, "background_tasks": 2}}),
    );
    assert_wire(
        &GatewayMessage::Reply {
            id: "n3".into(),
            reply: NodeReply::Activity,
        },
        json!({"k": "res", "id": "n3", "method": "activity"}),
    );
}

#[test]
fn a_node_with_nothing_open_is_idle() {
    assert!(Activity::default().is_idle());
    let turn = Activity {
        turns: 1,
        background_tasks: 0,
    };
    assert!(!turn.is_idle());
    let task = Activity {
        turns: 0,
        background_tasks: 1,
    };
    assert!(!task.is_idle());
}

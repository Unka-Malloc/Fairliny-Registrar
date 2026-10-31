use super::*;
use std::sync::atomic::Ordering;

struct FixtureApp;

impl McpApplication for FixtureApp {
    type CallerContext = String;

    fn tool_catalog(&self) -> Vec<Value> {
        vec![serde_json::json!({
            "name":"fixture_tool",
            "inputSchema":{"type":"object","additionalProperties":false}
        })]
    }

    fn validate_tool_arguments(&self, name: &str, arguments: &Map<String, Value>) -> bool {
        name == "fixture_tool" && arguments.is_empty()
    }

    fn call_tool(
        &self,
        context: McpToolCallContext<'_, Self::CallerContext>,
        _: &str,
        _: &Map<String, Value>,
    ) -> std::result::Result<Value, McpApplicationError> {
        assert!(!context.cancelled.load(Ordering::Acquire));
        Ok(serde_json::json!({"caller": context.caller}))
    }
}

fn engine(name: &'static str, revision: &'static str) -> McpServerEngine<FixtureApp> {
    engine_with_compatible_revisions(name, revision, &[])
}

fn engine_with_compatible_revisions(
    name: &'static str,
    revision: &'static str,
    compatible_protocol_revisions: &'static [&'static str],
) -> McpServerEngine<FixtureApp> {
    McpServerEngine::new(
        McpServerDefinition {
            protocol_revision: revision,
            compatible_protocol_revisions,
            server_name: name,
            server_version: "1.0.0",
            max_message_bytes: 4096,
        },
        FixtureApp,
    )
    .unwrap()
}

#[test]
fn initialize_negotiates_an_explicit_compatible_revision() {
    let engine = engine_with_compatible_revisions("fixture", "2025-06-18", &["2025-11-25"]);
    let session = McpSessionState::default();
    let response = engine
        .handle(
            &session,
            &"caller:fixture".to_owned(),
            McpMessage::request(
                1_i64,
                "initialize",
                Some(object(serde_json::json!({
                    "protocolVersion":"2025-11-25",
                    "capabilities":{},
                    "clientInfo":{"name":"fixture","version":"1"}
                }))),
            )
            .unwrap(),
        )
        .unwrap()
        .to_value();

    assert_eq!(response["result"]["protocolVersion"], "2025-11-25");
    assert_eq!(session.protocol_revision(), Some("2025-11-25"));
    assert!(session.initialized());
}

#[test]
fn engine_is_parameterized_without_outbound_revision_aliasing() {
    let subagent = engine("subagent", "2025-06-18");
    let outbound = engine("outbound", "2025-11-25");
    assert_eq!(subagent.definition().protocol_revision, "2025-06-18");
    assert_eq!(outbound.definition().protocol_revision, "2025-11-25");
    assert_ne!(
        subagent.definition().protocol_revision,
        outbound.definition().protocol_revision
    );
}

#[test]
fn initialize_list_and_call_share_the_closed_engine() {
    let engine = engine("fixture", "2025-06-18");
    let session = McpSessionState::default();
    let caller = "caller:fixture".to_owned();
    let initialize = McpMessage::request(
        1_i64,
        "initialize",
        Some(object(serde_json::json!({
            "protocolVersion":"2025-06-18",
            "capabilities":{},
            "clientInfo":{"name":"fixture","version":"1"},
            "_meta":{"client":"fixture"}
        }))),
    )
    .unwrap();
    let initialized = engine
        .handle(&session, &caller, initialize)
        .unwrap()
        .to_value();
    assert_eq!(initialized["result"]["serverInfo"]["name"], "fixture");
    assert!(session.initialized());

    let list = engine
        .handle(
            &session,
            &caller,
            McpMessage::request(
                2_i64,
                "tools/list",
                Some(object(serde_json::json!({"_meta":{"progressToken":2}}))),
            )
            .unwrap(),
        )
        .unwrap()
        .to_value();
    assert_eq!(list["result"]["tools"][0]["name"], "fixture_tool");
    let called = engine
        .handle(
            &session,
            &caller,
            McpMessage::request(
                3_i64,
                "tools/call",
                Some(object(serde_json::json!({
                    "name":"fixture_tool",
                    "arguments":{},
                    "_meta":{"progressToken":3}
                }))),
            )
            .unwrap(),
        )
        .unwrap()
        .to_value();
    assert_eq!(
        called["result"]["structuredContent"]["caller"],
        "caller:fixture"
    );
}

#[test]
fn standard_metadata_does_not_admit_unknown_or_malformed_params() {
    let engine = engine("fixture", "2025-06-18");
    let session = McpSessionState::default();
    let caller = "caller:fixture".to_owned();
    let initialize = McpMessage::request(
        1_i64,
        "initialize",
        Some(object(serde_json::json!({
            "protocolVersion":"2025-06-18",
            "capabilities":{},
            "clientInfo":{"name":"fixture","version":"1"}
        }))),
    )
    .unwrap();
    engine.handle(&session, &caller, initialize).unwrap();

    for params in [
        serde_json::json!({"_meta":"invalid"}),
        serde_json::json!({"unexpected":true}),
        serde_json::json!({"cursor":false}),
    ] {
        let response = engine
            .handle(
                &session,
                &caller,
                McpMessage::request(2_i64, "tools/list", Some(object(params))).unwrap(),
            )
            .unwrap()
            .to_value();
        assert_eq!(response["error"]["code"], -32602);
    }
}

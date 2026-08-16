use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkflowSpec {
    pub name: String,

    #[serde(default)]
    pub timeout: Option<u64>,

    pub tasks: BTreeMap<String, TaskSpec>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskSpec {
    #[serde(flatten)]
    pub config: TaskConfigSpec,

    #[serde(default)]
    pub timeout: Option<u64>,

    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TaskConfigSpec {
    Shell {
        command: String,

        #[serde(default)]
        env: HashMap<String, String>,

        #[serde(default)]
        cwd: Option<String>,

        #[serde(default)]
        shell: Option<String>,

        #[serde(default = "default_shell_success_codes")]
        success_codes: Vec<StatusMatcher>,
    },

    Http {
        method: HttpMethod,
        url: String,

        #[serde(default)]
        query: HashMap<String, String>,

        #[serde(default)]
        headers: HashMap<String, String>,

        #[serde(default)]
        body: Option<String>,

        #[serde(default = "default_http_success_codes")]
        success_codes: Vec<StatusMatcher>,
    },

    Wait {
        #[serde(default)]
        duration: Option<u64>,

        #[serde(default)]
        until: Option<String>,
    },

    Noop {
        #[serde(default)]
        message: Option<String>,
    },
}

fn default_shell_success_codes() -> Vec<StatusMatcher> {
    vec![StatusMatcher::Exact(0)]
}

fn default_http_success_codes() -> Vec<StatusMatcher> {
    vec![StatusMatcher::Range(200, 299)]
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
}

#[derive(Debug, Clone, Copy)]
pub enum StatusMatcher {
    Exact(u16),
    Range(u16, u16),
}

impl StatusMatcher {
    pub fn matches(&self, code: u16) -> bool {
        match self {
            StatusMatcher::Exact(c) => *c == code,
            StatusMatcher::Range(low, high) => (*low..=*high).contains(&code),
        }
    }
}

impl Serialize for StatusMatcher {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            StatusMatcher::Exact(code) => serializer.serialize_u16(*code),
            StatusMatcher::Range(low, high) => serializer.serialize_str(&format!("{low}-{high}")),
        }
    }
}

impl<'de> Deserialize<'de> for StatusMatcher {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Int(u16),
            Str(String),
        }

        match Raw::deserialize(deserializer)? {
            Raw::Int(code) => Ok(StatusMatcher::Exact(code)),
            Raw::Str(s) => parse_status_range(&s).map_err(serde::de::Error::custom),
        }
    }
}

fn parse_status_range(s: &str) -> Result<StatusMatcher, String> {
    if let Some(prefix) = s.strip_suffix("xx") {
        let digit: u16 = prefix
            .parse()
            .map_err(|_| format!("invalid status range shorthand: {s}"))?;

        return Ok(StatusMatcher::Range(digit * 100, digit * 100 + 99));
    }

    if let Some((low, high)) = s.split_once('-') {
        let low: u16 = low
            .trim()
            .parse()
            .map_err(|_| format!("invalid status range: {s}"))?;

        let high: u16 = high
            .trim()
            .parse()
            .map_err(|_| format!("invalid status range: {s}"))?;

        return Ok(StatusMatcher::Range(low, high));
    }

    Err(format!("invalid status matcher: {s}"))
}

// =========================================================================
// Testing
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_matcher_exact_matches_only_that_code() {
        let matcher = StatusMatcher::Exact(200);

        assert!(matcher.matches(200));
        assert!(!matcher.matches(201));
    }

    #[test]
    fn status_matcher_range_matches_inclusive_bounds() {
        let matcher = StatusMatcher::Range(200, 299);

        assert!(matcher.matches(200));
        assert!(matcher.matches(299));
        assert!(matcher.matches(250));
        assert!(!matcher.matches(199));
        assert!(!matcher.matches(300));
    }

    #[test]
    fn status_matcher_exact_serializes_as_integer() {
        let matcher = StatusMatcher::Exact(204);

        assert_eq!(serde_json::to_string(&matcher).unwrap(), "204");
    }

    #[test]
    fn status_matcher_range_serializes_as_dash_string() {
        let matcher = StatusMatcher::Range(200, 299);

        assert_eq!(serde_json::to_string(&matcher).unwrap(), "\"200-299\"");
    }

    #[test]
    fn status_matcher_deserializes_from_integer() {
        let matcher: StatusMatcher = serde_json::from_str("204").unwrap();

        assert!(matches!(matcher, StatusMatcher::Exact(204)));
    }

    #[test]
    fn status_matcher_deserializes_from_dash_range() {
        let matcher: StatusMatcher = serde_json::from_str("\"200-299\"").unwrap();

        assert!(matches!(matcher, StatusMatcher::Range(200, 299)));
    }

    #[test]
    fn status_matcher_deserializes_from_xx_shorthand() {
        let matcher: StatusMatcher = serde_json::from_str("\"2xx\"").unwrap();

        assert!(matches!(matcher, StatusMatcher::Range(200, 299)));
    }

    #[test]
    fn status_matcher_deserialize_rejects_invalid_string() {
        let result: Result<StatusMatcher, _> = serde_json::from_str("\"not-a-code\"");

        assert!(result.is_err());
    }

    #[test]
    fn status_matcher_deserialize_rejects_invalid_xx_shorthand() {
        let result: Result<StatusMatcher, _> = serde_json::from_str("\"xxx xx\"");

        assert!(result.is_err());
    }

    #[test]
    fn parse_status_range_parses_dash_range_with_whitespace() {
        let matcher = parse_status_range(" 200 - 299 ").unwrap();

        assert!(matches!(matcher, StatusMatcher::Range(200, 299)));
    }

    #[test]
    fn parse_status_range_parses_xx_shorthand() {
        let matcher = parse_status_range("4xx").unwrap();

        assert!(matches!(matcher, StatusMatcher::Range(400, 499)));
    }

    #[test]
    fn parse_status_range_rejects_string_without_separator() {
        let result = parse_status_range("ok");

        assert_eq!(result.unwrap_err(), "invalid status matcher: ok");
    }

    #[test]
    fn parse_status_range_rejects_non_numeric_bounds() {
        let result = parse_status_range("abc-def");

        assert_eq!(result.unwrap_err(), "invalid status range: abc-def");
    }

    #[test]
    fn parse_status_range_rejects_non_numeric_xx_prefix() {
        let result = parse_status_range("abcxx");

        assert_eq!(result.unwrap_err(), "invalid status range shorthand: abcxx");
    }

    #[test]
    fn shell_task_config_deserializes_with_defaults() {
        let config: TaskConfigSpec =
            serde_json::from_str(r#"{"type": "shell", "command": "echo hi"}"#).unwrap();

        match config {
            TaskConfigSpec::Shell {
                command,
                env,
                cwd,
                shell,
                success_codes,
            } => {
                assert_eq!(command, "echo hi");
                assert!(env.is_empty());
                assert!(cwd.is_none());
                assert!(shell.is_none());
                assert!(matches!(
                    success_codes.as_slice(),
                    [StatusMatcher::Exact(0)]
                ));
            }
            _ => panic!("expected Shell variant"),
        }
    }

    #[test]
    fn http_task_config_deserializes_with_defaults() {
        let config: TaskConfigSpec = serde_json::from_str(
            r#"{"type": "http", "method": "GET", "url": "https://example.com"}"#,
        )
        .unwrap();

        match config {
            TaskConfigSpec::Http {
                method,
                url,
                query,
                headers,
                body,
                success_codes,
            } => {
                assert!(matches!(method, HttpMethod::Get));
                assert_eq!(url, "https://example.com");
                assert!(query.is_empty());
                assert!(headers.is_empty());
                assert!(body.is_none());
                assert!(matches!(
                    success_codes.as_slice(),
                    [StatusMatcher::Range(200, 299)]
                ));
            }
            _ => panic!("expected Http variant"),
        }
    }

    #[test]
    fn noop_task_config_deserializes_without_message() {
        let config: TaskConfigSpec = serde_json::from_str(r#"{"type": "noop"}"#).unwrap();

        assert!(matches!(config, TaskConfigSpec::Noop { message: None }));
    }

    #[test]
    fn wait_task_config_deserializes_with_duration() {
        let config: TaskConfigSpec =
            serde_json::from_str(r#"{"type": "wait", "duration": 30}"#).unwrap();

        match config {
            TaskConfigSpec::Wait { duration, until } => {
                assert_eq!(duration, Some(30));
                assert!(until.is_none());
            }
            _ => panic!("expected Wait variant"),
        }
    }

    #[test]
    fn task_spec_deserializes_with_default_depends_on() {
        let task: TaskSpec = serde_json::from_str(r#"{"type": "noop"}"#).unwrap();

        assert!(task.depends_on.is_empty());
        assert!(task.timeout.is_none());
    }

    #[test]
    fn workflow_spec_deserializes_tasks_map() {
        let workflow: WorkflowSpec = serde_json::from_str(
            r#"{
                "name": "example",
                "tasks": {
                    "first": {"type": "noop"},
                    "second": {"type": "noop", "depends_on": ["first"]}
                }
            }"#,
        )
        .unwrap();

        assert_eq!(workflow.name, "example");
        assert!(workflow.timeout.is_none());
        assert_eq!(workflow.tasks.len(), 2);
        assert_eq!(workflow.tasks["second"].depends_on, vec!["first"]);
    }
}

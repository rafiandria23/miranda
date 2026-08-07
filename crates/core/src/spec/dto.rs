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

//! Typed runtime configuration for the `lmxd` broker.
//!
//! CLI flags are parsed through the project-local `.cli-flags.toml` with the
//! vendored `flags-2-env` parser. Reconciliation is intentionally explicit:
//! named CLI flags win over legacy positional CLI values, which win over env
//! vars, which finally fall back to typed defaults.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::fmt;
use std::fs;
use std::os::raw::{c_char, c_int};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::codec::WireCodec;
use crate::composite::Composite;
use crate::transport::TransportSettings;
use crate::{NodeId, QuorumPolicy, RENEW_INTERVAL};

pub const USAGE: &str = "usage: lmxd [--codec text|json|msgpack] [--client-http addr] [--client-tcp addr] <my_id> <addr_0> <addr_1> ... <addr_{n-1}>";

pub const ENV_CODEC: &str = "LMX_CODEC";
pub const ENV_NODE_ID: &str = "LMX_NODE_ID";
pub const ENV_PEER_ADDRS: &str = "LMX_PEER_ADDRS";
pub const ENV_CLIENT_HTTP: &str = "LMX_CLIENT_HTTP";
pub const ENV_CLIENT_TCP: &str = "LMX_CLIENT_TCP";
pub const ENV_STDIN: &str = "LMX_STDIN";
pub const ENV_LOG_INFO: &str = "LMX_LOG_INFO";
pub const ENV_CONNECT_RETRY_MS: &str = "LMX_CONNECT_RETRY_MS";
pub const ENV_TICK_MS: &str = "LMX_TICK_MS";
pub const ENV_MAX_FRAME_BYTES: &str = "LMX_MAX_FRAME_BYTES";
pub const ENV_QUORUM_POLICY: &str = "LMX_QUORUM_POLICY";
pub const ENV_DEMO: &str = "LMX_DEMO";
pub const ENV_DEMO_KEYS: &str = "LMX_DEMO_KEYS";
pub const ENV_DEMO_HOLD_MS: &str = "LMX_DEMO_HOLD_MS";
pub const ENV_DEMO_REST_MS: &str = "LMX_DEMO_REST_MS";

const ENV_POSITIONALS: &str = "LMX_POSITIONALS";
const ENV_UNKNOWN_OPTIONS: &str = "LMX_UNKNOWN_OPTIONS";
const ENV_PARSE_ERRORS: &str = "LMX_PARSE_ERRORS";
const ENV_FLAGS2ENV_CONFIG: &str = "FLAGS2ENV_CONFIG";
const ENV_LMX_CLI_FLAGS_CONFIG: &str = "LMX_CLI_FLAGS_CONFIG";
const DEFAULT_DEMO_KEYS: &str = "cap,mid,zed";
const CLI_FLAGS_FILE: &str = ".cli-flags.toml";
const EMBEDDED_CLI_FLAGS_TOML: &str = include_str!("../.cli-flags.toml");
const DEFAULT_CONNECT_RETRY_MS: u64 = 150;
const DEFAULT_TICK_MS: u64 = 500;
const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;
const DEFAULT_STDIN: bool = true;
const DEFAULT_LOG_INFO: bool = true;
const DEFAULT_DEMO_HOLD_MS: u64 = 800;
const DEFAULT_DEMO_REST_MS: u64 = 400;
const MIN_CONNECT_RETRY_MS: u64 = 10;
const MAX_CONNECT_RETRY_MS: u64 = 60_000;
const MIN_TICK_MS: u64 = 10;
const MIN_MAX_FRAME_BYTES: usize = 128;
const MAX_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const MAX_DEMO_DELAY_MS: u64 = 60 * 60 * 1000;

extern "C" {
    fn f2e_parse_json_argv_from_file(
        config_path: *const c_char,
        argv_json: *const c_char,
    ) -> *mut c_char;
    fn f2e_help_table_from_file(
        config_path: *const c_char,
        command_name: *const c_char,
        terminal_columns: c_int,
    ) -> *mut c_char;
    fn f2e_audit_config_status_from_file(config_path: *const c_char) -> c_int;
    fn f2e_audit_config_from_file(config_path: *const c_char) -> *mut c_char;
    fn f2e_free(value: *mut c_char);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerRuntimeConfig {
    pub id: NodeId,
    pub addrs: Vec<String>,
    pub codec: WireCodec,
    pub client_http: Option<String>,
    pub client_tcp: Option<String>,
    pub stdin: bool,
    pub log_info: bool,
    pub transport: TransportSettings,
    pub demo: BrokerDemoConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerDemoConfig {
    pub enabled: bool,
    pub keys: Vec<String>,
    pub hold: Duration,
    pub rest: Duration,
}

impl Default for BrokerDemoConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            keys: parse_csv_list(DEFAULT_DEMO_KEYS),
            hold: Duration::from_millis(DEFAULT_DEMO_HOLD_MS),
            rest: Duration::from_millis(DEFAULT_DEMO_REST_MS),
        }
    }
}

impl BrokerRuntimeConfig {
    pub fn from_process() -> Result<Self, BrokerRuntimeConfigError> {
        let env = std::env::vars().collect::<HashMap<_, _>>();
        let args = std::env::args().collect::<Vec<_>>();
        Self::from_env_and_args(&env, &args)
    }

    pub fn from_env_and_args(
        env: &HashMap<String, String>,
        args: &[String],
    ) -> Result<Self, BrokerRuntimeConfigError> {
        let config_path = resolve_cli_flags_config(env, args)?;
        let command_name = command_name(args);
        let cli_args = strip_config_selector(args.iter().skip(1).cloned())?;

        if cli_args.iter().any(|arg| arg == "--help" || arg == "-h") {
            return Err(BrokerRuntimeConfigError::HelpRequested(render_help(
                &config_path,
                &command_name,
            )?));
        }

        let cli_overrides = parse_cli_overrides(&config_path, &cli_args)?;
        Self::reconcile(env, cli_overrides)
    }

    pub fn reconcile(
        env: &HashMap<String, String>,
        mut cli_overrides: HashMap<String, String>,
    ) -> Result<Self, BrokerRuntimeConfigError> {
        let parse_errors = take_json_list(&mut cli_overrides, ENV_PARSE_ERRORS)?;
        if !parse_errors.is_empty() {
            return Err(BrokerRuntimeConfigError::CliParseErrors(parse_errors));
        }

        let unknown_options = take_json_list(&mut cli_overrides, ENV_UNKNOWN_OPTIONS)?;
        if !unknown_options.is_empty() {
            return Err(BrokerRuntimeConfigError::UnknownCliOptions(unknown_options));
        }

        let positionals = take_json_list(&mut cli_overrides, ENV_POSITIONALS)?;

        let codec = setting(&cli_overrides, env, ENV_CODEC)
            .map(|raw| parse_codec(&raw))
            .transpose()?
            .unwrap_or(WireCodec::Text);

        let id_raw = cli_overrides
            .get(ENV_NODE_ID)
            .map(|value| value.trim().to_string())
            .or_else(|| positionals.first().cloned())
            .or_else(|| non_empty_env(env, ENV_NODE_ID))
            .ok_or(BrokerRuntimeConfigError::MissingNodeId)?;
        let id = parse_node_id(&id_raw)?;

        let addrs = if let Some(raw) = cli_overrides.get(ENV_PEER_ADDRS) {
            parse_peer_addrs(raw.trim())?
        } else if positionals.len() > 1 {
            positionals[1..].to_vec()
        } else if let Some(raw) = non_empty_env(env, ENV_PEER_ADDRS) {
            parse_peer_addrs(&raw)?
        } else {
            return Err(BrokerRuntimeConfigError::MissingPeerAddrs);
        };

        validate_peer_addrs(&addrs)?;
        if id as usize >= addrs.len() {
            return Err(BrokerRuntimeConfigError::NodeIdOutOfRange {
                id,
                peers: addrs.len(),
            });
        }

        let client_http = optional_socket_addr_setting(&cli_overrides, env, ENV_CLIENT_HTTP)?;
        let client_tcp = optional_socket_addr_setting(&cli_overrides, env, ENV_CLIENT_TCP)?;
        let stdin = parse_bool_setting(&cli_overrides, env, ENV_STDIN, DEFAULT_STDIN)?;
        let log_info = parse_bool_setting(&cli_overrides, env, ENV_LOG_INFO, DEFAULT_LOG_INFO)?;
        let transport = parse_transport(&cli_overrides, env, codec)?;
        let demo = parse_demo(&cli_overrides, env)?;

        Ok(Self {
            id,
            addrs,
            codec,
            client_http,
            client_tcp,
            stdin,
            log_info,
            transport,
            demo,
        })
    }
}

#[derive(Debug)]
pub enum BrokerRuntimeConfigError {
    HelpRequested(String),
    CliFlagsConfigNotFound,
    CliFlagsConfigInvalid {
        path: PathBuf,
        report: String,
    },
    CliParserCString(std::ffi::NulError),
    CliParserJson {
        raw: String,
        source: serde_json::Error,
    },
    CliParseErrors(Vec<String>),
    UnknownCliOptions(Vec<String>),
    InvalidJsonList {
        env: &'static str,
        value: String,
    },
    MissingNodeId,
    InvalidNodeId(String),
    MissingPeerAddrs,
    NodeIdOutOfRange {
        id: NodeId,
        peers: usize,
    },
    InvalidCodec(String),
    InvalidQuorumPolicy(String),
    InvalidBool {
        env: &'static str,
        value: String,
    },
    MissingCliValue(&'static str),
    InvalidPeerAddrs(String),
    InvalidSocketAddr {
        env: &'static str,
        value: String,
    },
    InvalidNumber {
        env: &'static str,
        value: String,
    },
    NumberOutOfRange {
        env: &'static str,
        value: u64,
        min: u64,
        max: u64,
    },
    InvalidDemoKeys(String),
    InvalidConfigFlag(String),
    EmbeddedCliFlagsWrite {
        path: PathBuf,
        source: std::io::Error,
    },
    CurrentDir(std::io::Error),
}

impl fmt::Display for BrokerRuntimeConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HelpRequested(_) => write!(f, "help requested"),
            Self::CliFlagsConfigNotFound => write!(f, "could not find {CLI_FLAGS_FILE}"),
            Self::CliFlagsConfigInvalid { path, report } => {
                write!(f, "{} failed flags2env audit: {report}", path.display())
            }
            Self::CliParserCString(err) => write!(f, "invalid string for flags2env: {err}"),
            Self::CliParserJson { raw, source } => {
                write!(f, "flags2env returned invalid JSON ({source}): {raw}")
            }
            Self::CliParseErrors(errors) => {
                write!(f, "invalid CLI flag values: {}", errors.join("; "))
            }
            Self::UnknownCliOptions(options) => {
                write!(f, "unknown CLI option(s): {}", options.join(", "))
            }
            Self::InvalidJsonList { env, value } => {
                write!(f, "{env} must be a JSON string array, got {value:?}")
            }
            Self::MissingNodeId => write!(f, "missing broker node id"),
            Self::InvalidNodeId(raw) => write!(
                f,
                "broker node id must be a non-negative integer, got {raw:?}"
            ),
            Self::MissingPeerAddrs => write!(f, "missing broker peer address list"),
            Self::NodeIdOutOfRange { id, peers } => {
                write!(
                    f,
                    "broker node id {id} is outside the {peers}-peer address list"
                )
            }
            Self::InvalidCodec(raw) => {
                write!(f, "unknown codec {raw:?}; use text, json, or msgpack")
            }
            Self::InvalidQuorumPolicy(raw) => {
                write!(f, "unknown quorum policy {raw:?}; use majority or grid")
            }
            Self::InvalidBool { env, value } => write!(
                f,
                "{env} must be a boolean value (true/false, 1/0, yes/no, on/off), got {value:?}"
            ),
            Self::MissingCliValue(flag) => write!(f, "missing value after {flag}"),
            Self::InvalidPeerAddrs(raw) => {
                write!(
                    f,
                    "{ENV_PEER_ADDRS} must contain at least one address, got {raw:?}"
                )
            }
            Self::InvalidSocketAddr { env, value } => write!(
                f,
                "{env} must look like host:port with a valid TCP port, got {value:?}"
            ),
            Self::InvalidNumber { env, value } => {
                write!(f, "{env} must be a non-negative integer, got {value:?}")
            }
            Self::NumberOutOfRange {
                env,
                value,
                min,
                max,
            } => write!(f, "{env} must be between {min} and {max}, got {value}"),
            Self::InvalidDemoKeys(raw) => {
                write!(f, "{ENV_DEMO_KEYS} must contain at least one key when demo mode is enabled, got {raw:?}")
            }
            Self::InvalidConfigFlag(value) => write!(
                f,
                "invalid broker config flag syntax near {value:?}; use --config <path> or --config=<path>"
            ),
            Self::EmbeddedCliFlagsWrite { path, source } => write!(
                f,
                "could not write embedded broker flags config to {}: {source}",
                path.display()
            ),
            Self::CurrentDir(err) => write!(f, "could not inspect current directory: {err}"),
        }
    }
}

impl std::error::Error for BrokerRuntimeConfigError {}

fn resolve_cli_flags_config(
    env: &HashMap<String, String>,
    args: &[String],
) -> Result<PathBuf, BrokerRuntimeConfigError> {
    if let Some(path) = cli_flags_config_from_args(args)?
        .or_else(|| non_empty_env(env, ENV_LMX_CLI_FLAGS_CONFIG))
        .or_else(|| non_empty_env(env, ENV_FLAGS2ENV_CONFIG))
    {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        return Err(BrokerRuntimeConfigError::CliFlagsConfigNotFound);
    }

    let cwd = std::env::current_dir().map_err(BrokerRuntimeConfigError::CurrentDir)?;
    if let Some(path) = find_upward(&cwd, CLI_FLAGS_FILE) {
        return Ok(path);
    }

    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(CLI_FLAGS_FILE);
    if manifest_path.is_file() {
        return Ok(manifest_path);
    }

    embedded_cli_flags_config_path()
}

fn embedded_cli_flags_config_path() -> Result<PathBuf, BrokerRuntimeConfigError> {
    let path = std::env::temp_dir().join(format!("lmxd-cli-flags-{}.toml", std::process::id()));
    fs::write(&path, EMBEDDED_CLI_FLAGS_TOML).map_err(|source| {
        BrokerRuntimeConfigError::EmbeddedCliFlagsWrite {
            path: path.clone(),
            source,
        }
    })?;
    Ok(path)
}

fn cli_flags_config_from_args(args: &[String]) -> Result<Option<String>, BrokerRuntimeConfigError> {
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        if arg == "--" {
            return Ok(None);
        }
        if let Some(path) = arg
            .strip_prefix("--config=")
            .or_else(|| arg.strip_prefix("--cli-flags="))
            .or_else(|| arg.strip_prefix("--cli-flags-config="))
        {
            return Ok(Some(path.to_string()));
        }
        if arg == "--config" || arg == "--cli-flags" || arg == "--cli-flags-config" {
            return iter.next().map(|value| Some(value.clone())).ok_or(
                BrokerRuntimeConfigError::MissingCliValue(match arg.as_str() {
                    "--config" => "--config",
                    "--cli-flags" => "--cli-flags",
                    _ => "--cli-flags-config",
                }),
            );
        }
        if arg.starts_with("--config") || arg.starts_with("--cli-flags") {
            return Err(BrokerRuntimeConfigError::InvalidConfigFlag(arg.clone()));
        }
    }
    Ok(None)
}

fn strip_config_selector(
    args: impl IntoIterator<Item = String>,
) -> Result<Vec<String>, BrokerRuntimeConfigError> {
    let mut stripped = Vec::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg
            .strip_prefix("--config=")
            .or_else(|| arg.strip_prefix("--cli-flags="))
            .or_else(|| arg.strip_prefix("--cli-flags-config="))
            .is_some()
        {
            continue;
        }
        if arg == "--config" || arg == "--cli-flags" || arg == "--cli-flags-config" {
            iter.next()
                .ok_or(BrokerRuntimeConfigError::MissingCliValue(
                    match arg.as_str() {
                        "--config" => "--config",
                        "--cli-flags" => "--cli-flags",
                        _ => "--cli-flags-config",
                    },
                ))?;
            continue;
        }
        stripped.push(arg);
    }
    Ok(stripped)
}

fn find_upward(start: &Path, filename: &str) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    loop {
        let candidate = dir.join(filename);
        if candidate.is_file() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

fn parse_cli_overrides(
    config_path: &Path,
    args: &[String],
) -> Result<HashMap<String, String>, BrokerRuntimeConfigError> {
    let config_c = cstring(config_path.to_string_lossy().as_ref())?;
    let audit_status = unsafe { f2e_audit_config_status_from_file(config_c.as_ptr()) };
    if audit_status != 0 {
        let report = unsafe { take_owned_string(f2e_audit_config_from_file(config_c.as_ptr())) };
        return Err(BrokerRuntimeConfigError::CliFlagsConfigInvalid {
            path: config_path.to_path_buf(),
            report,
        });
    }

    let argv_json =
        serde_json::to_string(args).expect("a Vec<String> should always serialize to JSON");
    let argv_c = cstring(&argv_json)?;
    let raw = unsafe {
        take_owned_string(f2e_parse_json_argv_from_file(
            config_c.as_ptr(),
            argv_c.as_ptr(),
        ))
    };
    serde_json::from_str(&raw)
        .map_err(|source| BrokerRuntimeConfigError::CliParserJson { raw, source })
}

fn render_help(config_path: &Path, command_name: &str) -> Result<String, BrokerRuntimeConfigError> {
    let config_c = cstring(config_path.to_string_lossy().as_ref())?;
    let command_c = cstring(command_name)?;
    let table = unsafe {
        take_owned_string(f2e_help_table_from_file(
            config_c.as_ptr(),
            command_c.as_ptr(),
            0,
        ))
    };
    if table.trim().is_empty() {
        Ok(format!("{USAGE}\n"))
    } else {
        Ok(format!("{USAGE}\n\n{table}"))
    }
}

unsafe fn take_owned_string(value: *mut c_char) -> String {
    if value.is_null() {
        return String::new();
    }
    let raw = CStr::from_ptr(value).to_string_lossy().into_owned();
    f2e_free(value);
    raw
}

fn cstring(value: &str) -> Result<CString, BrokerRuntimeConfigError> {
    CString::new(value).map_err(BrokerRuntimeConfigError::CliParserCString)
}

fn command_name(args: &[String]) -> String {
    args.first()
        .and_then(|arg| Path::new(arg).file_name())
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("lmxd")
        .to_string()
}

fn take_json_list(
    values: &mut HashMap<String, String>,
    env: &'static str,
) -> Result<Vec<String>, BrokerRuntimeConfigError> {
    match values.remove(env) {
        Some(value) => serde_json::from_str(&value)
            .map_err(|_| BrokerRuntimeConfigError::InvalidJsonList { env, value }),
        None => Ok(Vec::new()),
    }
}

fn setting(
    cli: &HashMap<String, String>,
    env: &HashMap<String, String>,
    key: &'static str,
) -> Option<String> {
    cli.get(key)
        .map(|value| value.trim().to_string())
        .or_else(|| non_empty_env(env, key))
}

fn optional_setting(
    cli: &HashMap<String, String>,
    env: &HashMap<String, String>,
    key: &'static str,
) -> Option<String> {
    match cli.get(key) {
        Some(value) => {
            let value = value.trim();
            if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            }
        }
        None => non_empty_env(env, key),
    }
}

fn non_empty_map(values: &HashMap<String, String>, key: &'static str) -> Option<String> {
    values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn non_empty_env(env: &HashMap<String, String>, key: &'static str) -> Option<String> {
    non_empty_map(env, key)
}

fn parse_codec(raw: &str) -> Result<WireCodec, BrokerRuntimeConfigError> {
    WireCodec::parse(raw).ok_or_else(|| BrokerRuntimeConfigError::InvalidCodec(raw.to_string()))
}

fn parse_quorum_policy(
    cli: &HashMap<String, String>,
    env: &HashMap<String, String>,
) -> Result<QuorumPolicy, BrokerRuntimeConfigError> {
    match setting(cli, env, ENV_QUORUM_POLICY) {
        None => Ok(QuorumPolicy::Majority),
        Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "majority" | "maj" => Ok(QuorumPolicy::Majority),
            "grid" | "sqrt" | "maekawa" => Ok(QuorumPolicy::Grid),
            _ => Err(BrokerRuntimeConfigError::InvalidQuorumPolicy(raw)),
        },
    }
}

fn parse_node_id(raw: &str) -> Result<NodeId, BrokerRuntimeConfigError> {
    raw.parse::<NodeId>()
        .map_err(|_| BrokerRuntimeConfigError::InvalidNodeId(raw.to_string()))
}

fn parse_peer_addrs(raw: &str) -> Result<Vec<String>, BrokerRuntimeConfigError> {
    let addrs = if raw.trim_start().starts_with('[') {
        serde_json::from_str::<Vec<String>>(raw)
            .map_err(|_| BrokerRuntimeConfigError::InvalidPeerAddrs(raw.to_string()))?
            .into_iter()
            .map(|addr| addr.trim().to_string())
            .filter(|addr| !addr.is_empty())
            .collect::<Vec<_>>()
    } else {
        parse_csv_list(raw)
    };
    if addrs.is_empty() {
        return Err(BrokerRuntimeConfigError::InvalidPeerAddrs(raw.to_string()));
    }
    validate_peer_addrs(&addrs)?;
    Ok(addrs)
}

fn validate_peer_addrs(addrs: &[String]) -> Result<(), BrokerRuntimeConfigError> {
    if addrs.is_empty() {
        return Err(BrokerRuntimeConfigError::MissingPeerAddrs);
    }
    let mut seen = std::collections::HashSet::with_capacity(addrs.len());
    for addr in addrs {
        validate_socketish_addr(ENV_PEER_ADDRS, addr)?;
        if !seen.insert(addr) {
            return Err(BrokerRuntimeConfigError::InvalidPeerAddrs(format!(
                "duplicate address {addr:?}"
            )));
        }
    }
    Ok(())
}

fn optional_socket_addr_setting(
    cli: &HashMap<String, String>,
    env: &HashMap<String, String>,
    key: &'static str,
) -> Result<Option<String>, BrokerRuntimeConfigError> {
    let Some(value) = optional_setting(cli, env, key) else {
        return Ok(None);
    };
    validate_socketish_addr(key, &value)?;
    Ok(Some(value))
}

fn validate_socketish_addr(env: &'static str, value: &str) -> Result<(), BrokerRuntimeConfigError> {
    if value.is_empty()
        || value.starts_with('-')
        || value.chars().any(char::is_whitespace)
        || value.starts_with(':')
    {
        return Err(BrokerRuntimeConfigError::InvalidSocketAddr {
            env,
            value: value.to_string(),
        });
    }
    let Some((host, port)) = value.rsplit_once(':') else {
        return Err(BrokerRuntimeConfigError::InvalidSocketAddr {
            env,
            value: value.to_string(),
        });
    };
    if host.is_empty() || port.parse::<u16>().is_err() {
        return Err(BrokerRuntimeConfigError::InvalidSocketAddr {
            env,
            value: value.to_string(),
        });
    }
    Ok(())
}

fn parse_transport(
    cli: &HashMap<String, String>,
    env: &HashMap<String, String>,
    codec: WireCodec,
) -> Result<TransportSettings, BrokerRuntimeConfigError> {
    Ok(TransportSettings {
        codec,
        connect_retry: Duration::from_millis(parse_u64_setting(
            cli,
            env,
            ENV_CONNECT_RETRY_MS,
            DEFAULT_CONNECT_RETRY_MS,
            MIN_CONNECT_RETRY_MS,
            MAX_CONNECT_RETRY_MS,
        )?),
        tick: Duration::from_millis(parse_u64_setting(
            cli,
            env,
            ENV_TICK_MS,
            DEFAULT_TICK_MS,
            MIN_TICK_MS,
            RENEW_INTERVAL,
        )?),
        max_line_frame: parse_usize_setting(
            cli,
            env,
            ENV_MAX_FRAME_BYTES,
            DEFAULT_MAX_FRAME_BYTES,
            MIN_MAX_FRAME_BYTES,
            MAX_MAX_FRAME_BYTES,
        )?,
        quorum_policy: parse_quorum_policy(cli, env)?,
    })
}

fn parse_demo(
    cli: &HashMap<String, String>,
    env: &HashMap<String, String>,
) -> Result<BrokerDemoConfig, BrokerRuntimeConfigError> {
    let enabled = setting(cli, env, ENV_DEMO)
        .map(|raw| parse_bool(ENV_DEMO, &raw))
        .transpose()?
        .unwrap_or(false);
    let keys_raw = setting(cli, env, ENV_DEMO_KEYS);
    let keys = keys_raw
        .as_deref()
        .map(parse_csv_list)
        .unwrap_or_else(|| parse_csv_list(DEFAULT_DEMO_KEYS));

    if enabled && keys.is_empty() {
        return Err(BrokerRuntimeConfigError::InvalidDemoKeys(
            keys_raw.unwrap_or_default(),
        ));
    }
    if enabled {
        Composite::new(&keys)
            .map_err(|err| BrokerRuntimeConfigError::InvalidDemoKeys(err.to_string()))?;
    }
    let hold = Duration::from_millis(parse_u64_setting(
        cli,
        env,
        ENV_DEMO_HOLD_MS,
        DEFAULT_DEMO_HOLD_MS,
        1,
        MAX_DEMO_DELAY_MS,
    )?);
    let rest = Duration::from_millis(parse_u64_setting(
        cli,
        env,
        ENV_DEMO_REST_MS,
        DEFAULT_DEMO_REST_MS,
        1,
        MAX_DEMO_DELAY_MS,
    )?);

    Ok(BrokerDemoConfig {
        enabled,
        keys,
        hold,
        rest,
    })
}

fn parse_bool_setting(
    cli: &HashMap<String, String>,
    env: &HashMap<String, String>,
    key: &'static str,
    default: bool,
) -> Result<bool, BrokerRuntimeConfigError> {
    setting(cli, env, key)
        .map(|raw| parse_bool(key, &raw))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_bool(env: &'static str, raw: &str) -> Result<bool, BrokerRuntimeConfigError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "0" | "false" | "f" | "no" | "off" => Ok(false),
        "1" | "true" | "t" | "yes" | "on" => Ok(true),
        _ => Err(BrokerRuntimeConfigError::InvalidBool {
            env,
            value: raw.to_string(),
        }),
    }
}

fn parse_u64_setting(
    cli: &HashMap<String, String>,
    env: &HashMap<String, String>,
    key: &'static str,
    default: u64,
    min: u64,
    max: u64,
) -> Result<u64, BrokerRuntimeConfigError> {
    let Some(raw) = setting(cli, env, key) else {
        return Ok(default);
    };
    let value = raw
        .parse::<u64>()
        .map_err(|_| BrokerRuntimeConfigError::InvalidNumber {
            env: key,
            value: raw,
        })?;
    if value < min || value > max {
        return Err(BrokerRuntimeConfigError::NumberOutOfRange {
            env: key,
            value,
            min,
            max,
        });
    }
    Ok(value)
}

fn parse_usize_setting(
    cli: &HashMap<String, String>,
    env: &HashMap<String, String>,
    key: &'static str,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize, BrokerRuntimeConfigError> {
    let value = parse_u64_setting(cli, env, key, default as u64, min as u64, max as u64)?;
    usize::try_from(value).map_err(|_| BrokerRuntimeConfigError::NumberOutOfRange {
        env: key,
        value,
        min: min as u64,
        max: max as u64,
    })
}

fn parse_csv_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn env(values: &[(&str, &str)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn cli_flags_and_positionals_win_over_env() {
        let env = env(&[
            (ENV_CODEC, "text"),
            (ENV_NODE_ID, "2"),
            (ENV_PEER_ADDRS, "env0:9100,env1:9101,env2:9102"),
            (ENV_CLIENT_HTTP, "127.0.0.1:9990"),
            (ENV_CLIENT_TCP, "127.0.0.1:9991"),
            (ENV_DEMO, "false"),
            (ENV_DEMO_KEYS, "env-a,env-b"),
        ]);
        let config = BrokerRuntimeConfig::from_env_and_args(
            &env,
            &args(&[
                "lmxd",
                "--codec",
                "json",
                "--client-http",
                "127.0.0.1:9200",
                "--client-tcp=127.0.0.1:9300",
                "--demo=yes",
                "--demo-keys",
                "cli-a,cli-b",
                "1",
                "127.0.0.1:9100",
                "127.0.0.1:9101",
                "127.0.0.1:9102",
            ]),
        )
        .unwrap();

        assert_eq!(config.codec, WireCodec::Json);
        assert_eq!(config.id, 1);
        assert_eq!(
            config.addrs,
            vec![
                "127.0.0.1:9100".to_string(),
                "127.0.0.1:9101".to_string(),
                "127.0.0.1:9102".to_string()
            ]
        );
        assert_eq!(config.client_http.as_deref(), Some("127.0.0.1:9200"));
        assert_eq!(config.client_tcp.as_deref(), Some("127.0.0.1:9300"));
        assert!(config.demo.enabled);
        assert_eq!(
            config.demo.keys,
            vec!["cli-a".to_string(), "cli-b".to_string()]
        );
    }

    #[test]
    fn env_is_used_when_cli_omits_values() {
        let env = env(&[
            (ENV_CODEC, "msgpack"),
            (ENV_NODE_ID, "0"),
            (ENV_PEER_ADDRS, "127.0.0.1:9100,127.0.0.1:9101"),
            (ENV_CLIENT_HTTP, "127.0.0.1:9200"),
            (ENV_DEMO, "1"),
            (ENV_DEMO_KEYS, "cap,mid"),
        ]);

        let config = BrokerRuntimeConfig::from_env_and_args(&env, &args(&["lmxd"])).unwrap();

        assert_eq!(config.codec, WireCodec::Msgpack);
        assert_eq!(config.id, 0);
        assert_eq!(config.addrs.len(), 2);
        assert_eq!(config.client_http.as_deref(), Some("127.0.0.1:9200"));
        assert_eq!(config.client_tcp, None);
        assert!(config.demo.enabled);
        assert_eq!(config.demo.keys, vec!["cap".to_string(), "mid".to_string()]);
    }

    #[test]
    fn named_node_flags_win_over_positionals() {
        let env = env(&[
            (ENV_NODE_ID, "0"),
            (ENV_PEER_ADDRS, "env0:9100,env1:9101,env2:9102"),
        ]);
        let config = BrokerRuntimeConfig::from_env_and_args(
            &env,
            &args(&[
                "lmxd",
                "--node-id",
                "2",
                "--peer-addrs",
                "cli0:9100,cli1:9101,cli2:9102",
                "1",
                "pos0:9100",
                "pos1:9101",
            ]),
        )
        .unwrap();

        assert_eq!(config.id, 2);
        assert_eq!(
            config.addrs,
            vec![
                "cli0:9100".to_string(),
                "cli1:9101".to_string(),
                "cli2:9102".to_string()
            ]
        );
    }

    #[test]
    fn unknown_flag_before_positionals_errors() {
        let err = BrokerRuntimeConfig::from_env_and_args(
            &HashMap::new(),
            &args(&["lmxd", "--bogus", "0", "127.0.0.1:9100"]),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            BrokerRuntimeConfigError::UnknownCliOptions(_)
        ));
    }

    #[test]
    fn new_broker_tunables_parse_from_cli() {
        let config = BrokerRuntimeConfig::from_env_and_args(
            &HashMap::new(),
            &args(&[
                "lmxd",
                "--codec=msgpack",
                "--no-stdin",
                "--no-log-info",
                "--connect-retry-ms=250",
                "--tick-ms=100",
                "--max-frame-bytes=2048",
                "--demo",
                "--demo-keys=cap,mid,zed",
                "--demo-hold-ms=10",
                "--demo-rest-ms=20",
                "0",
                "127.0.0.1:9100",
            ]),
        )
        .unwrap();

        assert_eq!(config.codec, WireCodec::Msgpack);
        assert!(!config.stdin);
        assert!(!config.log_info);
        assert_eq!(config.transport.codec, WireCodec::Msgpack);
        assert_eq!(config.transport.connect_retry, Duration::from_millis(250));
        assert_eq!(config.transport.tick, Duration::from_millis(100));
        assert_eq!(config.transport.max_line_frame, 2048);
        assert!(config.demo.enabled);
        assert_eq!(config.demo.hold, Duration::from_millis(10));
        assert_eq!(config.demo.rest, Duration::from_millis(20));
    }

    #[test]
    fn empty_optional_cli_listener_disables_env_listener() {
        let config = BrokerRuntimeConfig::from_env_and_args(
            &env(&[(ENV_CLIENT_HTTP, "127.0.0.1:9200")]),
            &args(&["lmxd", "--client-http=", "0", "127.0.0.1:9100"]),
        )
        .unwrap();

        assert_eq!(config.client_http, None);
    }

    #[test]
    fn empty_codec_cli_value_does_not_fall_back_to_env() {
        let err = BrokerRuntimeConfig::from_env_and_args(
            &env(&[(ENV_CODEC, "json")]),
            &args(&["lmxd", "--codec=", "0", "127.0.0.1:9100"]),
        )
        .unwrap_err();

        assert!(matches!(err, BrokerRuntimeConfigError::InvalidCodec(_)));
    }

    #[test]
    fn empty_peer_addrs_cli_value_does_not_fall_back_to_env() {
        let err = BrokerRuntimeConfig::from_env_and_args(
            &env(&[(ENV_NODE_ID, "0"), (ENV_PEER_ADDRS, "127.0.0.1:9100")]),
            &args(&["lmxd", "--peer-addrs="]),
        )
        .unwrap_err();

        assert!(matches!(err, BrokerRuntimeConfigError::InvalidPeerAddrs(_)));
    }

    #[test]
    fn positional_addresses_are_validated() {
        let err = BrokerRuntimeConfig::from_env_and_args(
            &HashMap::new(),
            &args(&["lmxd", "0", "--not-an-address"]),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            BrokerRuntimeConfigError::InvalidSocketAddr { .. }
        ));
    }

    #[test]
    fn transport_bounds_are_enforced() {
        let err = BrokerRuntimeConfig::from_env_and_args(
            &env(&[(ENV_TICK_MS, "999999")]),
            &args(&["lmxd", "0", "127.0.0.1:9100"]),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            BrokerRuntimeConfigError::NumberOutOfRange {
                env: ENV_TICK_MS,
                ..
            }
        ));
    }

    #[test]
    fn config_flag_is_stripped_before_flags2env_parse() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(CLI_FLAGS_FILE);
        let config = BrokerRuntimeConfig::from_env_and_args(
            &HashMap::new(),
            &args(&[
                "lmxd",
                "--config",
                path.to_str().unwrap(),
                "0",
                "127.0.0.1:9100",
            ]),
        )
        .unwrap();

        assert_eq!(config.id, 0);
        assert_eq!(config.addrs, vec!["127.0.0.1:9100".to_string()]);
    }

    #[test]
    fn short_help_is_supported() {
        let err = BrokerRuntimeConfig::from_env_and_args(&HashMap::new(), &args(&["lmxd", "-h"]))
            .unwrap_err();

        assert!(matches!(err, BrokerRuntimeConfigError::HelpRequested(_)));
    }

    #[test]
    fn embedded_cli_flags_config_is_parseable() {
        let path = embedded_cli_flags_config_path().unwrap();
        let parsed = parse_cli_overrides(
            &path,
            &args(&["--codec=json", "--no-stdin", "0", "127.0.0.1:9100"]),
        )
        .unwrap();

        assert_eq!(parsed.get(ENV_CODEC).map(String::as_str), Some("json"));
        assert_eq!(parsed.get(ENV_STDIN).map(String::as_str), Some("false"));
    }
}

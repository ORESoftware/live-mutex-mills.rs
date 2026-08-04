#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

cargo test --locked broker_runtime_config::tests
cargo build --locked --bin lmxd

binary="$repo_root/target/debug/lmxd"
test -x "$binary"

scratch="$(mktemp -d)"
hostile_cwd="$scratch/hostile-cwd"
stdout_log="$scratch/stdout.log"
stderr_log="$scratch/stderr.log"
mkdir -p "$hostile_cwd"
trap 'rm -rf "$scratch"' EXIT

cat >"$hostile_cwd/.cli-flags.toml" <<'TOML'
[parse]
allow_unknown = true

[flags.attacker_owned]
env = "ATTACKER_OWNED"
aliases = ["attacker-owned"]
type = "string"
TOML

# The process working directory is never a policy source. The reviewed
# compile-time/package contract must own the help surface.
(
  cd "$hostile_cwd"
  env -u LMX_CLI_FLAGS_CONFIG -u FLAGS2ENV_CONFIG "$binary" --help
) >"$stdout_log" 2>"$stderr_log"
grep -F -- "--codec" "$stdout_log"
if grep -F -- "--attacker-owned" "$stdout_log" "$stderr_log"; then
  echo "hostile working-directory contract reached the help surface" >&2
  exit 1
fi

set +e
(
  cd "$hostile_cwd"
  LMX_CLI_FLAGS_CONFIG=attacker-runtime-secret.toml "$binary" --help
) >"$stdout_log" 2>"$stderr_log"
relative_status=$?
set -e
if [[ $relative_status -ne 2 ]]; then
  echo "relative selector returned unexpected status: $relative_status" >&2
  cat "$stderr_log" >&2
  exit 1
fi
grep -F -- "broker CLI contract selector must be an absolute path" "$stderr_log"
if grep -F -- "attacker-runtime-secret.toml" "$stdout_log" "$stderr_log" \
  || grep -F -- "runtime-secret" "$stdout_log" "$stderr_log"; then
  echo "relative selector leaked into process output" >&2
  exit 1
fi

missing_selector="$scratch/missing-runtime-secret.toml"
set +e
LMX_CLI_FLAGS_CONFIG="$missing_selector" "$binary" --help \
  >"$stdout_log" 2>"$stderr_log"
missing_status=$?
set -e
if [[ $missing_status -ne 2 ]]; then
  echo "missing selector returned unexpected status: $missing_status" >&2
  cat "$stderr_log" >&2
  exit 1
fi
grep -F -- "does not name a readable regular file" "$stderr_log"
if grep -F -- "$missing_selector" "$stdout_log" "$stderr_log" \
  || grep -F -- "runtime-secret" "$stdout_log" "$stderr_log"; then
  echo "missing selector leaked into process output" >&2
  exit 1
fi

rejected_value="postgres://runtime-secret@redacted.invalid/lmx"
set +e
"$binary" \
  "--definitely-not-a-real-flag=$rejected_value" \
  0 127.0.0.1:9100 \
  >"$stdout_log" 2>"$stderr_log"
unknown_status=$?
set -e
if [[ $unknown_status -ne 2 ]]; then
  echo "unknown option returned unexpected status: $unknown_status" >&2
  cat "$stderr_log" >&2
  exit 1
fi
grep -F -- "unknown CLI option" "$stderr_log"
if grep -F -- "$rejected_value" "$stdout_log" "$stderr_log" \
  || grep -F -- "runtime-secret" "$stdout_log" "$stderr_log"; then
  echo "unknown option value leaked into process output" >&2
  exit 1
fi

invalid_value="not-a-node-runtime-secret"
set +e
"$binary" \
  "--node-id=$invalid_value" \
  --peer-addrs=127.0.0.1:9100 \
  >"$stdout_log" 2>"$stderr_log"
invalid_status=$?
set -e
if [[ $invalid_status -ne 2 ]]; then
  echo "invalid typed option returned unexpected status: $invalid_status" >&2
  cat "$stderr_log" >&2
  exit 1
fi
grep -F -- "invalid CLI flag value" "$stderr_log"
if grep -F -- "$invalid_value" "$stdout_log" "$stderr_log" \
  || grep -F -- "runtime-secret" "$stdout_log" "$stderr_log"; then
  echo "invalid typed option leaked into process output" >&2
  exit 1
fi

for forbidden in 'current_dir(' 'find_upward('; do
  if grep -F -- "$forbidden" src/broker_runtime_config.rs; then
    echo "forbidden ambient contract discovery remains: $forbidden" >&2
    exit 1
  fi
done

test "$(git hash-object vendor/flags2env/native/parser.c)" \
  = "2567d723ccdf9f0703dda5dfebac8ac2cb0ff2dd"
test "$(git hash-object vendor/flags2env/native/parser.h)" \
  = "56668698e50bafd8ce1dc49518a93bf9e564d7b5"

echo "live-mutex-mills trusted flags runtime smoke passed"

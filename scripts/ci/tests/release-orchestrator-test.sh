#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
script="$repo_root/scripts/ci/release-orchestrator.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_kind() {
  local version=$1 expected=$2 actual
  actual=$(VERSION="$version" "$script" release-kind)
  [ "$actual" = "$expected" ] || fail "$version classified as $actual, expected $expected"
}

assert_invalid() {
  local version=$1
  if VERSION="$version" "$script" validate-version-format >/dev/null 2>&1; then
    fail "$version should be rejected"
  fi
}

assert_kind v1.6.0 final
assert_kind v1.6.0-rc1 rc
assert_kind v1.6.0-rc.2 rc
for version in v1.6 v1.6.0-rc v1.6.0-rc0 v1.6.0-rc.0 v1.6.0-rc01 v1.6.0-beta1 1.6.0; do
  assert_invalid "$version"
done

export_output="$tmp/export-output"
GITHUB_OUTPUT="$export_output" BACKEND_TAG_EXISTS=true VERSION=v1.6.0 "$script" export-release-vars
grep -qx 'backend-checkout-ref=v1.6.0' "$export_output" || fail "external tags must be tested directly"
: > "$export_output"
GITHUB_OUTPUT="$export_output" BACKEND_TAG_EXISTS=false VERSION=v1.6.0 "$script" export-release-vars
grep -qx 'backend-checkout-ref=release/1.6.0' "$export_output" || fail "orchestrated releases must test the prepared branch"

mkdir -p "$tmp/bin" "$tmp/state"
cat > "$tmp/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FAKE_GH_LOG"

if [ "${1:-}" = "run" ] && [ "${2:-}" = "watch" ]; then
  if [ "$FAKE_GH_SCENARIO" = "watch-timeout" ]; then
    sleep 5
  fi
  exit 0
fi
if [ "${1:-}" = "run" ] && [ "${2:-}" = "cancel" ]; then
  exit 0
fi

args=" $* "
if [[ "$args" == *"/git/ref/heads/"* ]]; then
  printf '%s\n' "$EXPECTED_SHA"
  exit 0
fi
if [[ "$args" == *"/git/ref/tags/"* ]]; then
  case "$FAKE_GH_SCENARIO" in
    tag-missing) exit 1 ;;
    tag-matching) printf '{"object":{"type":"commit","sha":"%s"}}\n' "$EXPECTED_SHA" ;;
    tag-conflict) printf '{"object":{"type":"commit","sha":"other-sha"}}\n' ;;
    tag-race)
      if [ -f "$FAKE_GH_STATE/created" ]; then
        printf '{"object":{"type":"commit","sha":"%s"}}\n' "$EXPECTED_SHA"
      else
        exit 1
      fi
      ;;
  esac
  exit 0
fi
if [[ "$args" == *"/git/refs "* ]]; then
  if [ "$FAKE_GH_SCENARIO" = "tag-race" ]; then
    touch "$FAKE_GH_STATE/created"
    exit 1
  fi
  exit 0
fi
if [[ "$args" == *"/actions/workflows/"*"/dispatches"* ]]; then
  if [ "$FAKE_GH_SCENARIO" = "dispatch-missing-id" ]; then
    printf '{}\n'
  else
    printf '{"workflow_run_id":12345,"html_url":"https://github.com/example/actions/runs/12345"}\n'
  fi
  exit 0
fi

echo "Unexpected fake gh call: $*" >&2
exit 1
EOF
chmod +x "$tmp/bin/gh"

export PATH="$tmp/bin:$PATH"
export FAKE_GH_LOG="$tmp/gh.log"
export FAKE_GH_STATE="$tmp/state"
export EXPECTED_SHA=expected-sha
export ORG=pbinitiative VERSION=v1.6.0 RELEASE_BRANCH=release/1.6.0
export BACKEND_REPO=zenbpm FRONTEND_REPO=zenbpm-ui

export FAKE_GH_SCENARIO=tag-missing
: > "$FAKE_GH_LOG"
"$script" tag-backend >/dev/null

export FAKE_GH_SCENARIO=tag-matching
"$script" tag-backend >/dev/null

export FAKE_GH_SCENARIO=tag-conflict
if "$script" tag-backend >/dev/null 2>&1; then
  fail "a tag pointing to another commit should be rejected"
fi

export FAKE_GH_SCENARIO=tag-race
rm -f "$FAKE_GH_STATE/created"
"$script" tag-backend >/dev/null

export FAKE_GH_SCENARIO=dispatch
export GITHUB_OUTPUT="$tmp/output"
: > "$GITHUB_OUTPUT"
"$script" dispatch-frontend-release >/dev/null
grep -qx 'run-id=12345' "$GITHUB_OUTPUT" || fail "dispatch did not export the exact run ID"
grep -qx 'run-url=https://github.com/example/actions/runs/12345' "$GITHUB_OUTPUT" || fail "dispatch did not export the run URL"

export FAKE_GH_SCENARIO=dispatch-missing-id
if "$script" dispatch-frontend-release >/dev/null 2>&1; then
  fail "dispatch without a returned run ID should fail"
fi

export FAKE_GH_SCENARIO=dispatch
export RUN_ID=12345
: > "$FAKE_GH_LOG"
"$script" wait-frontend-release >/dev/null
grep -qx 'run watch 12345 --repo pbinitiative/zenbpm-ui --exit-status' "$FAKE_GH_LOG" || fail "wait did not watch the exact run ID"

export FAKE_GH_SCENARIO=watch-timeout
if WORKFLOW_WAIT_TIMEOUT_SECONDS=1 "$script" wait-frontend-release >"$tmp/wait-output" 2>&1; then
  fail "a stalled workflow should time out"
fi
grep -q 'Timed out waiting for pbinitiative/zenbpm-ui run 12345 after 1s' "$tmp/wait-output" || fail "wait timeout was not reported"
grep -qx 'run cancel 12345 --repo pbinitiative/zenbpm-ui' "$FAKE_GH_LOG" || fail "timed-out workflow was not canceled"

echo "release orchestrator tests passed"

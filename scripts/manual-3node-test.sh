#!/usr/bin/env bash
# Manual 3-node / 1-partition cluster smoke test.
#
# Builds zenbpm, opens a single tilix window split into 3 tiles (one per node)
# so the engine logs are visible live, then from this terminal:
#   - deploys pkg/bpmn/test-cases/simple_task.bpmn on node-1,
#   - creates a process instance on node-1,
#   - reads the instance back from node-2 and node-3.
#
# Each pane runs a restart loop: after Ctrl-C the node exits and the pane asks
# whether to restart it. Restart reuses the same data dir so the node rejoins
# the existing raft cluster via its joinAddresses.
#
# Does NOT tear down the cluster at the end — close the tilix window or run:
#   pkill -f /tmp/zenbpm-manual
set -uo pipefail

REPO="${REPO:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
BIN="/tmp/zenbpm-manual"
SESSION_DIR="$(mktemp -d -t zen-3node-XXXXXX)"

if ! command -v tilix >/dev/null; then
  echo "tilix not found (install with: sudo apt install tilix)"
  exit 1
fi

echo ">> build"
( cd "$REPO" && go build -o "$BIN" ./cmd/zenbpm/main.go ) || { echo "build failed"; exit 1; }

echo ">> write 3 node configs to $SESSION_DIR"
for i in 1 2 3; do
  cat > "$SESSION_DIR/conf-node$i.yaml" <<EOF
name: zenbpm
httpServer:
  context: /
  addr: :808$i
grpcServer:
  addr: :909$i
cluster:
  addr: localhost:809$i
  adv: localhost:809$i
  raft:
    dir: $SESSION_DIR/node-$i
    bootstrapExpectTimeout: 2m
    bootstrapExpect: 3
    joinAttempts: 20
    joinInterval: 1s
    joinAddresses:
      - localhost:8091
      - localhost:8092
      - localhost:8093
  nodeId: node-$i
  script:
    feel:
      maxVmPoolSize: 10
      minVmPoolSize: 2
    js:
      maxVmPoolSize: 10
      minVmPoolSize: 2
EOF
done

NODE_RUNNER="$SESSION_DIR/run-node.sh"
cat > "$NODE_RUNNER" <<'RUNNER'
#!/usr/bin/env bash
# Runs one zenbpm node in a loop. When the binary exits (e.g. after Ctrl-C)
# the user can restart it — the data dir is preserved so raft rejoins the
# cluster via the configured joinAddresses.
# Argv: <session_dir> <node_id> <binary_path>
set -u
SESSION_DIR="$1"
NODE_ID="$2"
BIN="$3"
while true; do
  clear
  echo "=== zenbpm node-$NODE_ID (http=:808$NODE_ID cluster=:809$NODE_ID) ==="
  echo "config: $SESSION_DIR/conf-node$NODE_ID.yaml"
  echo "data:   $SESSION_DIR/node-$NODE_ID"
  echo "pid shell: $$"
  echo "(Ctrl-C stops the node; you'll be prompted to restart it)"
  echo
  CONFIG_FILE="$SESSION_DIR/conf-node$NODE_ID.yaml" LOG_LEVEL=INFO "$BIN"
  status=$?
  echo
  echo "[node-$NODE_ID exited with status $status]"
  printf '[r]estart and rejoin cluster, or [q]uit: '
  read -r ans
  case "$ans" in
    r|R|'') echo ">> restarting node-$NODE_ID ..."; sleep 0.3 ;;
    *) break ;;
  esac
done
RUNNER
chmod +x "$NODE_RUNNER"

echo ">> launch tilix window with 3 tiled panes"
# Pane 1: open a new tilix window.
tilix --title="zenbpm node-1" -e bash -c \
  "'$NODE_RUNNER' '$SESSION_DIR' 1 '$BIN'" &
# Give tilix time to open the window and take focus so session-add-* attaches
# to this window rather than any other tilix window the user may have open.
sleep 1.5
# Pane 2: split the current pane horizontally -> node-2 on the right.
tilix --action=session-add-right --title="zenbpm node-2" -e bash -c \
  "'$NODE_RUNNER' '$SESSION_DIR' 2 '$BIN'" &
sleep 0.8
# Pane 3: split the focused (right) pane vertically -> node-3 below node-2.
tilix --action=session-add-down --title="zenbpm node-3" -e bash -c \
  "'$NODE_RUNNER' '$SESSION_DIR' 3 '$BIN'" &
sleep 0.8

echo ">> waiting for cluster to become healthy (up to 120s)"
healthy=0
for attempt in $(seq 1 120); do
  status=$(curl -fs http://localhost:8081/system/status 2>/dev/null || true)
  if [[ -n "$status" ]]; then
    nodes=$(printf '%s' "$status" | jq '.nodes | length' 2>/dev/null || echo 0)
    leader=$(printf '%s' "$status" | jq -r '.partitions["1"].leaderId // ""' 2>/dev/null || echo "")
    # /system/status renders enums via String() (see internal/rest/cluster_status.go).
    inited=$(printf '%s' "$status" | jq '[.nodes[].partitions["1"] | select(.state == "NodePartitionStateInitialized")] | length' 2>/dev/null || echo 0)
    if [[ "$nodes" -ge 3 && -n "$leader" && "$inited" -ge 3 ]]; then
      echo "   HEALTHY — nodes=$nodes  partition-1 leader=$leader  initialized=$inited"
      healthy=1
      break
    fi
    printf "   attempt %3d: nodes=%s leader='%s' initialized=%s\r" "$attempt" "$nodes" "$leader" "$inited"
  fi
  sleep 1
done
echo
if [[ "$healthy" -ne 1 ]]; then
  echo "   TIMEOUT. Last /system/status:"
  printf '%s\n' "$status" | jq . 2>/dev/null || printf '%s\n' "$status"
  echo "   nodes are still running — close the tilix window to stop them."
  exit 1
fi

BPMN="$REPO/pkg/bpmn/test-cases/simple_task.bpmn"
echo
echo ">> POST /v1/process-definitions on node-1"
curl -fsS -X POST "http://localhost:8081/v1/process-definitions" \
  -F "resource=@$BPMN" | jq .

echo
echo ">> fetch definition key from node-1 (preserving int64 precision)"
def_key=""
for attempt in $(seq 1 20); do
  def_resp=$(curl -fs "http://localhost:8081/v1/process-definitions" || true)
  def_key=$(printf '%s' "$def_resp" \
    | grep -oE '"key"[[:space:]]*:[[:space:]]*[0-9]+' \
    | head -1 | grep -oE '[0-9]+$' || true)
  [[ -n "$def_key" ]] && break
  sleep 0.5
done
if [[ -z "$def_key" ]]; then
  echo "   definition never appeared on node-1"
  exit 1
fi
echo "   definition key: $def_key"

echo
echo ">> POST /v1/process-instances on node-1"
inst_resp=$(curl -fsS -X POST "http://localhost:8081/v1/process-instances" \
  -H "Content-Type: application/json" \
  -d "{\"processDefinitionKey\": $def_key}")
printf '%s\n' "$inst_resp" | jq .
inst_key=$(printf '%s' "$inst_resp" \
  | grep -oE '"key"[[:space:]]*:[[:space:]]*[0-9]+' \
  | head -1 | grep -oE '[0-9]+$')
if [[ -z "$inst_key" ]]; then
  echo "   could not parse instance key from response"
  exit 1
fi
echo "   instance key: $inst_key"

for i in 2 3; do
  echo
  echo ">> GET /v1/process-instances/$inst_key on node-$i"
  seen=0
  for attempt in $(seq 1 60); do
    body=$(curl -fsS "http://localhost:808$i/v1/process-instances/$inst_key" 2>&1 || true)
    if printf '%s' "$body" | grep -q "\"key\"[[:space:]]*:[[:space:]]*$inst_key"; then
      echo "   visible after ${attempt} attempt(s):"
      printf '%s' "$body" | jq . 2>/dev/null || printf '%s\n' "$body"
      seen=1
      break
    fi
    sleep 0.5
  done
  if [[ "$seen" -ne 1 ]]; then
    echo "   NOT VISIBLE after 30s. Last response:"
    printf '%s\n' "$body"
  fi
done

echo
echo ">> done."
echo "   cluster is still running."
echo "   stop a node with Ctrl-C in its pane; the pane will prompt to restart."
echo "   tear down entirely: close the tilix window, or run:  pkill -f $BIN"
echo "   session dir:  $SESSION_DIR"

# ZenBPM Profiling & Diagnostics Manual

When ZenBPM gets stuck or behaves unexpectedly, use these profiling endpoints to diagnose the issue.

## Prerequisites

The debug endpoints are **enabled by default**. To disable them in production, set:
```yaml
debug:
  enabled: false
```

Or via environment variable:
```bash
export DEBUG_PPROF_ENABLED=false
```

## Endpoints Overview

All profiling endpoints are available at: `http://localhost:8080/system/debug/pprof/`

| Endpoint | Description |
|----------|-------------|
| `/system/debug/pprof/` | Index page with links to all profiles |
| `/system/debug/pprof/goroutine` | **Goroutine dump** (thread dump equivalent) |
| `/system/debug/pprof/heap` | **Heap dump** (memory allocations) |
| `/system/debug/pprof/profile` | **CPU profile** (where CPU time is spent) |
| `/system/debug/pprof/block` | Blocking profile (where goroutines block) |
| `/system/debug/pprof/mutex` | Mutex contention profile |
| `/system/debug/pprof/trace` | Execution trace |
| `/system/debug/pprof/allocs` | All past memory allocations |
| `/system/debug/pprof/threadcreate` | OS thread creation stacks |
| `/system/debug/pprof/runtime` | Runtime stats in JSON format |

---

## When The Application Gets Stuck

### 1. Goroutine Dump (Thread Dump Equivalent)

This is the **most useful** when the app is stuck. Shows all goroutines and what they're waiting on.

**Quick view in browser:**
```
http://localhost:8080/system/debug/pprof/goroutine?debug=1
```

**Save to file:**
```bash
curl -o goroutine_dump.txt "http://localhost:8080/system/debug/pprof/goroutine?debug=2"
```

**Analyze with pprof tool:**
```bash
go tool pprof http://localhost:8080/system/debug/pprof/goroutine
```

**What to look for:**
- Goroutines stuck in `select`, `chan receive`, or `mutex.Lock`
- Large numbers of goroutines in the same state
- Deadlock patterns (goroutines waiting on each other)

---

### 2. Heap Dump (Memory Analysis)

Use when you suspect memory leaks or high memory usage.

**Save heap profile:**
```bash
curl -o heap.pprof http://localhost:8080/system/debug/pprof/heap
```

**Interactive analysis:**
```bash
go tool pprof http://localhost:8080/system/debug/pprof/heap
```

**Common pprof commands:**
```
(pprof) top 20          # Show top 20 memory allocators
(pprof) list funcName   # Show source code with allocations
(pprof) web             # Open interactive graph in browser
(pprof) png > heap.png  # Export as PNG image
```

**Quick text view:**
```bash
curl "http://localhost:8080/system/debug/pprof/heap?debug=1" | head -100
```

---

### 3. CPU Profile

Use when the app is slow or using high CPU.

**Capture 30-second CPU profile:**
```bash
curl -o cpu.pprof "http://localhost:8080/system/debug/pprof/profile?seconds=30"
```

**Interactive analysis:**
```bash
go tool pprof http://localhost:8080/system/debug/pprof/profile?seconds=30
```

**Web-based visualization:**
```bash
go tool pprof -http=:8081 cpu.pprof
```

---

### 4. Block Profile (Synchronization Blocking)

Use when goroutines seem to be blocked on channels/mutexes.

**Enable block profiling first** (set in config or restart with):
```yaml
debug:
  enabled: true
  blockProfileRate: 1   # 1 = track all blocking events
```

Or environment variable:
```bash
export DEBUG_BLOCK_PROFILE_RATE=1
```

**Capture profile:**
```bash
curl -o block.pprof http://localhost:8080/system/debug/pprof/block
go tool pprof block.pprof
```

---

### 5. Mutex Profile (Lock Contention)

Use when you suspect mutex contention is causing slowness.

**Enable mutex profiling first:**
```yaml
debug:
  enabled: true
  mutexProfileFraction: 1   # 1 = track all mutex events
```

Or environment variable:
```bash
export DEBUG_MUTEX_PROFILE_FRACTION=1
```

**Capture profile:**
```bash
curl -o mutex.pprof http://localhost:8080/system/debug/pprof/mutex
go tool pprof mutex.pprof
```

---

### 6. Execution Trace

Detailed trace of program execution. Useful for understanding goroutine scheduling.

**Capture 5-second trace:**
```bash
curl -o trace.out "http://localhost:8080/system/debug/pprof/trace?seconds=5"
```

**View trace:**
```bash
go tool trace trace.out
```

---

### 7. Runtime Statistics

Quick JSON overview of runtime state:

```bash
curl -s http://localhost:8080/system/debug/pprof/runtime | jq .
```

Returns:
- Number of goroutines
- Memory usage (heap, allocs, sys)
- GC statistics
- Available profiles

---

## Full Diagnostic Collection Script

When the app gets stuck, run this script to collect all diagnostics:

```bash
#!/bin/bash
# collect_diagnostics.sh

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
HOST="${1:-localhost:8080}"
OUTDIR="zenbpm_diag_$TIMESTAMP"

mkdir -p "$OUTDIR"
echo "Collecting diagnostics from $HOST into $OUTDIR/"

# Goroutine dump (most important for stuck processes)
echo "-> Collecting goroutine dump..."
curl -s "$HOST/system/debug/pprof/goroutine?debug=2" > "$OUTDIR/goroutine.txt"

# Heap profile
echo "-> Collecting heap profile..."
curl -s "$HOST/system/debug/pprof/heap" > "$OUTDIR/heap.pprof"
curl -s "$HOST/system/debug/pprof/heap?debug=1" > "$OUTDIR/heap.txt"

# Runtime stats
echo "-> Collecting runtime stats..."
curl -s "$HOST/system/debug/pprof/runtime" > "$OUTDIR/runtime.json"

# All allocations
echo "-> Collecting allocs profile..."
curl -s "$HOST/system/debug/pprof/allocs" > "$OUTDIR/allocs.pprof"

# Block profile (if enabled)
echo "-> Collecting block profile..."
curl -s "$HOST/system/debug/pprof/block" > "$OUTDIR/block.pprof"

# Mutex profile (if enabled)
echo "-> Collecting mutex profile..."
curl -s "$HOST/system/debug/pprof/mutex" > "$OUTDIR/mutex.pprof"

# Threadcreate
echo "-> Collecting threadcreate profile..."
curl -s "$HOST/system/debug/pprof/threadcreate" > "$OUTDIR/threadcreate.pprof"

# CPU profile (5 seconds)
echo "-> Collecting CPU profile (5 seconds)..."
curl -s "$HOST/system/debug/pprof/profile?seconds=5" > "$OUTDIR/cpu.pprof"

# Trace (2 seconds)
echo "-> Collecting trace (2 seconds)..."
curl -s "$HOST/system/debug/pprof/trace?seconds=2" > "$OUTDIR/trace.out"

echo ""
echo "Done! Diagnostics saved to $OUTDIR/"
echo ""
echo "To analyze:"
echo "  Goroutines: cat $OUTDIR/goroutine.txt"
echo "  Heap:       go tool pprof $OUTDIR/heap.pprof"
echo "  CPU:        go tool pprof $OUTDIR/cpu.pprof"
echo "  Trace:      go tool trace $OUTDIR/trace.out"
```

**Usage:**
```bash
chmod +x collect_diagnostics.sh
./collect_diagnostics.sh localhost:8080
```

---

## Interpreting Goroutine Dumps

When looking at goroutine dumps for a stuck process, look for:

### Deadlock Pattern
```
goroutine 1 [semacquire]:
sync.runtime_SemacquireMutex(...)
    ...

goroutine 2 [semacquire]:
sync.runtime_SemacquireMutex(...)
    ...
```
Both waiting on mutexes = potential deadlock.

### Channel Blocking
```
goroutine 42 [chan receive]:
main.worker(...)
    /path/to/file.go:123
```
Goroutine waiting to receive from a channel that may never send.

### Many Goroutines in Same State
```
goroutine 100 [select]:
goroutine 101 [select]:
goroutine 102 [select]:
... (hundreds more)
```
Could indicate goroutine leak or resource exhaustion.

---

## Security Considerations

The pprof endpoints expose internal application state. In production:

1. **Disable if not needed:**
   ```yaml
   debug:
     enabled: false
   ```

2. **Restrict access** via firewall/reverse proxy to internal networks only

3. **Enable only when debugging** - restart with `DEBUG_PPROF_ENABLED=true` when needed

---

## Quick Reference

| Symptom | First Profile to Check |
|---------|----------------------|
| App frozen/not responding | Goroutine dump (`/goroutine?debug=2`) |
| High CPU usage | CPU profile (`/profile?seconds=30`) |
| High memory usage | Heap profile (`/heap`) |
| Slow response times | CPU profile + Block profile |
| Suspected deadlock | Goroutine dump |
| Goroutine leak | Goroutine dump (look for counts) + Runtime stats |

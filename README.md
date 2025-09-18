# Scylla Cloud Resize + Load Tester

This script (test.py) stress-tests a ScyllaDB Cloud cluster while automatically resizing it back and forth between a min and max node count. It:

- Writes an infinite stream of rows using unlogged batches (size 10–50) across multiple writer threads.
- Reads random rows at a paced target rate (e.g., 1000 reads/sec) across multiple reader threads.
- Resizes the cluster from SCY_MIN_NODES → SCY_MAX_NODES (or vice-versa), waits until healthy, then requires a stable period (e.g., 5–10 minutes) before resizing back. This loop runs indefinitely.

Great for validating: scaling behavior, write/read throughput during resizes, and app-level stability under changing capacity.

---

## Prereqs

- Python 3.9+
- Packages:
```bash
pip install scylla-driver requests lz4
```
- A ScyllaDB Cloud cluster + Personal Access Token with permission to resize.

---

## Quick Start

1. Copy test.py and create a .env file (example below).
2. Load env & run:

```bash
# macOS/Linux
source .env
python test.py
```

3. Stop with Ctrl+C.

You’ll see:
- writers reporting rows/sec,
- readers reporting RPS,
- scaler printing cluster maps and resize progress (“Resize complete & healthy”, “Stable 300/300s …”).

.env Example
```bash
# --- CQL connectivity ---
export SCY_CONTACT_POINTS="node-0.<>,node-1.<>,node-2.<>..."
export SCY_LOCAL_DC="AWS_US_EAST_1"
export SCY_USERNAME="scylla"
export SCY_PASSWORD=""

# --- Schema ---
export SCY_KEYSPACE="demo_ks"
export SCY_TABLE="demo_table"

# --- Cloud API (required for resizing) ---
export SCY_CLOUD_API_BASE="https://api.cloud.scylladb.com"
export SCY_CLOUD_TOKEN="<PASTE_YOUR_TOKEN>"
export SCY_ACCOUNT_ID="<YOUR_ACCOUNT_ID>"
export SCY_CLUSTER_ID="<YOUR_CLUSTER_ID>"

# --- Resize oscillation ---
export SCY_MIN_NODES=3
export SCY_MAX_NODES=6
export SCY_STABLE_SECS=600               # require 10m stability before flipping back
export SCY_POLL_SECS=20                  # How often to check node count

# --- Load profile ---
export SCY_WRITE_WORKERS=4               # writer threads (increase for more write load)
export SCY_READ_WORKERS=4                # reader threads
export SCY_READ_RPS=1000                 # total reads/sec (shared across readers)
export SCY_BATCH_MIN=10                  # min rows per unlogged batch
export SCY_BATCH_MAX=50                  # max rows per unlogged batch
export SCY_BATCH_INFLIGHT=128            # total concurrent batches across ALL writers
export SCY_BATCHES_PER_CYCLE=256         # how many batches we submit per loop iteration
export SCY_BATCH_BIAS_HIGH=1             # skew batch sizes toward the high end for fewer requests
```

---

## What each setting does

### CQL / Schema

- `SCY_CONTACT_POINTS` — comma-separated list of node hostnames/IPs.
- `SCY_LOCAL_DC` — your cluster’s local DC name (e.g., AWS_US_EAST_1).
- `SCY_USERNAME`, `SCY_PASSWORD` — CQL auth credentials (if required).
- `SCY_KEYSPACE`, `SCY_TABLE` — where data is written/read. The script auto-creates them if missing.

### Cloud API (Resizing)

- `SCY_CLOUD_API_BASE` — Cloud API base URL (default is correct).
- `SCY_CLOUD_TOKEN` — required bearer token for Cloud API.
- `SCY_ACCOUNT_ID`, `SCY_CLUSTER_ID` — your account/cluster IDs (from the UI/API).

### Oscillation & Health Checking

- `SCY_MIN_NODES` — low watermark node count (e.g., 3).
- `SCY_MAX_NODES` — high watermark node count (e.g., 6).
- `SCY_POLL_SECS` — how often the scaler polls cluster state.
- `SCY_STABLE_SECS` — consecutive seconds the cluster must remain healthy at the target size before flipping to the other size.

### Load Generation

- `SCY_WRITE_WORKERS` — number of writer threads. More threads → more write throughput.
- `SCY_READ_WORKERS` — number of reader threads sampling random primary keys.
- `SCY_READ_RPS` — total read ops/sec across all reader threads (paced).
- `SCY_BATCH_MIN`, `SCY_BATCH_MAX` — unlogged batch size range (kept small to simulate real app batches).
- `SCY_BATCH_INFLIGHT` — global cap on concurrent batches (across all writers); prevents socket overload. Increase cautiously (e.g., 96 → 128 → 192).
- `SCY_BATCHES_PER_CYCLE` — how many new batches each writer tries to submit per loop before yielding.
- `SCY_BATCH_BIAS_HIGH` — if 1, batch sizes skew toward the high end of [MIN, MAX], reducing requests/second.

---

## How it works (high-level)

### Writers
Each writer thread builds unlogged batches of 10–50 rows, submits them asynchronously (execute_async), and keeps many batches in flight.
- A global semaphore caps total in-flight batches across all writers.
- On ConnectionBusy/NoHostAvailable, the batch is retried with backoff (AIMD).
- Inserted UUIDs are pushed into a shared pool for readers.

### Readers
Reader threads perform primary-key lookups at a paced RPS, randomly selecting UUIDs from the pool to simulate point reads.

### Scaler

- Determines current node count, requests resize to the opposite bound (MIN ↔ MAX).
- Polls until the cluster reports ready/healthy at the target size.
- Starts a stability timer; if health holds for SCY_STABLE_SECS, flips to the other size and repeats.
- Refuses to submit a resize if another request is already in progress.

Everything runs indefinitely; Ctrl+C sets a stop flag and drains gracefully.

---

### Tuning Tips

1. Install lz4 so driver compression kicks in.

2. If you get ConnectionBusy frequently:
- lower SCY_BATCH_INFLIGHT,
- raise SCY_BATCH_MIN/MAX (fewer, larger batches),
- or reduce SCY_WRITE_WORKERS.

3. For max write ceiling tests (not realistic for prod), try CONSISTENCY=ONE temporarily in the code; revert to LOCAL_QUORUM for realism.

---

### Troubleshooting

- Auth/API errors when resizing
Check SCY_CLOUD_TOKEN, SCY_ACCOUNT_ID, SCY_CLUSTER_ID, and permissions. The script prints API responses on error.

- Cluster never becomes “healthy”
Verify RF and node counts are compatible; ensure the cluster isn’t in another pending request; check Cloud UI for errors.

- Low throughput
Increase SCY_WRITE_WORKERS, SCY_BATCH_INFLIGHT, or move the client closer to the cluster. Ensure the client VM/box has enough CPU.

- Readers idle
They wait until writers populate the ID pool—expected early on. You can temporarily lower SCY_READ_RPS or raise writers.

---

One-liner run (bash)

```bash
set -a; source .env; set +a
python test.py
```
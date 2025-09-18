#!/usr/bin/env python3
# pip install scylla-driver requests
import os, sys, time, json, uuid, random, string, threading, signal
from datetime import datetime, timedelta
import time
from typing import List, Dict

import requests
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, BatchType, ConsistencyLevel
from collections import deque
import math
from cassandra import ReadTimeout, OperationTimedOut
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from threading import Semaphore
GLOBAL_BATCH_INFLIGHT = int(os.getenv("SCY_BATCH_INFLIGHT", "96"))
_global_sem = Semaphore(GLOBAL_BATCH_INFLIGHT)

# ===================== Config via ENV =====================
CONTACT_POINTS = os.getenv("SCY_CONTACT_POINTS",
    "node-0.aws-us-east-1.1a25b80a0bf522bb12c7.clusters.scylla.cloud,"
    "node-1.aws-us-east-1.1a25b80a0bf522bb12c7.clusters.scylla.cloud,"
    "node-2.aws-us-east-1.1a25b80a0bf522bb12c7.clusters.scylla.cloud"
).split(",")
CQL_PORT   = int(os.getenv("SCY_PORT", "9042"))
LOCAL_DC   = os.getenv("SCY_LOCAL_DC", "AWS_US_EAST_1")
USERNAME   = os.getenv("SCY_USERNAME", "scylla")
PASSWORD   = os.getenv("SCY_PASSWORD", "changeme")

KEYSPACE   = os.getenv("SCY_KEYSPACE", "demo_ks")
TABLE      = os.getenv("SCY_TABLE", "demo_table")

TOTAL_ROWS = int(os.getenv("SCY_TOTAL_ROWS", "1000000"))
CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM

API_BASE   = os.getenv("SCY_CLOUD_API_BASE", "https://api.cloud.scylladb.com")
ACCOUNT_ID = os.getenv("SCY_ACCOUNT_ID")
CLUSTER_ID = os.getenv("SCY_CLUSTER_ID")
CLOUD_TOKEN= os.getenv("SCY_CLOUD_TOKEN")
TARGET_NODES = int(os.getenv("SCY_TARGET_NODES", "6"))
POLL_SECS  = int(os.getenv("SCY_POLL_SECS", "20"))

WRITE_WORKERS      = int(os.getenv("SCY_WRITE_WORKERS", "8"))       # parallel loader threads
READ_WORKERS      = int(os.getenv("SCY_READ_WORKERS", "4"))       # parallel loader threads
READ_RPS      = int(os.getenv("SCY_READ_RPS", "1000"))       # parallel loader threads
BATCH_MIN  = int(os.getenv("SCY_BATCH_MIN", "10"))
BATCH_MAX  = int(os.getenv("SCY_BATCH_MAX", "50"))
BATCH_INFLIGHT  = int(os.getenv("SCY_BATCH_INFLIGHT", "128"))# concurrent batches per writer
BATCHES_PER_CYCLE = int(os.getenv("SCY_BATCHES_PER_CYCLE", "256"))  # build/submit per loop
BIAS_HIGH = bool(int(os.getenv("SCY_BATCH_BIAS_HIGH", "0")))


# Thread-safe pool of written UUIDs for readers to sample
_id_pool = deque(maxlen=2_000_000)  # plenty of headroom
_id_lock = threading.Lock()

stop_event = threading.Event()

# ===================== Cloud API helpers =====================
def _hdrs():
    if not CLOUD_TOKEN:
        print("ERROR: SCY_CLOUD_TOKEN not set", file=sys.stderr); sys.exit(2)
    return {
        "Authorization": f"Bearer {CLOUD_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "scylla-resize-load/1.0",
    }

def api_get(path: str):
    r = requests.get(f"{API_BASE}{path}", headers=_hdrs(), timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {path} -> {r.status_code} {r.text[:400]}")
    try:
        return r.json()
    except Exception:
        return r.text

def api_post(path: str, body: Dict):
    r = requests.post(f"{API_BASE}{path}", headers=_hdrs(), json=body, timeout=60)
    text = r.text[:2000]
    try:
        parsed = r.json()
    except Exception:
        parsed = None
    # surface any error quickly
    if r.status_code >= 400 or (isinstance(parsed, dict) and ("error" in parsed or "code" in parsed)):
        print(f"[api] POST {path} status={r.status_code}")
        print(f"[api] req: {json.dumps(body)}")
        print(f"[api] raw: {text}")
        if isinstance(parsed, dict):
            print(f"[api] parsed: {json.dumps(parsed, indent=2)[:2000]}")
        raise RuntimeError("Cloud API returned an error")
    return parsed if parsed is not None else text

def _normalize_nodes(resp) -> List[Dict]:
    """Accept shapes: {'data':{'nodes':[...]}}, {'nodes':[...]} or just [ ... ]."""
    if isinstance(resp, dict):
        env = resp.get("data", resp)
        nodes = env.get("nodes") if isinstance(env, dict) else None
        if isinstance(nodes, list): return nodes
    if isinstance(resp, list): return resp
    # try string → json
    if isinstance(resp, str):
        try: return _normalize_nodes(json.loads(resp))
        except Exception: pass
    print(f"[scaler] WARN: unexpected nodes payload: {str(resp)[:400]}")
    return []

def get_nodes() -> List[Dict]:
    return _normalize_nodes(api_get(f"/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}/nodes"))

def get_cluster() -> Dict:
    return api_get(f"/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}")

def list_requests(limit=5) -> List[Dict]:
    resp = api_get(f"/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}/request?limit={limit}")
    env = resp.get("data", resp) if isinstance(resp, dict) else resp
    items = (env.get("requests") or env.get("items") or env) if isinstance(env, dict) else env
    return items if isinstance(items, list) else []

def any_active_request() -> bool:
    for it in list_requests(limit=5):
        st = (it.get("status") or "").upper()
        if st in {"PENDING","RUNNING","IN_PROGRESS"}:
            print(f"[scaler] Active request: id={it.get('id')} type={it.get('type')} status={st}")
            return True
    return False

def discover_dc_and_instance():
    """Return (dc_id, current_size, instance_type_id, dc_name)."""
    info = get_cluster()
    env  = info.get("data", info) if isinstance(info, dict) else info
    dcs  = env.get("datacenters") or env.get("dcs") or []
    if dcs:
        dc = dcs[0]
        dc_id = dc.get("id") or dc.get("dcId")
        size  = dc.get("nodesCount") or dc.get("size") or dc.get("wantedSize") or 0
        itype = dc.get("instanceTypeId") or dc.get("instanceId")
        name  = dc.get("name") or str(dc_id)
    else:
        nodes = get_nodes()
        if not nodes: raise RuntimeError("No nodes available to infer dc/instance")
        dc_id = nodes[0].get("dcId")
        name  = str(dc_id)
        size  = sum(1 for n in nodes if n.get("dcId")==dc_id)
        itype = nodes[0].get("instanceTypeId") or nodes[0].get("instanceId")
    if not (dc_id and itype):
        raise RuntimeError(f"Missing dcId/instanceTypeId (dcId={dc_id}, instanceTypeId={itype})")
    return int(dc_id), int(size), int(itype), name

def request_resize_to(target_nodes: int):
    if any_active_request():
        raise RuntimeError("Refusing to resize: another cluster request is active")
    dc_id, cur, itype, name = discover_dc_and_instance()
    if cur == target_nodes:
        print(f"[scaler] DC {name} already at {cur} nodes; no-op.")
        return
    body = {"dcNodes":[{"dcId": dc_id, "wantedSize": target_nodes, "instanceTypeId": itype}]}
    print(f"[scaler] Resize DC={name}({dc_id}) {cur} -> {target_nodes} (instanceTypeId={itype})")
    api_post(f"/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}/resize", body)
    # echo newest request for visibility
    reqs = list_requests(limit=1)
    if reqs:
        it = reqs[0]
        print("[scaler] Submitted request:",
              json.dumps({k: it.get(k) for k in ("id","type","status","message","createdAt")}, indent=2))

def all_nodes_ready(nodes: List[Dict]) -> bool:
    for n in nodes:
        status = str(n.get("status") or "").upper()  # e.g., ACTIVE
        state  = str(n.get("state")  or "").upper()  # e.g., NORMAL
        if status not in {"ACTIVE","READY","RUNNING"}: return False
        if state  not in {"NORMAL","UP"}: return False
    return True

def print_cluster_map(nodes: List[Dict]):
    print("\n=== Cluster Map ===")
    for n in nodes:
        nid = n.get("id") or n.get("nodeId") or "?"
        addr = n.get("dns") or n.get("publicIp") or n.get("privateIp") or "?"
        dcid = n.get("dcId") or n.get("datacenterId") or n.get("dc") or "?"
        status = str(n.get("status") or "").upper()
        state  = str(n.get("state")  or "").upper()
        inst   = n.get("instanceType") or n.get("instance") or ""
        print(f"- Node {nid} | {addr} | DC={dcid} | {status}/{state} {('['+inst+']') if inst else ''}")
    print(f"Total nodes: {len(nodes)}\n")

# ===================== Loader (CQL) =====================
def rand_str(max_len: int) -> str:
    n = random.randint(0, max_len)
    alpha = string.ascii_letters + string.digits + "     "
    return "".join(random.choices(alpha, k=n))

def rand_name():
    firsts = ["Alex","Sam","Jordan","Taylor","Riley","Casey","Morgan","Avery","Quinn","Jamie",
              "Chris","Jesse","Dana","Robin","Kendall","Cameron","Lee","Drew","Shawn","Elliot"]
    lasts  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
              "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin"]
    return random.choice(firsts), random.choice(lasts)

def ensure_schema(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{ 'class': 'NetworkTopologyStrategy', '{LOCAL_DC}': 3 }}
        AND durable_writes = true;
    """)
    session.set_keyspace(KEYSPACE)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id uuid PRIMARY KEY,
            create_time timestamp,
            updated_time timestamp,
            fname text,
            lname text,
            data_object_json text,
            i1 int, i2 int, i3 int, i4 int, i5 int,
            s1 text, s2 text, s3 text, s4 text, s5 text
        ) WITH default_time_to_live = 0;
    """)

def connect_cql():
    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)),
        request_timeout=10.0,  # explicit
    )
    cluster = Cluster(
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=CONTACT_POINTS,
        port=CQL_PORT,
        auth_provider=PlainTextAuthProvider(username=USERNAME, password=PASSWORD),
        compression=True,  # uses lz4 if installed: pip install lz4
    )
    return cluster, cluster.connect()


def loader_thread():
    # spin up writer workers
    writers = []
    rows_per_worker = TOTAL_ROWS // WRITE_WORKERS
    remainder = TOTAL_ROWS % WRITE_WORKERS
    for wid in range(WRITE_WORKERS):
        quota = rows_per_worker + (1 if wid < remainder else 0)
        t = threading.Thread(target=_write_worker, args=(wid, quota), daemon=True)
        t.start()
        writers.append(t)

    # spin up reader workers
    readers = []
    for rid in range(READ_WORKERS):
        t = threading.Thread(target=_read_worker, args=(rid,), daemon=True)
        t.start()
        readers.append(t)

    # wait for writers to finish; then let readers run a tiny grace and stop
    for t in writers:
        t.join()
    # optional: let readers sample a bit longer while resize stabilizes
    time.sleep(2)
    stop_event.set()
    for t in readers:
        t.join()


    cluster, session = connect_cql()
    try:
        # only one worker needs to ensure schema; others can skip
        if worker_id == 0:
            ensure_schema(session)

        ps = session.prepare(f"""
            INSERT INTO {KEYSPACE}.{TABLE} (
                id, create_time, updated_time, fname, lname, data_object_json,
                i1, i2, i3, i4, i5, s1, s2, s3, s4, s5
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        ps.consistency_level = CONSISTENCY
        ps.is_idempotent = True

        written, start = 0, time.time()
        report_every = max(50_000 // max(1, WRITE_WORKERS), 20_000)

        while not stop_event.is_set() and written < quota:
            batch_size = min(random.randint(BATCH_MIN, BATCH_MAX), quota - written)
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            batch.consistency_level = CONSISTENCY

            now = datetime.utcnow()
            ids_this_batch = []

            for _ in range(batch_size):
                rid = uuid.uuid4()
                ct = now - timedelta(seconds=random.randint(0, 86_400))
                ut = ct + timedelta(seconds=random.randint(0, 86_400))
                fname, lname = rand_name()
                data_json = json.dumps({
                    "version": 1,
                    "flags": {"active": bool(random.getrandbits(1)), "beta": bool(random.getrandbits(1))},
                    "score": random.randint(0, 1_000_000),
                    "tags": [rand_str(12) for _ in range(random.randint(0, 5))]
                }, separators=(",", ":"))
                ivals = [random.randint(-10_000, 10_000) for _ in range(5)]
                svals = [rand_str(random.randint(0, 250)) for _ in range(5)]

                batch.add(ps, (rid, ct, ut, fname, lname, data_json,
                               ivals[0], ivals[1], ivals[2], ivals[3], ivals[4],
                               svals[0], svals[1], svals[2], svals[3], svals[4]))
                ids_this_batch.append(rid)

            session.execute(batch)
            written += batch_size

            # publish IDs for readers
            with _id_lock:
                _id_pool.extend(ids_this_batch)

            if written % report_every == 0 or written == quota:
                elapsed = time.time() - start
                rate = written / elapsed if elapsed > 0 else 0
                print(f"[writer-{worker_id}] {written:,}/{quota:,} rows | {rate:,.0f} rows/s")

        elapsed = time.time() - start
        print(f"[writer-{worker_id}] done: {written:,} rows in {elapsed:.1f}s ({written/elapsed:,.0f} rows/s)")
    except Exception as e:
        print(f"[writer-{worker_id}] ERROR: {e}", file=sys.stderr)
    finally:
        try: cluster.shutdown()
        except Exception: pass

def _choose_batch_size(remaining: int) -> int:
    lo, hi = BATCH_MIN, min(BATCH_MAX, remaining)
    if hi <= lo: 
        return hi
    if BIAS_HIGH:
        # skew toward larger sizes: pick from top half
        mid = (lo + hi) // 2
        return random.randint(mid, hi)
    return random.randint(lo, hi)

def _make_insert_ps(session):
    ps = session.prepare(f"""
        INSERT INTO {KEYSPACE}.{TABLE} (
            id, create_time, updated_time, fname, lname, data_object_json,
            i1, i2, i3, i4, i5, s1, s2, s3, s4, s5
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    ps.consistency_level = CONSISTENCY
    ps.is_idempotent = True  # rows are upserts; safe to retry
    return ps

def _row_tuple(now):
    rid = uuid.uuid4()
    ct  = now - timedelta(seconds=random.randint(0, 86_400))
    ut  = ct + timedelta(seconds=random.randint(0, 86_400))
    fname, lname = rand_name()
    data_json = json.dumps({
        "version": 1,
        "flags": {"active": bool(random.getrandbits(1)), "beta": bool(random.getrandbits(1))},
        "score": random.randint(0, 1_000_000),
        "tags": [rand_str(12) for _ in range(random.randint(0, 5))]
    }, separators=(",", ":"))
    ivals = [random.randint(-10_000, 10_000) for _ in range(5)]
    svals = [rand_str(random.randint(0, 250)) for _ in range(5)]
    return (rid, ct, ut, fname, lname, data_json,
            ivals[0], ivals[1], ivals[2], ivals[3], ivals[4],
            svals[0], svals[1], svals[2], svals[3], svals[4])

def _build_batch(ps, batch_size):
    b = BatchStatement(batch_type=BatchType.UNLOGGED)
    b.consistency_level = CONSISTENCY
    now = datetime.utcnow()
    ids = []
    for _ in range(batch_size):
        params = _row_tuple(now)
        ids.append(params[0])
        b.add(ps, params)
    return b, ids

def _write_worker(worker_id: int, quota: int):
    cluster, session = connect_cql()
    try:
        if worker_id == 0:
            ensure_schema(session)
        ps = _make_insert_ps(session)

        written = 0
        start = time.time()
        # exponential backoff state for busy bursts
        backoff = 0.01    # seconds
        backoff_max = 0.5

        def submit_batch(stmt, size, ids):
            """Acquire global permit, submit, and attach callbacks that recycle the permit."""
            _global_sem.acquire()
            fut = session.execute_async(stmt)

            def on_ok(_res):
                nonlocal written, backoff
                with _id_lock:
                    _id_pool.extend(ids)
                written += size
                backoff = max(0.01, backoff * 0.5)  # heal quickly
                _global_sem.release()

            def on_err(exc):
                nonlocal backoff
                # ConnectionBusy / NoHostAvailable → backoff and retry same batch
                msg = repr(exc)
                if "ConnectionBusy" in msg or "Unable to complete the operation against any hosts" in msg:
                    _global_sem.release()
                    time.sleep(backoff)
                    backoff = min(backoff * 2.0, backoff_max)  # AIMD: multiplicative decrease
                    submit_batch(stmt, size, ids)  # requeue same batch
                else:
                    # log & release permit; move on
                    print(f"[writer-{worker_id}] batch error: {exc}", file=sys.stderr)
                    _global_sem.release()

            fut.add_callbacks(on_ok, on_err)

        report_every = max(50_000 // max(1, WRITE_WORKERS), 20_000)

        while not stop_event.is_set() and written < quota:
            # produce a moderated burst of batches
            to_make = min(BATCHES_PER_CYCLE, quota - written)
            for _ in range(to_make):
                if written >= quota or stop_event.is_set():
                    break
                bsz = _choose_batch_size(quota - written)
                stmt, ids = _build_batch(ps, bsz)
                submit_batch(stmt, bsz, ids)

            # quick yield so callbacks run
            time.sleep(0.01)

            if written % report_every == 0 or written >= quota:
                elapsed = time.time() - start
                print(f"[writer-{worker_id}] {written:,}/{quota:,} rows | {written/max(1e-9,elapsed):,.0f} rows/s")

        # wait for all outstanding permits to come back
        while _global_sem._value < GLOBAL_BATCH_INFLIGHT:
            time.sleep(0.05)

        elapsed = time.time() - start
        print(f"[writer-{worker_id}] done: {written:,} rows in {elapsed:.1f}s "
              f"({written/max(1e-9,elapsed):,.0f} rows/s)")
    except Exception as e:
        print(f"[writer-{worker_id}] ERROR: {e}", file=sys.stderr)
    finally:
        try: cluster.shutdown()
        except Exception: pass

def _read_worker(reader_id: int):
    # per-reader target rps
    target = max(1.0, READ_RPS / max(1, READ_WORKERS))
    interval = 1.0 / target
    next_deadline = time.perf_counter()

    cluster, session = connect_cql()
    try:
        ps = session.prepare(f"SELECT id, create_time, updated_time FROM {KEYSPACE}.{TABLE} WHERE id = ?")
        ps.consistency_level = ConsistencyLevel.LOCAL_QUORUM

        ops = 0
        start = time.perf_counter()

        while not stop_event.is_set():
            # pace
            now = time.perf_counter()
            if now < next_deadline:
                time.sleep(next_deadline - now)
            next_deadline += interval

            # pick a random id; if pool empty, skip this tick
            with _id_lock:
                n = len(_id_pool)
                rid = _id_pool[random.randrange(n)] if n else None

            if not rid:
                continue

            try:
                session.execute(ps, (rid,))
            except (ReadTimeout, OperationTimedOut) as e:
                # light-touch logging to avoid spam
                if ops % 1000 == 0:
                    print(f"[reader-{reader_id}] timeout: {e}", file=sys.stderr)
            except Exception as e:
                if ops % 1000 == 0:
                    print(f"[reader-{reader_id}] error: {e}", file=sys.stderr)

            ops += 1
            # occasional progress
            if ops % 5000 == 0:
                elapsed = time.perf_counter() - start
                print(f"[reader-{reader_id}] {ops:,} reads | {ops/elapsed:,.0f} rps")

    finally:
        try: cluster.shutdown()
        except Exception: pass


# ===================== Scaler thread =====================
def scaler_thread():
    try:
        nodes = get_nodes()
        print_cluster_map(nodes)
        if len(nodes) < TARGET_NODES:
            print(f"[scaler] Requesting resize {len(nodes)} -> {TARGET_NODES}…")
            request_resize_to(TARGET_NODES)
        else:
            print(f"[scaler] Already at {len(nodes)} nodes; no resize.")

        # poll for readiness
        while not stop_event.is_set():
            time.sleep(POLL_SECS)
            nodes = get_nodes()
            ready = all_nodes_ready(nodes)
            print(f"[scaler] Poll: nodes={len(nodes)} ready={ready}")
            if len(nodes) == TARGET_NODES and ready:
                print_cluster_map(nodes)
                print("[scaler] Resize complete & healthy.")
                break
    except Exception as e:
        print(f"[scaler] ERROR: {e}", file=sys.stderr)

# ===================== Main =====================
def main():
    if not ACCOUNT_ID or not CLUSTER_ID:
        print("ERROR: SCY_ACCOUNT_ID and SCY_CLUSTER_ID must be set.", file=sys.stderr)
        sys.exit(2)

    def _sigint(sig, frame):
        print("\n[main] Stopping…"); stop_event.set()
    signal.signal(signal.SIGINT, _sigint)

    t_load  = threading.Thread(target=loader_thread,  name="loader", daemon=True)
    t_scale = threading.Thread(target=scaler_thread, name="scaler", daemon=True)
    t_load.start(); t_scale.start()
    t_load.join();  t_scale.join(timeout=0)
    print("[main] Done.")

if __name__ == "__main__":
    main()

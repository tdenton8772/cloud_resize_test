#!/usr/bin/env python3
# pip install scylla-driver requests
import os, sys, time, json, uuid, random, string, threading, signal
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import requests
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, BatchType, ConsistencyLevel

# ------------- ENV / CONFIG -------------
# CQL
CONTACT_POINTS = os.getenv("SCY_CONTACT_POINTS", "node-0.aws-us-east-1.1a25b80a0bf522bb12c7.clusters.scylla.cloud,node-1.aws-us-east-1.1a25b80a0bf522bb12c7.clusters.scylla.cloud,node-2.aws-us-east-1.1a25b80a0bf522bb12c7.clusters.scylla.cloud").split(",")
CQL_PORT = int(os.getenv("SCY_PORT", "9042"))
LOCAL_DC = os.getenv("SCY_LOCAL_DC", "AWS_US_EAST_1")
USERNAME = os.getenv("SCY_USERNAME", "scylla")
PASSWORD = os.getenv("SCY_PASSWORD", "changeme")  # set env var!

KEYSPACE = os.getenv("SCY_KEYSPACE", "demo_ks")
TABLE = os.getenv("SCY_TABLE", "demo_table")

TOTAL_ROWS = int(os.getenv("SCY_TOTAL_ROWS", "1000000"))
BATCH_MIN = int(os.getenv("SCY_BATCH_MIN", "10"))
BATCH_MAX = int(os.getenv("SCY_BATCH_MAX", "50"))
CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM

# Scylla Cloud API
API_BASE = os.getenv("SCY_CLOUD_API_BASE", "https://api.cloud.scylladb.com")
ACCOUNT_ID = os.getenv("SCY_ACCOUNT_ID")      # e.g. "12345"
CLUSTER_ID = os.getenv("SCY_CLUSTER_ID")      # e.g. "67890"
CLOUD_TOKEN = os.getenv("SCY_CLOUD_TOKEN")    # Personal Access Token (Bearer)
TARGET_NODE_COUNT = int(os.getenv("SCY_TARGET_NODES", "6"))
POLL_SECS = int(os.getenv("SCY_POLL_SECS", "20"))

# ------------- GLOBALS -------------
stop_event = threading.Event()

# ------------- HELPERS -------------
def api_headers():
    if not CLOUD_TOKEN:
        print("ERROR: SCY_CLOUD_TOKEN not set", file=sys.stderr)
        sys.exit(2)
    return {
        "Authorization": f"Bearer {CLOUD_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "scylla-resize-load/1.0",
    }

def api_get(path: str) -> Any:
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers=api_headers(), timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {url} -> {r.status_code}: {r.text}")
    return r.json()

def api_post(path: str, body: Dict[str, Any]) -> Any:
    url = f"{API_BASE}{path}"
    r = requests.post(url, headers=api_headers(), json=body, timeout=60)
    # Always print diagnostics on non-2xx or when body signals error
    try:
        parsed = r.json()
    except Exception:
        parsed = None

    if r.status_code >= 400 or (isinstance(parsed, dict) and ("error" in parsed or "code" in parsed)):
        print(f"[api] POST {url} status={r.status_code}")
        print(f"[api] req: {json.dumps(body)}")
        print(f"[api] raw: {r.text[:2000]}")
        if isinstance(parsed, dict):
            print(f"[api] parsed: {json.dumps(parsed, indent=2)[:2000]}")
        raise RuntimeError("Resize request failed")

    return parsed if parsed is not None else r.text


def print_cluster_map(nodes: List[Dict[str, Any]]):
    print("\n=== Cluster Map (from Cloud API) ===")
    for n in nodes:
        nid   = n.get("id") or n.get("nodeId") or n.get("name") or "?"
        addr  = (
            n.get("address") or n.get("ip") or n.get("privateIp") or n.get("publicIp")
            or n.get("publicAddress") or n.get("privateAddress") or "?"
        )
        dc    = n.get("dc") or n.get("datacenter") or n.get("dataCenter") or "?"
        rack  = n.get("rack") or n.get("rackName") or "?"
        state = str(n.get("status") or n.get("state") or n.get("lifecycleState") or "?")
        inst  = n.get("instanceType") or n.get("instance") or n.get("flavor") or ""
        print(f"- Node {nid} | {addr} | DC={dc} Rack={rack} | {state} {('['+inst+']') if inst else ''}")
    print(f"Total nodes: {len(nodes)}")
    print("====================================\n")

def get_nodes() -> List[Dict[str, Any]]:
    path = f"/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}/nodes"
    resp = api_get(path)
    nodes = _normalize_nodes_payload(resp)
    if not nodes:
        # one-time diagnostic: show a compact preview to help troubleshoot
        prev = resp if isinstance(resp, (dict, list)) else repr(resp)
        print(f"[scaler] WARN: Could not parse nodes list. Raw preview: {str(prev)[:400]}")
    return nodes

def get_cluster() -> Dict[str, Any]:
    path = f"/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}"
    return api_get(path)

def request_resize_to(target_nodes: int):
    info = get_cluster()
    env = info.get("data", info)
    # Try to find DCs from cluster; if absent, derive dcId from the nodes list
    dcs = (env.get("datacenters") or env.get("dcs") or [])
    if not dcs:
        nodes = get_nodes()
        # fall back to dcId from first node
        dc_id = nodes[0].get("dcId")
        if not dc_id:
            raise RuntimeError("Cannot determine dcId for resize body")
        resize_body = {"dcNodes": [{"dcId": dc_id, "nodeCount": target_nodes}]}
    else:
        first_dc = dcs[0]
        dc_id = first_dc.get("id") or first_dc.get("dcId") or first_dc.get("name")
        resize_body = {"dcNodes": [{"dcId": dc_id, "nodeCount": target_nodes}]}
    print(f"Requesting resize to {target_nodes} nodes with body: {json.dumps(resize_body)}")
    path = f"/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}/resize"
    resp = api_post(path, resize_body)

    print(f"Resize request accepted: {resp}")
    list_cluster_requests()

def list_cluster_requests(limit=5, req_type="RESIZE"):
    # Endpoints on your tenant expose "Cluster Request" listing under account scope
    # and commonly accept filters like clusterId/type.
    path = f"/account/{ACCOUNT_ID}/requests?clusterId={CLUSTER_ID}&type={req_type}&limit={limit}"
    resp = api_get(path)
    env = resp.get("data", resp)
    items = env.get("items") or env.get("requests") or env
    print("\n[requests] recent cluster requests:")
    try:
        for it in items[:limit]:
            rid   = it.get("id")
            rtype = it.get("type")
            st    = it.get("status")
            code  = it.get("errorCode") or it.get("code")
            msg   = it.get("errorMessage") or it.get("message")
            print(f"- id={rid} type={rtype} status={st} code={code} msg={msg}")
    except Exception:
        print(str(env)[:1000])

def all_nodes_ready(nodes: List[Dict[str, Any]]) -> bool:
    good = {"READY", "UP", "RUNNING", "HEALTHY", "ACTIVE", "READY_TO_USE", "OK"}
    for n in nodes:
        st = str(n.get("status") or n.get("state") or n.get("lifecycleState") or "").upper()
        if not st or all(g not in st for g in good):
            return False
    return True

def _normalize_nodes_payload(resp):
    # Handle enveloped responses like {'data': {'nodes': [...]}}
    if isinstance(resp, dict):
        env = resp.get("data", resp)
        # common shapes
        if isinstance(env, dict):
            if isinstance(env.get("nodes"), list):
                return env["nodes"]
            if isinstance(env.get("items"), list):
                return env["items"]
        if isinstance(env, list):
            return env
        # last resort: single-node dict?
        if {"id","status","state"} <= set(env.keys()):
            return [env]
        print(f"[scaler] Unexpected node payload dict keys: {list(resp.keys())[:10]}")
        return []

    if isinstance(resp, list):
        return resp if (not resp or isinstance(resp[0], dict)) else []

    if isinstance(resp, str):
        try:
            return _normalize_nodes_payload(json.loads(resp))
        except Exception:
            print(f"[scaler] Unexpected string payload (first 200): {resp[:200]!r}")
            return []

    print(f"[scaler] Unexpected payload type: {type(resp)}")
    return []

# ------------- LOADER (CQL) -------------
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
    ks_cql = f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{ 'class': 'NetworkTopologyStrategy', '{LOCAL_DC}': 3 }}
    AND durable_writes = true;
    """
    session.execute(ks_cql)
    session.set_keyspace(KEYSPACE)
    tbl_cql = f"""
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
    """
    session.execute(tbl_cql)

def get_cluster_session():
    profile = ExecutionProfile(load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)))
    cluster = Cluster(
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=CONTACT_POINTS,
        port=CQL_PORT,
        auth_provider=PlainTextAuthProvider(username=USERNAME, password=PASSWORD),
    )
    return cluster, cluster.connect()

def loader_thread():
    try:
        print("[loader] Connecting to cluster…")
        cluster, session = get_cluster_session()
        print(f"[loader] Connected: {cluster.metadata.cluster_name}")
        for h in cluster.metadata.all_hosts():
            print(f"[loader] Host: {h.address} | DC={h.datacenter} Rack={h.rack}")

        ensure_schema(session)

        ps = session.prepare(f"""
            INSERT INTO {KEYSPACE}.{TABLE} (
                id, create_time, updated_time, fname, lname, data_object_json,
                i1, i2, i3, i4, i5,
                s1, s2, s3, s4, s5
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        ps.consistency_level = CONSISTENCY

        written = 0
        start = time.time()
        report_every = 50_000

        while not stop_event.is_set() and written < TOTAL_ROWS:
            size = random.randint(BATCH_MIN, BATCH_MAX)
            if written + size > TOTAL_ROWS:
                size = TOTAL_ROWS - written

            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            batch.consistency_level = CONSISTENCY
            now = datetime.utcnow()

            for _ in range(size):
                rec_id = uuid.uuid4()
                create_ts = now - timedelta(seconds=random.randint(0, 86_400))
                updated_ts = create_ts + timedelta(seconds=random.randint(0, 86_400))
                fname, lname = rand_name()

                data_obj = {
                    "version": 1,
                    "flags": {"active": bool(random.getrandbits(1)), "beta": bool(random.getrandbits(1))},
                    "score": random.randint(0, 1_000_000),
                    "tags": [rand_str(12) for _ in range(random.randint(0, 5))],
                }
                data_json = json.dumps(data_obj, separators=(",", ":"))

                ivals = [random.randint(-10_000, 10_000) for _ in range(5)]
                svals = [rand_str(random.randint(0, 250)) for _ in range(5)]

                batch.add(ps, (rec_id, create_ts, updated_ts, fname, lname, data_json,
                               ivals[0], ivals[1], ivals[2], ivals[3], ivals[4],
                               svals[0], svals[1], svals[2], svals[3], svals[4]))
            session.execute(batch)
            written += size

            if written % report_every == 0 or written == TOTAL_ROWS:
                elapsed = time.time() - start
                rate = written / elapsed if elapsed > 0 else 0
                print(f"[loader] {written:,}/{TOTAL_ROWS:,} rows  | {rate:,.0f} rows/sec")

        elapsed = time.time() - start
        print(f"[loader] Done: {written:,} rows in {elapsed:.1f}s  ({(written/elapsed):,.0f} rows/sec)")
    except Exception as e:
        print(f"[loader] ERROR: {e}", file=sys.stderr)
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

# ------------- SCALER (Cloud API) -------------
def scaler_thread():
    try:
        # Initial map
        nodes = get_nodes()
        print_cluster_map(nodes)

        current = len(nodes)
        if current >= TARGET_NODE_COUNT:
            print(f"[scaler] Cluster already at {current} nodes. No resize requested.")
        else:
            print(f"[scaler] Requesting resize from {current} -> {TARGET_NODE_COUNT} nodes…")
            request_resize_to(TARGET_NODE_COUNT)

        # Poll until node count == target and nodes are ready
        while not stop_event.is_set():
            time.sleep(POLL_SECS)
            try:
                nodes = get_nodes()
            except Exception as e:
                print(f"[scaler] Poll error: {e}")
                continue

            cnt = len(nodes)
            ready = all_nodes_ready(nodes)
            print(f"[scaler] Poll: nodes={cnt} ready={ready}")
            if cnt == TARGET_NODE_COUNT and ready:
                print_cluster_map(nodes)
                print("[scaler] Resize complete & nodes healthy.")
                break

    except Exception as e:
        print(f"[scaler] ERROR: {e}", file=sys.stderr)

# ------------- MAIN -------------
def main():
    if not ACCOUNT_ID or not CLUSTER_ID:
        print("ERROR: SCY_ACCOUNT_ID and SCY_CLUSTER_ID must be set.", file=sys.stderr)
        sys.exit(2)

    def handle_sigint(sig, frame):
        print("\n[main] Caught signal, stopping…")
        stop_event.set()
    signal.signal(signal.SIGINT, handle_sigint)

    t_load = threading.Thread(target=loader_thread, name="loader", daemon=True)
    t_scale = threading.Thread(target=scaler_thread, name="scaler", daemon=True)

    t_load.start()
    t_scale.start()

    # Wait for loader to finish writing all rows; scaler will stop after success or on Ctrl+C.
    t_load.join()
    # Give scaler time to complete if still running
    t_scale.join(timeout=0)

    print("[main] Done.")

if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import os
from typing import Any, Dict

from src.app.raft.log import RaftLog
from src.app.raft.node import RaftNode

METADATA_FILE = "metadata.json"
LOG_FILE = "log.json"
KV_FILE = "kv.json"
SNAPSHOT_FILE = "snapshot.json"


def ensure_node_data_dir(base_dir: str, node_id: str) -> str:
    """Проверка и создание каталога для узла."""
    node_dir = os.path.join(base_dir, node_id)
    os.makedirs(node_dir, exist_ok=True)
    return node_dir


def _read_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_json(path: str, data: Any) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def load_node_state(node: RaftNode, node_dir: str) -> None:
    """Загружает состояние узла из файлов, если они есть."""
    snap_path = os.path.join(node_dir, SNAPSHOT_FILE)
    snap = _read_json(snap_path)
    base_index = 0
    base_term = 0
    if isinstance(snap, dict):
        base_index = int(snap.get("last_included_index", 0) or 0)
        base_term = int(snap.get("last_included_term", 0) or 0)
        snap_state = snap.get("state")
        if isinstance(snap_state, dict):
            node.snapshot_state = dict(snap_state)
            node.state_machine.load_state(snap_state)

    meta_path = os.path.join(node_dir, METADATA_FILE)
    meta = _read_json(meta_path) or {}

    node.current_term = int(meta.get("current_term", 0) or 0)
    node.voted_for = meta.get("voted_for", None)
    node.commit_index = int(meta.get("commit_index", 0) or 0)

    node.last_applied = base_index

    peer_addrs = meta.get("peer_addresses")
    if isinstance(peer_addrs, dict):
        for k, v in peer_addrs.items():
            if k and k != node.node_id and isinstance(v, str) and v:
                node.peer_addresses[str(k)] = v

    log_path = os.path.join(node_dir, LOG_FILE)
    log_data = _read_json(log_path)
    if isinstance(log_data, list):
        node.log = RaftLog.from_serializable(log_data, base_index=base_index, base_term=base_term)
    else:
        node.log.base_index = base_index
        node.log.base_term = base_term

    kv_path = os.path.join(node_dir, KV_FILE)
    kv_data = _read_json(kv_path)
    if isinstance(kv_data, dict):
        node.state_machine.load_state(kv_data)

    if node.commit_index < node.log.base_index:
        node.commit_index = node.log.base_index
    if node.last_applied < node.log.base_index:
        node.last_applied = node.log.base_index

    if node.commit_index > node.log.last_index():
        node.commit_index = node.log.last_index()
    if node.last_applied > node.commit_index:
        node.last_applied = node.commit_index

def save_metadata(node: RaftNode, node_dir: str) -> None:
    meta_path = os.path.join(node_dir, METADATA_FILE)

    data: Dict[str, Any] = {
        "current_term": node.current_term,
        "voted_for": node.voted_for,
        "commit_index": node.commit_index,
        "members": sorted([node.node_id] + list(node.peers)),
        "peer_addresses": dict(getattr(node, "peer_addresses", {}) or {}),
    }

    _write_json(meta_path, data)


def save_log(node: RaftNode, node_dir: str) -> None:
    log_path = os.path.join(node_dir, LOG_FILE)
    data = node.log.to_serializable()
    _write_json(log_path, data)


def save_kv_state(node: RaftNode, node_dir: str) -> None:
    kv_path = os.path.join(node_dir, KV_FILE)
    data = node.state_machine.export_state()
    _write_json(kv_path, data)


def save_full_state(node: RaftNode, node_dir: str) -> None:
    """Сохраняет metadata +  + log + kv."""
    try:
        threshold = int(os.getenv("SNAPSHOT_THRESHOLD", "50"))
    except Exception:
        threshold = 50

    created = node.maybe_create_snapshot(threshold=threshold)
    if created:
        save_snapshot(node, node_dir)

    save_metadata(node, node_dir)
    save_log(node, node_dir)
    save_kv_state(node, node_dir)

    if node.log.base_index > 0:
        save_snapshot(node, node_dir)


def save_snapshot(node: RaftNode, node_dir: str) -> None:
    snap_path = os.path.join(node_dir, SNAPSHOT_FILE)

    state = node.snapshot_state
    if state is None and node.log.base_index > 0:
        state = node.state_machine.export_state()

    if node.log.base_index <= 0 and not state:
        return

    data = {
        "last_included_index": node.log.base_index,
        "last_included_term": node.log.base_term,
        "state": state or {},
    }
    _write_json(snap_path, data)
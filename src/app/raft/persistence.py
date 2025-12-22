# src/app/raft/persistence.py
#
# Простейшая персистентность состояния RAFT-узла:
#   - metadata.json: current_term, voted_for
#   - log.json: журнал RAFT
#   - kv.json: состояние key-value
#
# Каталог хранится как: <base_data_dir>/<node_id>/
# Например: ./data/node1/metadata.json

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
    """
    Создаёт (если нужно) каталог для конкретного узла.
    Возвращает путь к нему.
    """
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
    # гарантируем, что директория существует
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    tmp_path = f"{path}.tmp"
    # tmp лежит в той же директории — она уже создана выше
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


# ==== ЗАГРУЗКА ====

def load_node_state(node: RaftNode, node_dir: str) -> None:
    """
    Загружает состояние узла из файлов, если они есть:
      - snapshot.json (если есть): base_index/base_term + snapshot_state + state machine
      - metadata.json
      - log.json (только хвост после base_index)
      - kv.json (если есть — считается "истиной", совместимо с прошлым форматом)
    """
    # 0) snapshot (важно грузить первым, чтобы log.from_serializable получил base_index/base_term)
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

    # 1) metadata
    meta_path = os.path.join(node_dir, METADATA_FILE)
    meta = _read_json(meta_path) or {}

    node.current_term = int(meta.get("current_term", 0) or 0)
    node.voted_for = meta.get("voted_for", None)
    node.commit_index = int(meta.get("commit_index", 0) or 0)
    node.last_applied = int(meta.get("last_applied", 0) or 0)

    # 1b) dynamic membership (backward compatible)
    members = meta.get("members")
    if isinstance(members, list) and members:
        node.members = set(map(str, members))
        node.peers = sorted(list(node.members - {node.node_id}))

    peer_addrs = meta.get("peer_addresses")
    if isinstance(peer_addrs, dict):
        # merge into existing (init from env/config)
        for k, v in peer_addrs.items():
            if k and k != node.node_id and isinstance(v, str) and v:
                node.peer_addresses[str(k)] = v

    joint_old = meta.get("joint_old")
    joint_new = meta.get("joint_new")
    if (
        isinstance(joint_old, list)
        and isinstance(joint_new, list)
        and joint_old
        and joint_new
    ):
        node.joint_old = set(map(str, joint_old))
        node.joint_new = set(map(str, joint_new))
        node.members = set(node.joint_old) | set(node.joint_new)
        node.peers = sorted(list(node.members - {node.node_id}))

    # 2) log
    log_path = os.path.join(node_dir, LOG_FILE)
    log_data = _read_json(log_path)
    if isinstance(log_data, list):
        node.log = RaftLog.from_serializable(log_data, base_index=base_index, base_term=base_term)
    else:
        # даже если лог пуст — base_index/base_term должны сохраниться
        node.log.base_index = base_index
        node.log.base_term = base_term

    # 3) kv (совместимость: если kv.json есть, он перезатирает snapshot state)
    kv_path = os.path.join(node_dir, KV_FILE)
    kv_data = _read_json(kv_path)
    if isinstance(kv_data, dict):
        node.state_machine.load_state(kv_data)

    # Санитизация индексов относительно лога/снапшота
    if node.commit_index < node.log.base_index:
        node.commit_index = node.log.base_index
    if node.last_applied < node.log.base_index:
        node.last_applied = node.log.base_index

    if node.commit_index > node.log.last_index():
        node.commit_index = node.log.last_index()
    if node.last_applied > node.commit_index:
        node.last_applied = node.commit_index


# ==== СОХРАНЕНИЕ ====

def save_metadata(node: RaftNode, node_dir: str) -> None:
    meta_path = os.path.join(node_dir, METADATA_FILE)

    data: Dict[str, Any] = {
        "current_term": node.current_term,
        "voted_for": node.voted_for,
        # (2) Persist volatile индексы, чтобы пережить рестарт без повторного apply.
        "commit_index": node.commit_index,
        "last_applied": node.last_applied,

        # (4) dynamic membership
        "members": sorted(list(node.members)) if getattr(node, "members", None) else sorted([node.node_id] + list(node.peers)),
        "peer_addresses": dict(getattr(node, "peer_addresses", {}) or {}),
        "joint_old": sorted(list(node.joint_old)) if getattr(node, "joint_old", None) else None,
        "joint_new": sorted(list(node.joint_new)) if getattr(node, "joint_new", None) else None,
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
    """
    Сохраняет metadata + (возможно snapshot+compaction) + log + kv.
    """
    # 1) возможно создаём snapshot и компактим лог
    try:
        threshold = int(os.getenv("SNAPSHOT_THRESHOLD", "50"))
    except Exception:
        threshold = 50

    created = node.maybe_create_snapshot(threshold=threshold)
    if created:
        save_snapshot(node, node_dir)

    # 2) обычная персистентность
    save_metadata(node, node_dir)
    save_log(node, node_dir)
    save_kv_state(node, node_dir)

    # 3) если snapshot уже есть (base_index > 0), поддерживаем snapshot.json актуальным
    if node.log.base_index > 0:
        save_snapshot(node, node_dir)


def save_snapshot(node: RaftNode, node_dir: str) -> None:
    snap_path = os.path.join(node_dir, SNAPSHOT_FILE)

    # snapshot_state обязан соответствовать log.base_index
    state = node.snapshot_state
    if state is None and node.log.base_index > 0:
        # fallback: если почему-то нет snapshot_state, но base_index уже есть,
        # сохраняем текущее состояние (лучше, чем ничего)
        state = node.state_machine.export_state()

    if node.log.base_index <= 0 and not state:
        return

    data = {
        "last_included_index": node.log.base_index,
        "last_included_term": node.log.base_term,
        "state": state or {},
    }
    _write_json(snap_path, data)
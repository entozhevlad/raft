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
from typing import Any, Dict, Optional, Tuple

from src.app.raft.log import RaftLog
from src.app.raft.node import RaftNode


METADATA_FILE = "metadata.json"
LOG_FILE = "log.json"
KV_FILE = "kv.json"


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
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


# ==== ЗАГРУЗКА ====

def load_node_state(node: RaftNode, node_dir: str) -> None:
    """
    Загружает состояние узла (term, voted_for, log, kv) из файлов, если они есть.
    Если файлов нет — узел стартует "с нуля".
    """
    # metadata
    meta_path = os.path.join(node_dir, METADATA_FILE)
    meta = _read_json(meta_path) or {}
    term = int(meta.get("current_term", 0) or 0)
    voted_for = meta.get("voted_for", None)

    if term > 0:
        node.current_term = term
        node.voted_for = voted_for

    # log
    log_path = os.path.join(node_dir, LOG_FILE)
    log_data = _read_json(log_path)
    if isinstance(log_data, list):
        node.log = RaftLog.from_serializable(log_data)

    # kv
    kv_path = os.path.join(node_dir, KV_FILE)
    kv_data = _read_json(kv_path)
    if isinstance(kv_data, dict):
        node.state_machine.load_state(kv_data)


# ==== СОХРАНЕНИЕ ====

def save_metadata(node: RaftNode, node_dir: str) -> None:
    meta_path = os.path.join(node_dir, METADATA_FILE)
    data = {
        "current_term": node.current_term,
        "voted_for": node.voted_for,
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
    Удобный метод: сохраняет metadata + log + kv.
    Можно вызывать после каждого коммита.
    """
    save_metadata(node, node_dir)
    save_log(node, node_dir)
    save_kv_state(node, node_dir)

# src/app/config.py
#
# Конфигурация узла.
# Читает NODE_ID, PEERS и DATA_DIR из переменных окружения.
#
# Примеры:
#   NODE_ID=node1
#   PEERS=node2:http://node2:8000,node3:http://node3:8000
#   DATA_DIR=/data   (для Docker) или ./data (по умолчанию)

from __future__ import annotations

import os
from typing import Dict, List

from pydantic import BaseModel


class NodeConfig(BaseModel):
    node_id: str
    peers: List[str]
    peer_addresses: Dict[str, str]
    data_dir: str


def load_node_config() -> NodeConfig:
    node_id = os.getenv("NODE_ID", "node1")
    peers_raw = os.getenv("PEERS", "")
    data_dir = os.getenv("DATA_DIR", "./data")

    peers: List[str] = []
    peer_addresses: Dict[str, str] = {}

    if peers_raw:
        for item in peers_raw.split(","):
            item = item.strip()
            if not item:
                continue

            if ":" in item:
                peer_id, addr = item.split(":", 1)
                peer_id = peer_id.strip()
                addr = addr.strip()
            else:
                peer_id = item
                addr = f"http://{peer_id}:8000"

            if not peer_id or peer_id == node_id:
                continue

            peers.append(peer_id)
            peer_addresses[peer_id] = addr

    return NodeConfig(
        node_id=node_id,
        peers=peers,
        peer_addresses=peer_addresses,
        data_dir=data_dir,
    )

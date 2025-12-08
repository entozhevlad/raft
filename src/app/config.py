# app/config.py
#
# Конфигурация узла.
# Читает NODE_ID и PEERS из переменных окружения.
#
# Формат PEERS:
#   PEERS="node2:http://localhost:8001,node3:http://localhost:8002"
#
# В docker-compose мы позже подставим реальные адреса контейнеров.

from __future__ import annotations

import os
from typing import Dict, List

from pydantic import BaseModel


class NodeConfig(BaseModel):
    node_id: str
    peers: List[str]                 # список id пиров
    peer_addresses: Dict[str, str]   # id -> base_url (например "http://node2:8000")


def load_node_config() -> NodeConfig:
    node_id = os.getenv("NODE_ID", "node1")
    peers_raw = os.getenv("PEERS", "")

    peers: List[str] = []
    peer_addresses: Dict[str, str] = {}

    # Пример строки: "node2:http://node2:8000,node3:http://node3:8000"
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
                # Если URL не указан, попробуем собрать по умолчанию
                peer_id = item
                addr = f"http://{peer_id}:8000"

            if not peer_id or peer_id == node_id:
                # Не добавляем самих себя в список пиров
                continue

            peers.append(peer_id)
            peer_addresses[peer_id] = addr

    return NodeConfig(node_id=node_id, peers=peers, peer_addresses=peer_addresses)

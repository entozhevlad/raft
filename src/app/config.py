# app/config.py
#
# Конфигурация узла.
# Сейчас читаем NODE_ID и PEERS из переменных окружения.
# В docker-compose будем как раз их задавать.

from __future__ import annotations

import os
from typing import List
from pydantic import BaseModel


class NodeConfig(BaseModel):
    node_id: str
    peers: List[str]


def load_node_config() -> NodeConfig:
    """
    Загружает конфиг узла из переменных окружения.
      NODE_ID  - обязательный (по-хорошему), но дадим дефолт: "node1"
      PEERS    - список id других узлов через запятую, например: "node2,node3"
                 пустая строка или отсутствие переменной => нет пиров.

    На этом шаге peers пока особо не используем, но они пригодятся,
    когда будем реализовывать отправку RPC другим узлам.
    """
    node_id = os.getenv("NODE_ID", "node1")
    peers_raw = os.getenv("PEERS", "")
    peers = [p for p in (s.strip() for s in peers_raw.split(",")) if p and p != node_id]
    return NodeConfig(node_id=node_id, peers=peers)

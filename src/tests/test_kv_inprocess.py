# src/tests/test_kv_inprocess.py
#
# In-process тесты для RAFT + KV без Docker.
#
# Здесь мы не полагаемся на таймеры выбора лидера.
# Вместо этого в тестах явно "назначаем" узел лидером,
# чтобы проверить работу KV-слоя и интеграцию с RAFT-логом.

from __future__ import annotations

from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.raft.node import RaftRole


def create_single_node_client() -> TestClient:
    """
    Создаём приложение с одним узлом и явно назначаем его лидером.
    Это упрощённый режим для unit-тестов, чтобы не зависеть от
    таймеров выборов лидера и фоновых задач.
    """
    app = create_app()

    # Достаём RaftNode и "костыльно" делаем его лидером.
    node = app.state.raft_node
    node.role = RaftRole.LEADER
    node.leader_id = node.node_id

    # Можно также сбросить commit_index / last_applied, если хочется чистоту,
    # но для наших тестов достаточно явного лидера.
    return TestClient(app)


def test_single_node_marked_as_leader():
    """
    Проверяем, что в тестовом режиме узел действительно видится как LEADER.
    """
    client = create_single_node_client()

    resp = client.get("/raft/status")
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["node_id"]
    assert data["role"] == "LEADER"
    assert data["leader_id"] == data["node_id"]


def test_kv_put_get_delete_single_node():
    """
    Проверяем, что PUT/GET/DELETE по /kv работают,
    когда узел явно помечен как лидер.
    """
    client = create_single_node_client()

    key = "pytest-key"
    value = {"msg": "hello", "n": 42}

    # PUT
    r_put = client.put(f"/kv/{key}", json={"value": value})
    assert r_put.status_code == 200, r_put.text
    put_data = r_put.json()
    assert put_data["status"] == "ok"
    assert put_data["key"] == key

    # GET
    r_get = client.get(f"/kv/{key}")
    assert r_get.status_code == 200, r_get.text
    get_data = r_get.json()
    assert get_data["key"] == key
    assert get_data["value"] == value

    # DELETE
    r_del = client.delete(f"/kv/{key}")
    assert r_del.status_code == 200, r_del.text
    del_data = r_del.json()
    assert del_data["status"] == "ok"

    # GET после удаления -> 404
    r_get2 = client.get(f"/kv/{key}")
    assert r_get2.status_code == 404

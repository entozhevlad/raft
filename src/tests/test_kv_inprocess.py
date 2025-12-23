# src/tests/test_kv_inprocess.py
#
# In-process тесты для RAFT + KV без Docker.
#
# Здесь мы не полагаемся на таймеры выбора лидера.
# Вместо этого в тестах явно "назначаем" узел лидером,
# чтобы проверить работу KV-слоя и интеграцию с RAFT-логом.

from __future__ import annotations

import asyncio

import httpx
from fastapi.testclient import TestClient

from src.app.api.kv_routes import _confirm_leader_quorum
from src.app.main import create_app
from src.app.raft.node import RaftNode, RaftRole


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

def test_restart_persists_commit_index_and_last_applied(tmp_path, monkeypatch):
    """(2) После рестарта узел не должен повторно применять уже применённые записи.

    Проверяем два инварианта:
      1) commit_index и last_applied сохраняются и восстанавливаются.
      2) повторный вызов apply_committed_entries() после рестарта не меняет last_applied.
    """
    # Изолируем персистентность для теста.
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("NODE_ID", "node_test")
    monkeypatch.setenv("PEERS", "")

    # --- Первый запуск ---
    app1 = create_app()
    node1 = app1.state.raft_node
    node1.role = RaftRole.LEADER
    node1.leader_id = node1.node_id
    client1 = TestClient(app1)

    key = "k"
    value = {"v": 1}
    r_put = client1.put(f"/kv/{key}", json={"value": value})
    assert r_put.status_code == 200, r_put.text

    st1 = client1.get("/raft/status").json()
    assert st1["commit_index"] > 0
    assert st1["commit_index"] == st1["last_applied"]

    # --- "Рестарт" (новый объект приложения/узла, тот же DATA_DIR) ---
    app2 = create_app()
    node2 = app2.state.raft_node
    node2.role = RaftRole.LEADER
    node2.leader_id = node2.node_id
    client2 = TestClient(app2)

    # KV не должен измениться.
    r_get = client2.get(f"/kv/{key}")
    assert r_get.status_code == 200, r_get.text
    assert r_get.json()["value"] == value

    st2 = client2.get("/raft/status").json()
    assert st2["commit_index"] == st1["commit_index"]
    assert st2["last_applied"] == st1["last_applied"]

    # Повторный apply после рестарта не должен ничего "доприменять".
    before = node2.last_applied
    node2.apply_committed_entries()
    assert node2.last_applied == before

def test_snapshot_compaction_and_restart(tmp_path, monkeypatch):
    """
    (1) Snapshot/log compaction:
      - маленький threshold
      - много PUT
      - лог компакчен (base_index > 0, entries короткий)
      - после рестарта state восстановлен и base_index сохраняется
    """
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("NODE_ID", "node_test")
    monkeypatch.setenv("PEERS", "")
    monkeypatch.setenv("SNAPSHOT_THRESHOLD", "3")

    app1 = create_app()
    node1 = app1.state.raft_node
    node1.role = RaftRole.LEADER
    node1.leader_id = node1.node_id
    client1 = TestClient(app1)

    # 5 команд -> при threshold=3 должен появиться snapshot и compaction
    for i in range(5):
        r = client1.put(f"/kv/k{i}", json={"value": {"v": i}})
        assert r.status_code == 200, r.text

    assert node1.log.base_index >= 3
    assert len(node1.log.entries) <= 2  # хвост после snapshot

    # рестарт
    app2 = create_app()
    node2 = app2.state.raft_node
    node2.role = RaftRole.LEADER
    node2.leader_id = node2.node_id
    client2 = TestClient(app2)

    assert node2.log.base_index == node1.log.base_index
    # проверяем пару ключей
    r0 = client2.get("/kv/k0")
    assert r0.status_code == 200
    assert r0.json()["value"] == {"v": 0}

    r4 = client2.get("/kv/k4")
    assert r4.status_code == 200
    assert r4.json()["value"] == {"v": 4}




def test_readindex_quorum_ok():
    node = RaftNode(node_id="n1", peers=["n2", "n3"])
    node.role = RaftRole.LEADER
    node.leader_id = "n1"
    node.current_term = 5

    peer_addresses = {"n2": "http://n2:8000", "n3": "http://n3:8000"}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"term": 5, "success": True})

    transport = httpx.MockTransport(handler)

    async def run():
        async with httpx.AsyncClient(transport=transport) as client:
            await _confirm_leader_quorum(
                node=node,
                peer_addresses=peer_addresses,
                timeout_s=0.2,
                client=client,
            )

    asyncio.run(run())


def test_readindex_rejects_stale_leader_on_higher_term():
    node = RaftNode(node_id="n1", peers=["n2", "n3"])
    node.role = RaftRole.LEADER
    node.leader_id = "n1"
    node.current_term = 5

    peer_addresses = {"n2": "http://n2:8000", "n3": "http://n3:8000"}

    def handler(request: httpx.Request) -> httpx.Response:
        # Один peer отвечает term выше -> лидер должен стать follower
        host = request.url.host
        if host == "n3":
            return httpx.Response(200, json={"term": 6, "success": False})
        return httpx.Response(200, json={"term": 5, "success": True})

    transport = httpx.MockTransport(handler)

    async def run():
        async with httpx.AsyncClient(transport=transport) as client:
            try:
                await _confirm_leader_quorum(
                    node=node,
                    peer_addresses=peer_addresses,
                    timeout_s=0.2,
                    client=client,
                )
                assert False, "Expected stale_leader"
            except Exception as exc:
                # FastAPI HTTPException
                assert getattr(exc, "status_code", None) == 409
                assert node.role == RaftRole.FOLLOWER
                assert node.current_term == 6

    asyncio.run(run())

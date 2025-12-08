# app/api/kv_routes.py
#
# KV-API поверх RAFT:
#   - PUT    /kv/{key}
#   - GET    /kv/{key}
#   - DELETE /kv/{key}
#
# PUT/DELETE отправляются только лидеру.
# Лидер записывает команду в журнал и реплицирует её через AppendEntries.

from __future__ import annotations

from typing import Any, Dict

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from src.app.raft.log import LogEntry
from src.app.raft.models import AppendEntriesRequest
from src.app.raft.node import RaftNode, RaftRole

router = APIRouter(tags=["kv"])


def get_raft_node(request: Request) -> RaftNode:
    """
    Достаём RaftNode из state приложения.
    """
    raft_node: RaftNode = request.app.state.raft_node  # type: ignore[assignment]
    return raft_node


class PutRequest(BaseModel):
    """
    Тело запроса для PUT /kv/{key}.

    Можно было бы принимать произвольный JSON,
    но через модель проще документировать API.
    """
    value: Any


async def _replicate_command(request: Request, command: Dict[str, Any]) -> int:
    """
    Репликация одной команды через RAFT.
    Вызывается только на лидере.

    Алгоритм (упрощённый):
      1. Лидер добавляет запись в свой журнал.
      2. Шлёт AppendEntries с этой записью всем пирам.
      3. Считает успешные ответы.
      4. Если достигнуто большинство — увеличивает commitIndex
         и применяет запись к state machine.

    Возвращает индекс записи в журнале.
    """
    node = get_raft_node(request)

    if node.role != RaftRole.LEADER:
        raise RuntimeError("replicate_command called on non-leader")

    app = request.app
    peer_addresses: Dict[str, str] = app.state.peer_addresses  # type: ignore[assignment]

    # 1. Добавляем запись в локальный журнал.
    new_index = node.log.last_index() + 1
    entry = LogEntry(
        term=node.current_term,
        index=new_index,
        command=command,
    )
    node.log.append([entry])

    # 2. Формируем AppendEntries для пиров.
    term = node.current_term
    prev_log_index = new_index - 1
    prev_log_term = node.log.term_at(prev_log_index)
    leader_commit = node.commit_index

    successes = 1  # сам лидер
    total_nodes = 1 + len(peer_addresses)
    majority = total_nodes // 2 + 1

    async with httpx.AsyncClient(timeout=1.0) as client:
        for peer_id, base_url in peer_addresses.items():
            req = AppendEntriesRequest(
                term=term,
                leader_id=node.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=[command],  # отправляем только новую команду
                leader_commit=leader_commit,
            )

            try:
                resp = await client.post(
                    f"{base_url}/raft/append_entries",
                    json=req.model_dump(),
                )
                resp.raise_for_status()
                data = resp.json()
                resp_term = int(data.get("term", term))
                success = bool(data.get("success", False))

                if resp_term > node.current_term:
                    # Увидели более новый term — перестаём быть лидером.
                    node.become_follower(resp_term)
                    break

                if success:
                    successes += 1

            except Exception:
                # Узел недоступен или ошибка сети — игнорируем, RAFT это выдерживает.
                continue

    # 3. Если получили большинство — считаем запись закоммиченной.
    if successes >= majority:
        node.commit_index = new_index
        node.apply_committed_entries()
    else:
        # В "настоящем" RAFT запись остаётся в логе и будет
        # потихоньку доталкиваться до отставших узлов.
        # Для учебного проекта этого поведения достаточно:
        #   - мы не удаляем запись,
        #   - commitIndex не двигаем, пока не будет большинства.
        pass

    return new_index


# ============ HANDLERS ============

async def put_value(request: Request, key: str, body: PutRequest) -> dict:
    """
    Обработка PUT /kv/{key}.

    Только лидер принимает запись и реплицирует её через RAFT.
    Остальные узлы возвращают ошибку "not_leader".
    """
    node = get_raft_node(request)

    if node.role != RaftRole.LEADER:
        # Узел не лидер — подсказываем, кого он считает лидером.
        detail: Dict[str, Any] = {
            "error": "not_leader",
            "node_id": node.node_id,
            "role": node.role.name,
            "leader_id": node.leader_id,
        }

        # Попробуем найти адрес лидера, если знаем его id.
        peer_addresses: Dict[str, str] = request.app.state.peer_addresses  # type: ignore[assignment]
        if node.leader_id == node.node_id:
            # Теоретически не должно случаться
            pass
        elif node.leader_id is not None:
            leader_addr = peer_addresses.get(node.leader_id)
            if leader_addr:
                detail["leader_address"] = leader_addr

        raise HTTPException(status_code=409, detail=detail)

    command = {"op": "put", "key": key, "value": body.value}
    log_index = await _replicate_command(request, command)

    return {
        "status": "ok",
        "key": key,
        "value": body.value,
        "log_index": log_index,
        "term": node.current_term,
    }


async def get_value(request: Request, key: str) -> dict:
    """
    Обработка GET /kv/{key}.

    Для простоты читаем локальное состояние (state machine).
    Это означает, что при обращении к отстающему узлу
    могут быть слегка "старые" данные, но для учебного проекта это допустимо.
    """
    node = get_raft_node(request)
    value = node.state_machine.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail={"error": "key_not_found", "key": key})

    return {
        "key": key,
        "value": value,
        "node_id": node.node_id,
        "role": node.role.name,
        "leader_id": node.leader_id,
    }


async def delete_value(request: Request, key: str) -> dict:
    """
    Обработка DELETE /kv/{key}.

    Аналогично PUT: только лидер принимает команду и реплицирует её.
    """
    node = get_raft_node(request)

    if node.role != RaftRole.LEADER:
        detail: Dict[str, Any] = {
            "error": "not_leader",
            "node_id": node.node_id,
            "role": node.role.name,
            "leader_id": node.leader_id,
        }
        peer_addresses: Dict[str, str] = request.app.state.peer_addresses  # type: ignore[assignment]
        if node.leader_id and node.leader_id in peer_addresses:
            detail["leader_address"] = peer_addresses[node.leader_id]
        raise HTTPException(status_code=409, detail=detail)

    command = {"op": "delete", "key": key}
    log_index = await _replicate_command(request, command)

    return {
        "status": "ok",
        "key": key,
        "log_index": log_index,
        "term": node.current_term,
    }


# ============ РЕГИСТРАЦИЯ МАРШРУТОВ ============

router.add_api_route(
    path="/{key}",
    endpoint=put_value,
    methods=["PUT"],
    summary="Set value for key (through RAFT)",
)

router.add_api_route(
    path="/{key}",
    endpoint=get_value,
    methods=["GET"],
    summary="Get value for key",
)

router.add_api_route(
    path="/{key}",
    endpoint=delete_value,
    methods=["DELETE"],
    summary="Delete key (through RAFT)",
)

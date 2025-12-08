# app/raft/timers.py
#
# Фоновые задачи RAFT:
#   - цикл выборов (election loop)
#   - цикл heartbeat лидера (heartbeat loop)

from __future__ import annotations

import asyncio
import random
import time
import logging
from typing import Dict

import httpx
from fastapi import FastAPI

from src.app.raft.models import RequestVoteRequest, AppendEntriesRequest
from src.app.raft.node import RaftNode, RaftRole

logger = logging.getLogger("raft")

# Тайминги можно потом подстроить
ELECTION_TIMEOUT_RANGE = (1.5, 3.0)  # секунды
HEARTBEAT_INTERVAL = 0.5             # секунды


def setup_raft_background_tasks(app: FastAPI) -> None:
    """
    Вешаем startup/shutdown-обработчики, которые запускают/останавливают
    фоновые циклы выбора лидера и heartbeat.
    """

    @app.on_event("startup")
    async def _start_raft_tasks() -> None:
        raft_node: RaftNode = app.state.raft_node  # type: ignore[assignment]
        logger.info("[%s] Starting RAFT background tasks", raft_node.node_id)

        app.state._raft_tasks = [
            asyncio.create_task(election_loop(app, raft_node), name=f"election_loop-{raft_node.node_id}"),
            asyncio.create_task(heartbeat_loop(app, raft_node), name=f"heartbeat_loop-{raft_node.node_id}"),
        ]

    @app.on_event("shutdown")
    async def _stop_raft_tasks() -> None:
        tasks = getattr(app.state, "_raft_tasks", [])
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def election_loop(app: FastAPI, node: RaftNode) -> None:
    """
    Цикл выборов:
      - ждём случайный таймаут
      - если за это время не было heartbeat и мы не лидер — начинаем выборы
    """
    peer_addresses: Dict[str, str] = app.state.peer_addresses  # type: ignore[assignment]

    while True:
        timeout = random.uniform(*ELECTION_TIMEOUT_RANGE)
        await asyncio.sleep(timeout)

        if node.role == RaftRole.LEADER:
            continue

        now = time.monotonic()
        if now - node.last_heartbeat_ts < timeout:
            # недавно приходил heartbeat или проходили выборы
            continue

        # Старт выборов
        node.become_candidate()
        current_term = node.current_term
        last_index, last_term = node.last_log_index_term()

        total_nodes = 1 + len(peer_addresses)
        majority = total_nodes // 2 + 1
        votes_granted = 1  # голос за себя

        logger.info(
            "[%s] Start election term=%s, majority=%s",
            node.node_id,
            current_term,
            majority,
        )

        async with httpx.AsyncClient(timeout=1.0) as client:
            for peer_id, base_url in peer_addresses.items():
                if node.role != RaftRole.CANDIDATE or node.current_term != current_term:
                    break

                req = RequestVoteRequest(
                    term=current_term,
                    candidate_id=node.node_id,
                    last_log_index=last_index,
                    last_log_term=last_term,
                )

                try:
                    logger.info(
                        "[%s] -> RequestVote to %s at %s",
                        node.node_id,
                        peer_id,
                        base_url,
                    )
                    resp = await client.post(
                        f"{base_url}/raft/request_vote",
                        json=req.model_dump(),
                    )
                    logger.info(
                        "[%s] <- resp from %s: %s",
                        node.node_id,
                        peer_id,
                        resp.status_code,
                    )

                    if resp.status_code != 200:
                        # 503 и прочее — голос не засчитываем
                        continue

                    data = resp.json()
                    resp_term = int(data.get("term", current_term))
                    vote_granted = bool(data.get("vote_granted", False))

                    if resp_term > node.current_term:
                        node.become_follower(resp_term)
                        break

                    if vote_granted:
                        votes_granted += 1
                        logger.info(
                            "[%s] vote from %s (now votes=%s/%s)",
                            node.node_id,
                            peer_id,
                            votes_granted,
                            majority,
                        )
                        if votes_granted >= majority:
                            node.become_leader()
                            break

                except Exception as exc:
                    logger.warning(
                        "[%s] RequestVote to %s failed: %r",
                        node.node_id,
                        peer_id,
                        exc,
                    )
                    continue

        # Если цикл закончился, а мы всё ещё кандидат в том же term —
        # проверяем, набрали ли большинство (важно для кластера из 1 ноды).
        if (
            node.role == RaftRole.CANDIDATE
            and node.current_term == current_term
            and votes_granted >= majority
        ):
            node.become_leader()


async def heartbeat_loop(app: FastAPI, node: RaftNode) -> None:
    """
    Цикл heartbeat'ов лидера:
      - если мы лидер, периодически шлём AppendEntries с пустыми entries
    """
    peer_addresses: Dict[str, str] = app.state.peer_addresses  # type: ignore[assignment]

    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)

        if node.role != RaftRole.LEADER:
            continue

        current_term = node.current_term
        leader_commit = node.commit_index
        last_index, last_term = node.last_log_index_term()

        async with httpx.AsyncClient(timeout=1.0) as client:
            for peer_id, base_url in peer_addresses.items():
                req = AppendEntriesRequest(
                    term=current_term,
                    leader_id=node.node_id,
                    prev_log_index=last_index,
                    prev_log_term=last_term,
                    entries=[],
                    leader_commit=leader_commit,
                )

                try:
                    logger.info(
                        "[%s] -> Heartbeat to %s at %s (term=%s, commit=%s)",
                        node.node_id,
                        peer_id,
                        base_url,
                        current_term,
                        leader_commit,
                    )
                    resp = await client.post(
                        f"{base_url}/raft/append_entries",
                        json=req.model_dump(),
                    )
                    logger.info(
                        "[%s] <- heartbeat resp from %s: %s",
                        node.node_id,
                        peer_id,
                        resp.status_code,
                    )

                    if resp.status_code != 200:
                        continue

                    data = resp.json()
                    resp_term = int(data.get("term", current_term))

                    if resp_term > node.current_term:
                        node.become_follower(resp_term)
                        break

                except Exception as exc:
                    logger.warning(
                        "[%s] Heartbeat to %s failed: %r",
                        node.node_id,
                        peer_id,
                        exc,
                    )
                    continue

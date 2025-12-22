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

from src.app.raft.persistence import save_metadata, save_full_state
from src.app.raft.replication import replicate_to_peer, advance_commit_index
from src.app.raft.models import RequestVoteRequest
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
    node_data_dir: str = app.state.data_dir  # type: ignore[assignment]

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
        save_metadata(node, node_data_dir)

        current_term = node.current_term
        last_index, last_term = node.last_log_index_term()

        # Вместо "majority по всем peer_addresses" считаем кворум по текущей конфигурации
        votes_granted_by = {node.node_id}  # голос за себя

        logger.info(
            "[%s] Start election term=%s (voting_members=%s)",
            node.node_id,
            current_term,
            sorted(list(node.voting_members())),
        )

        async with httpx.AsyncClient(timeout=1.0) as client:
            for peer_id, base_url in peer_addresses.items():
                # Шлём RequestVote только voting members (и не себе)
                if peer_id == node.node_id:
                    continue
                if peer_id not in node.voting_members():
                    continue

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
                        continue

                    data = resp.json()
                    resp_term = int(data.get("term", current_term))
                    vote_granted = bool(data.get("vote_granted", False))

                    if resp_term > node.current_term:
                        node.become_follower(resp_term)
                        save_metadata(node, node_data_dir)
                        break

                    if vote_granted:
                        votes_granted_by.add(peer_id)
                        logger.info(
                            "[%s] vote from %s (votes_by=%s)",
                            node.node_id,
                            peer_id,
                            sorted(list(votes_granted_by)),
                        )
                        if node.has_election_quorum(votes_granted_by):
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

        if (
            node.role == RaftRole.CANDIDATE
            and node.current_term == current_term
            and node.has_election_quorum(votes_granted_by)
        ):
            node.become_leader()


async def heartbeat_loop(app: FastAPI, node: RaftNode) -> None:
    """
    Цикл heartbeat'ов/репликации лидера:
      - если мы лидер, периодически:
          1) догоняюще реплицируем лог на всех peers (это же и heartbeat)
          2) пытаемся продвинуть commitIndex
          3) применяем закоммиченное к state machine

    Важно: тут heartbeat НЕ шлётся "всем одинаковый prev_log_index".
    Вместо этого используем nextIndex/matchIndex для каждого peer.
    """
    peer_addresses: Dict[str, str] = app.state.peer_addresses  # type: ignore[assignment]
    node_data_dir: str = app.state.data_dir  # type: ignore[assignment]

    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)

        if node.role != RaftRole.LEADER:
            continue

        cluster_size = 1 + len(peer_addresses)

        async with httpx.AsyncClient(timeout=1.0) as client:
            leader_commit = node.commit_index

            # 1) Репликация/heartbeat на peers (только voting members текущей конфигурации)
            for peer_id, base_url in peer_addresses.items():
                if peer_id == node.node_id:
                    continue
                if peer_id not in node.voting_members():
                    continue

                ok = await replicate_to_peer(
                    client=client,
                    node=node,
                    peer_id=peer_id,
                    base_url=base_url,
                    leader_commit=leader_commit,
                )

                # replicate_to_peer мог перевести нас в follower, если увидел более новый term
                if node.role != RaftRole.LEADER:
                    save_metadata(node, node_data_dir)
                    break

                logger.debug(
                    "[%s] heartbeat/replicate peer=%s ok=%s nextIndex=%s matchIndex=%s",
                    node.node_id,
                    peer_id,
                    ok,
                    node.next_index.get(peer_id),
                    node.match_index.get(peer_id),
                )

            if node.role != RaftRole.LEADER:
                continue

            # 2) Пробуем продвинуть commitIndex (advance_commit_index теперь должен учитывать joint consensus)
            advanced = advance_commit_index(node, cluster_size=cluster_size)
            if advanced is not None:
                logger.info(
                    "[%s] leader advanced commit_index -> %s",
                    node.node_id,
                    node.commit_index,
                )
                node.apply_committed_entries()
                save_full_state(node, node_data_dir)

            # 3) Сохраняем metadata (для учебного проекта нормально)
            save_metadata(node, node_data_dir)
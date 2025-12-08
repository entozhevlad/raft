from __future__ import annotations

import os
import time
from typing import Dict, List, Tuple

import httpx
import pytest


DEFAULT_NODES = [
    "http://127.0.0.1:8000",
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",
]


@pytest.fixture(scope="session")
def nodes() -> List[str]:
    env = os.getenv("RAFT_NODES")
    if env:
        return [x.strip() for x in env.split(",") if x.strip()]
    return DEFAULT_NODES


@pytest.fixture(scope="session")
def http_client() -> httpx.Client:
    with httpx.Client(timeout=2.0) as client:
        yield client


@pytest.fixture(scope="session")
def node_statuses(nodes: List[str], http_client: httpx.Client) -> Dict[str, Dict]:
    """
    Читаем /raft/status со всех узлов один раз на сессию,
    с ожиданием старта сервиса.

    - Для каждого узла пробуем до 40 раз (до ~20 секунд),
      выходим сразу, как только получаем 200.
    - Узлы, которые так и не ответили 200, просто игнорируются.
    - Если вообще ни одного живого узла нет — валим тесты.
    """
    statuses: Dict[str, Dict] = {}

    for base in nodes:
        status_json = None
        for attempt in range(40):  # до ~20 секунд на узел
            try:
                resp = http_client.get(f"{base}/raft/status")
                if resp.status_code == 200:
                    status_json = resp.json()
                    print(
                        f"[INFO] {base} -> 200 OK (попытка {attempt + 1}/40, role={status_json.get('role')})"
                    )
                    break
                else:
                    print(
                        f"[WARN] {base} -> {resp.status_code} (попытка {attempt + 1}/40)"
                    )
            except Exception as exc:
                print(
                    f"[WARN] ошибка запроса к {base}: {exc} (попытка {attempt + 1}/40)"
                )

            time.sleep(0.5)

        if status_json is not None:
            statuses[base] = status_json
        else:
            print(
                f"[ERROR] {base} так и не ответил 200 на /raft/status, "
                f"пропускаем его в тестах."
            )

    if not statuses:
        pytest.fail("Ни один узел не ответил 200 на /raft/status — кластер не готов.")

    return statuses


@pytest.fixture(scope="session")
def leader(node_statuses: Dict[str, Dict]) -> Tuple[str, Dict]:
    """
    Находит единственного лидера среди узлов, по /raft/status.
    Возвращает (base_url, status_json).
    """
    leaders: List[Tuple[str, Dict]] = []
    for base, st in node_statuses.items():
        if st.get("role") == "LEADER":
            leaders.append((base, st))

    if not leaders:
        pytest.fail("Лидер не найден (нет узлов с role=LEADER).")

    if len(leaders) > 1:
        pytest.fail(f"Найдено несколько лидеров: {leaders!r}")

    return leaders[0]

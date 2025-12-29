# Distributed Key-Value Store on RAFT

Учебный курсовой проект: распределённое отказоустойчивое key-value хранилище, реализованное на основе алгоритма консенсуса Raft.

## Цель проекта

Разработка распределённой системы с консенсусом, обеспечивающей:
- согласованный журнал операций
- отказоустойчивость
- восстановление состояния без потери данных
- линейризуемые операции записи и чтения

## Архитектура

- Алгоритм консенсуса: Raft
- Тип хранилища: key-value
- Роли узлов: follower / candidate / leader
- Репликация: AppendEntries + InstallSnapshot
- Персистентность: WAL + snapshot
- Транспорт: HTTP (FastAPI)
- Запуск: Docker + docker-compose
- Кластер: статический

## Запуск

docker compose up -d --build

Кластер из 3 узлов:
- node1 — 8000
- node2 — 8001
- node3 — 8002

## Проверка

Health:
curl http://localhost:8000/health

Raft status:
curl http://localhost:8000/raft/status

## Key-Value API

PUT:
curl -X PUT http://localhost:8000/kv/a -H "Content-Type: application/json" -d '{"value": 123}'

GET:
curl http://localhost:8000/kv/a

DELETE:
curl -X DELETE http://localhost:8000/kv/a

Все операции выполняются через лидера. При обращении к не-лидеру возвращается ошибка not_leader с адресом текущего лидера.

## Отказоустойчивость

Реализованы сценарии:
- перевыбор лидера при падении
- отказ записи при отсутствии кворума
- восстановление узла после перезапуска
- догоняющая репликация
- установка snapshot при сильном отставании
- pause/unpause контейнера

## Персистентность

Каждый узел сохраняет состояние на диск:
- metadata.json
- log.json
- snapshot.json
- kv.json

После рестарта состояние корректно восстанавливается.

## Технологии

- Python 3.13
- FastAPI
- asyncio
- httpx
- Docker, docker-compose

## Назначение

Проект выполнен в рамках учебного курса для демонстрации работы алгоритма Raft и распределённых систем.

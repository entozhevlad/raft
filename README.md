# Raft KV Store (static cluster)

Простейшая учебная реализация KV-хранилища поверх Raft.

- Состав кластера статический: `NODE_ID` + `PEERS` из переменных окружения.
- Dynamic membership / joint consensus не поддерживаются; состав не меняется во время работы.
- Кворум вычисляется как `len(cluster_nodes) // 2 + 1` одинаково для выборов и коммита.
- Поддержаны базовые части Raft: выборы, AppendEntries с backtracking, commit/apply, snapshot/compaction и восстановление после перезапуска.


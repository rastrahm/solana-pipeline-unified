# Flujograma del sistema

[English](flujograma-EN.md) · **Español** · [Índice](README.md)

Vista de **alto nivel**: cómo se conectan los tres crates del stack de aprendizaje.

```mermaid
flowchart LR
  subgraph red["mini-solana-turbine"]
    UDP["UDP / bytes sintéticos"]
    PIPE["Pipeline<br/>parse + FEC + forward plan"]
    UDP --> PIPE
  end

  subgraph orq["solana-pipeline-unified"]
    ORCH["Orchestrator<br/>ciclo de vida"]
    BR["Bridge<br/>cola acotada"]
    ORCH --> BR
    ORCH --> PIPE
  end

  subgraph disco["nvme-state-db"]
    ENG["Engine"]
    WAL["WAL O_DIRECT"]
    MEM["MemTable"]
    SST["SSTables"]
    ENG --> WAL
    ENG --> MEM
    MEM --> SST
  end

  PIPE -->|"ingest_bytes<br/>shards learn/v1/…"| ORCH
  BR -->|"put"| ENG
```

## Lectura rápida

| Capa | Responsabilidad |
| --- | --- |
| `mini-solana-turbine` | Shreds, Reed-Solomon, plan Turbine (sin disco) |
| `solana-pipeline-unified` | Pegar etapas: arranque, cola, llamadas a APIs |
| `nvme-state-db` | **Única** persistencia (WAL → MemTable → SST) |

## Analogía Solana (simplificada)

```mermaid
flowchart TB
  TVU["TVU / Turbine<br/>propagación de shreds"] --> GLUE["Orquestador<br/>este repo"]
  GLUE --> ADB["Estado KV<br/>≈ AccountsDB"]
  BS["Blockstore / ledger"] -.->|"fuera de alcance"| X["—"]
```

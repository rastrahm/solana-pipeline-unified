# System flowchart

**English** · [Español](flujograma-ES.md) · [Index](README.md)

**High-level** view: how the three crates of the learning stack connect.

```mermaid
flowchart LR
  subgraph red["mini-solana-turbine"]
    UDP["UDP / synthetic bytes"]
    PIPE["Pipeline<br/>parse + FEC + forward plan"]
    UDP --> PIPE
  end

  subgraph orq["solana-pipeline-unified"]
    ORCH["Orchestrator<br/>lifecycle"]
    BR["Bridge<br/>bounded queue"]
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

## Quick read

| Layer | Responsibility |
| --- | --- |
| `mini-solana-turbine` | Shreds, Reed-Solomon, Turbine plan (no disk) |
| `solana-pipeline-unified` | Glue stages together: startup, queue, API calls |
| `nvme-state-db` | The **only** persistence (WAL → MemTable → SST) |

## Solana analogy (simplified)

```mermaid
flowchart TB
  TVU["TVU / Turbine<br/>shred propagation"] --> GLUE["Orchestrator<br/>this repo"]
  GLUE --> ADB["KV state<br/>≈ AccountsDB"]
  BS["Blockstore / ledger"] -.->|"out of scope"| X["—"]
```

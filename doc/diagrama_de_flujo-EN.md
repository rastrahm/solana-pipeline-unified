# Control flow diagram

**English** · [Español](diagrama_de_flujo-ES.md) · [Index](README.md)

The orchestrator's **control** flow: from startup to persisting shards and shutting down.

## 1. Lifecycle (`start` / `shutdown`)

```mermaid
flowchart TD
  A([Start]) --> B[Orchestrator::new config]
  B --> C{start}
  C -->|already Running| E1[Error InvalidOrchestratorState]
  C -->|Idle| D[Engine::open_with]
  D -->|fails| E2[Error EngineOpenFailed]
  D --> F[Bridge::start pipeline-bridge thread]
  F -->|spawn fails| E3[Error BridgeSpawnFailed]
  F --> G{simd feature?}
  G -->|yes| H[TurbineTree + Pipeline::with_defaults]
  H -->|fails| E4[Error TurbineSetupFailed]
  H --> I[phase = Running]
  G -->|no| I
  I --> J([Ready for submit / ingest])

  J --> K{shutdown}
  K -->|Idle| L([no-op Ok])
  K -->|Running| M[Bridge::shutdown drains queue]
  M --> N[Engine::flush]
  N --> O[drop Pipeline and Engine]
  O --> P[phase = Idle]
  P --> Q([End])
```

## 2. Ingest → bridge → disk (`ingest_bytes` / `submit_record`)

```mermaid
flowchart TD
  A([Shred bytes]) --> B{Orchestrator Running?}
  B -->|no| E0[InvalidOrchestratorState]
  B -->|yes| C[Pipeline::ingest_bytes]
  C -->|parse/FEC error| E1[TurbineIngestFailed]
  C --> D[For each present original_shard]
  D --> E[make_learn_key shard/N]
  E --> F[BridgeSender::try_send]
  F -->|queue full| G[maybe_schedule_flush]
  G --> E2[PipelineStall]
  F -->|ok| H[Bridge thread: Engine::put]
  H -->|put fails| E3[PersistFailed]
  H --> I[maybe_schedule_flush if needs_flush]
  I --> J[IngestOutcome]
  J --> K([End ingest])

  L([direct submit_record]) --> F
```

## 3. CLI demo (`--demo`)

```mermaid
flowchart TD
  A([cargo run -- --data-dir DIR --demo]) --> B[parse argv]
  B --> C[Orchestrator::start]
  C --> D[Encode synthetic data + code shreds]
  D --> E[ingest_bytes data]
  E --> F[ingest_bytes code]
  F --> G[Print Turbine metrics]
  G --> H[shutdown: drain bridge + flush]
  H --> I[Engine::open again]
  I --> J{get shard/0 and shard/1 = 64 B?}
  J -->|yes| K([Demo OK])
  J -->|no| L([Error: incomplete demo])
```

## Durable write order (inside nvme)

```mermaid
sequenceDiagram
  participant O as Orchestrator
  participant B as Bridge worker
  participant E as Engine
  participant W as WAL
  participant M as MemTable

  O->>B: StateRecord enqueued
  B->>E: put(key, value)
  E->>W: 4K-aligned append
  E->>M: put into SkipList
  Note over E: separate flush: MemTable → SST
```

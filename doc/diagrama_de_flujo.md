# Diagrama de flujo

Flujo de **control** del orquestador: desde el arranque hasta persistir shards y apagar.

## 1. Ciclo de vida (`start` / `shutdown`)

```mermaid
flowchart TD
  A([Inicio]) --> B[Orchestrator::new config]
  B --> C{start}
  C -->|ya Running| E1[Error InvalidOrchestratorState]
  C -->|Idle| D[Engine::open_with]
  D -->|falla| E2[Error EngineOpenFailed]
  D --> F[Bridge::start hilo pipeline-bridge]
  F -->|falla spawn| E3[Error BridgeSpawnFailed]
  F --> G{feature simd?}
  G -->|sí| H[TurbineTree + Pipeline::with_defaults]
  H -->|falla| E4[Error TurbineSetupFailed]
  H --> I[phase = Running]
  G -->|no| I
  I --> J([Listo para submit / ingest])

  J --> K{shutdown}
  K -->|Idle| L([no-op Ok])
  K -->|Running| M[Bridge::shutdown drena cola]
  M --> N[Engine::flush]
  N --> O[soltar Pipeline y Engine]
  O --> P[phase = Idle]
  P --> Q([Fin])
```

## 2. Ingestión → bridge → disco (`ingest_bytes` / `submit_record`)

```mermaid
flowchart TD
  A([Bytes de shred]) --> B{Orchestrator Running?}
  B -->|no| E0[InvalidOrchestratorState]
  B -->|sí| C[Pipeline::ingest_bytes]
  C -->|error parse/FEC| E1[TurbineIngestFailed]
  C --> D[Por cada original_shard presente]
  D --> E[make_learn_key shard/N]
  E --> F[BridgeSender::try_send]
  F -->|cola llena| G[maybe_schedule_flush]
  G --> E2[PipelineStall]
  F -->|ok| H[Hilo bridge: Engine::put]
  H -->|falla put| E3[PersistFailed]
  H --> I[maybe_schedule_flush si needs_flush]
  I --> J[IngestOutcome]
  J --> K([Fin ingest])

  L([submit_record directo]) --> F
```

## 3. Demo CLI (`--demo`)

```mermaid
flowchart TD
  A([cargo run -- --data-dir DIR --demo]) --> B[parse argv]
  B --> C[Orchestrator::start]
  C --> D[Codificar shred data + code sintéticos]
  D --> E[ingest_bytes data]
  E --> F[ingest_bytes code]
  F --> G[Imprimir métricas Turbine]
  G --> H[shutdown: drena bridge + flush]
  H --> I[Engine::open otra vez]
  I --> J{get shard/0 y shard/1 = 64 B?}
  J -->|sí| K([Demo OK])
  J -->|no| L([Error demo incompleta])
```

## Orden durable de escritura (en nvme)

```mermaid
sequenceDiagram
  participant O as Orchestrator
  participant B as Bridge worker
  participant E as Engine
  participant W as WAL
  participant M as MemTable

  O->>B: StateRecord en cola
  B->>E: put(key, value)
  E->>W: append alineado 4K
  E->>M: put en SkipList
  Note over E: flush aparte: MemTable → SST
```

# Diagrama de clases

[English](diagrama_de_clases-EN.md) · **Español** · [Índice](README.md)

Tipos públicos principales de `solana-pipeline-unified` y su relación con los crates hermanos (solo lo que el orquestador usa).

```mermaid
classDiagram
  direction TB

  class OrchestratorConfig {
    +PathBuf data_dir
    +usize queue_capacity
    +Option~usize~ mem_capacity_bytes
    +NodeId self_id
    +Stake self_stake
    +SocketAddr self_addr
    +bool auto_schedule_flush
    +local(data_dir, addr) OrchestratorConfig
  }

  class Orchestrator {
    -OrchestratorConfig config
    -Phase phase
    -Option~Arc~Engine~~ engine
    -Option~Bridge~ bridge
    -Option~Pipeline~ pipeline
    +new(config) Orchestrator
    +start() Result
    +shutdown() Result
    +submit_record(key, value) Result
    +ingest_bytes(bytes) Result~IngestOutcome~
    +needs_flush() Result~bool~
    +schedule_flush() Result
    +flush() Result
    +maybe_schedule_flush() Result
  }

  class IngestOutcome {
    +usize reconstructed
    +usize forward_dest_count
    +usize records_submitted
  }

  class Bridge {
    -Option~BridgeSender~ tx
    -Option~JoinHandle~ worker
    +start(engine, capacity) Result~Bridge~
    +submit_record(key, value) Result
    +shutdown() Result
  }

  class BridgeSender {
    +submit_record(key, value) Result
  }

  class BridgeReceiver {
    +drain_into(engine) Result
  }

  class StateRecord {
    +Vec~u8~ key
    +Vec~u8~ value
  }

  class Error {
    <<enumeration>>
    Unimplemented
    BridgeSaturated
    BridgeSpawnFailed
    PersistFailed
    EmptyRecordKey
    InvalidOrchestratorState
    EngineOpenFailed
    TurbineSetupFailed
    TurbineIngestFailed
    PipelineStall
  }

  class Engine {
    <<nvme-state-db>>
    +open(dir) Result
    +put(key, value) Result
    +get(key) Result
    +needs_flush() Result~bool~
    +schedule_flush() Result
    +flush() Result
  }

  class Pipeline {
    <<mini-solana-turbine>>
    +with_defaults(tree, self_id) Result
    +ingest_bytes(bytes) Result
    +original_shard(index) Result
  }

  OrchestratorConfig --> Orchestrator : configura
  Orchestrator --> Bridge : posee
  Orchestrator --> Engine : Arc
  Orchestrator --> Pipeline : opcional simd
  Orchestrator ..> IngestOutcome : produce
  Orchestrator ..> Error : propaga
  Bridge --> BridgeSender : productor
  Bridge --> BridgeReceiver : hilo consumidor
  BridgeSender ..> StateRecord : encola
  BridgeReceiver --> Engine : put
  Bridge ..> Error : propaga
```

## Notas

- `Pipeline` solo existe con feature `simd`.
- El disco (WAL/MemTable/SST) vive **dentro** de `Engine`; el orquestador no lo reimplementa.
- `make_learn_key` / `LEARN_KEY_PREFIX` (`learn/v1/`) son helpers de clave educativa, no tipos de clase.

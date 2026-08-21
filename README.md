# solana-pipeline-unified

Orquestador de **aprendizaje**: une [`mini-solana-turbine`](../mini-solana-turbine) (shreds / FEC / Turbine en memoria) con [`nvme-state-db`](../nvme-state-db) (única persistencia a disco).

No es un validador Solana. No hay gossip, Blockstore de ledger, replay ni consenso. El objetivo es seguir el cableado red→FEC→cola→`Engine::put` en laboratorio.

Plan por fases (autorizar una a una): [`FASES.md`](FASES.md).

---

## Requisitos

| Requisito | Valor |
| --- | --- |
| SO | Linux x86_64 |
| Crates hermanos | `../mini-solana-turbine`, `../nvme-state-db` (path) |
| Rust | edición 2021 |
| Features útiles | `simd` (FEC + `ingest_bytes`), `uring` (tipos UDP de turbine; la demo CLI no hace bind) |

---

## Cómo fluye el dato (mapa mental)

```text
shreds (bytes sintéticos o, más adelante, UDP)
        │
        ▼
mini-solana-turbine::Pipeline   ← parse + Reed-Solomon + plan de forward
        │
        ▼
bridge (cola acotada)           ← try_send; si llena → PipelineStall
        │
        ▼
nvme-state-db::Engine::put      ← WAL / MemTable / SST (solo este crate toca disco)
```

Analogía Solana (simplificada): Turbine ≈ trozo de TVU; `nvme-state-db` ≈ almacén de **estado** tipo AccountsDB (KV), no Blockstore.

Claves educativas: `learn/v1/shard/N` (no es el encoding real de Solana).

---

## Inicio rápido

```bash
# tests (features default: uring + simd)
cargo test

cargo clippy --all-targets -- -D warnings

# demo: FEC → bridge → Engine en un directorio temporal
mkdir -p /tmp/spu-demo
cargo run --features simd -- --data-dir /tmp/spu-demo --demo
```

Sin `--demo` solo abre y cierra el motor (comprueba wiring).

### CLI

```text
solana-pipeline-unified --data-dir DIR [--addr HOST:PORT] [--queue-cap N] [--demo] [--help]
```

| Flag | Rol |
| --- | --- |
| `--data-dir` | Directorio WAL/SST de `nvme-state-db` |
| `--addr` | Addr lógica del nodo en el árbol Turbine (default `127.0.0.1:9001`) |
| `--queue-cap` | Tamaño de la cola del bridge |
| `--demo` | Ingiere data+code sintéticos y persiste `shard/0` y `shard/1` |

---

## Qué aprender en cada capa

1. **Orchestrator** — `start` / `shutdown`, ciclo de vida del `Engine` y del bridge.
2. **Bridge** — cola acotada; el productor no se bloquea (`BridgeSaturated` / `PipelineStall`).
3. **Flush** — `needs_flush` → `schedule_flush`; recovery al volver a abrir el mismo `data-dir`.
4. **Ingest** — `ingest_bytes` (feature `simd`): Turbine en memoria, sin UDP de reenvío en esta demo.

Detalle de fases: [`FASES.md`](FASES.md). Reglas del agente: [`.cursorrules`](.cursorrules).

---

## Limitaciones vs Solana real

| Solana (Agave) | Este repo |
| --- | --- |
| TVU + retransmit UDP | Demo con bytes sintéticos; sin bind/send en el CLI |
| Blockstore (ledger) | Fuera de alcance |
| AccountsDB / Bank / replay | Solo KV genérico vía `Engine` |
| Gossip, repair, votos | Fuera de alcance |

---

## Árbol relevante

```text
solana-pipeline-unified/
├── FASES.md
├── README.md
├── Cargo.toml
├── src/
│   ├── main.rs              # CLI demo
│   ├── lib.rs
│   ├── error.rs
│   └── pipeline/
│       ├── bridge.rs
│       ├── orchestrator.rs
│       └── outcome.rs
├── tests/
│   ├── deps_iniciales.rs
│   ├── ingest_to_bridge.rs
│   └── flush_recovery.rs
└── benches/
    └── e2e_throughput.rs
```
Bench (laboratorio, no mainnet):

```bash
cargo bench --bench e2e_throughput --features simd
```

Números de línea base: ver [`FASES.md`](FASES.md) (fase 8).

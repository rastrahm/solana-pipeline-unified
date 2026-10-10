# solana-pipeline-unified

**English** · [Español](README-ES.md) · [Index](README.md)

**Learning** orchestrator: wires [`mini-solana-turbine`](../mini-solana-turbine) (in-memory shreds / FEC / Turbine) to [`nvme-state-db`](../nvme-state-db) (the only disk persistence layer).

This is not a Solana validator. There is no gossip, ledger Blockstore, replay, or consensus. The goal is to follow the network→FEC→queue→`Engine::put` wiring in a lab setting.

Phase plan (authorized one at a time): [`FASES.md`](FASES.md) (Spanish).

---

## Requirements

| Requirement | Value |
| --- | --- |
| OS | Linux x86_64 |
| Sibling crates | `../mini-solana-turbine`, `../nvme-state-db` (path) |
| Rust | 2021 edition |
| Useful features | `simd` (FEC + `ingest_bytes`), `uring` (turbine UDP types; the CLI demo does not bind) |

---

## How data flows (mental map)

```text
shreds (synthetic bytes or, later, UDP)
        │
        ▼
mini-solana-turbine::Pipeline   ← parse + Reed-Solomon + forward plan
        │
        ▼
bridge (bounded queue)          ← try_send; if full → PipelineStall
        │
        ▼
nvme-state-db::Engine::put      ← WAL / MemTable / SST (only this crate touches disk)
```

Solana analogy (simplified): Turbine ≈ a slice of the TVU; `nvme-state-db` ≈ an AccountsDB-style **state** store (KV), not the Blockstore.

Educational keys: `learn/v1/shard/N` (not Solana's real encoding).

---

## Quick start

```bash
# tests (default features: uring + simd)
cargo test

cargo clippy --all-targets -- -D warnings

# demo: FEC → bridge → Engine in a temporary directory
mkdir -p /tmp/spu-demo
cargo run --features simd -- --data-dir /tmp/spu-demo --demo
```

Without `--demo` it only opens and closes the engine (checks the wiring).

### CLI

```text
solana-pipeline-unified --data-dir DIR [--addr HOST:PORT] [--queue-cap N] [--demo] [--help]
```

| Flag | Role |
| --- | --- |
| `--data-dir` | WAL/SST directory for `nvme-state-db` |
| `--addr` | Logical node address in the Turbine tree (default `127.0.0.1:9001`) |
| `--queue-cap` | Bridge queue size |
| `--demo` | Ingests synthetic data+code shreds and persists `shard/0` and `shard/1` |

---

## What to learn at each layer

1. **Orchestrator** — `start` / `shutdown`, lifecycle of the `Engine` and the bridge.
2. **Bridge** — bounded queue; the producer never blocks (`BridgeSaturated` / `PipelineStall`).
3. **Flush** — `needs_flush` → `schedule_flush`; recovery when reopening the same `data-dir`.
4. **Ingest** — `ingest_bytes` (`simd` feature): in-memory Turbine, no forwarding UDP in this demo.

Phase details: [`FASES.md`](FASES.md). Agent rules: [`.cursorrules`](.cursorrules).

---

## Limitations vs real Solana

| Solana (Agave) | This repo |
| --- | --- |
| TVU + UDP retransmit | Demo with synthetic bytes; no bind/send in the CLI |
| Blockstore (ledger) | Out of scope |
| AccountsDB / Bank / replay | Generic KV only, via `Engine` |
| Gossip, repair, votes | Out of scope |

---

## Relevant tree

```text
solana-pipeline-unified/
├── FASES.md
├── README.md                # ES/EN index
├── README-ES.md
├── README-EN.md
├── doc/
│   ├── README.md            # ES/EN index
│   ├── README-{ES,EN}.md
│   ├── flujograma-{ES,EN}.md
│   ├── diagrama_de_clases-{ES,EN}.md
│   └── diagrama_de_flujo-{ES,EN}.md
├── portfolio/               # bilingual HTML page
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
Bench (lab, not mainnet):

```bash
cargo bench --bench e2e_throughput --features simd
```

Baseline numbers: see [`FASES.md`](FASES.md) (phase 8).

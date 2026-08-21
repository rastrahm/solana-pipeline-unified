# Plan de fases — solana-pipeline-unified

Proyecto de **aprendizaje**: orquestador que une `mini-solana-turbine` (red + shreds + FEC + reenvío) con `nvme-state-db` (única persistencia a disco).

Cada fase requiere autorización explícita (`autoriza fase N`) antes de empezar. Al cerrar una fase se explica qué se hizo, qué se aprendió y se pide permiso para la siguiente.

Estado del repositorio: **Fase 6 completa**. Siguiente: fase 7 (binario demo + README).

---

## Protocolo

1. Autorizar por escrito: **autoriza fase N**.
2. Implementar **solo esa fase**, con TDD (`mod tests` o `tests/` primero).
3. Verificar con `cargo test` y `cargo clippy` (desde la fase en que exista el crate).
4. Resumir: qué se construyó, qué se aprendió, qué queda fuera.
5. No adelantar fases.

Reglas vigentes: `.cursorrules` de este repo. Persistencia: solo vía `nvme-state-db` (ver su `FASES.md` / API `Engine`).

---

## Decisiones cerradas (aplican a todas las fases)

| Tema | Decisión |
| --- | --- |
| Rol de este crate | Solo **orquestación**: ciclo de vida, colas entre etapas, llamadas a las APIs de los otros dos crates. |
| Red / shreds / FEC / Turbine | Solo `mini-solana-turbine`. No reimplementar. |
| Disco / WAL / MemTable / SST | Solo `nvme-state-db`. Este crate **no** abre archivos de estado ni habla de `O_DIRECT`. |
| API de persistencia | `Engine::put` / `get` / `delete` / `flush` (y helpers). No duplicar `Wal` a mano salvo que una fase lo autorice explícitamente como ejercicio. |
| Analogía Solana (aprendizaje) | Turbine ≈ propagación de shreds (TVU parcial). `nvme-state-db` ≈ almacén de **estado** tipo AccountsDB (KV), no un Blockstore completo de ledger. |
| Errores | Lib/pipeline: `thiserror`. Binario (`main`): `anyhow` solo en bootstrap. |
| Producción | Sin `unwrap()` / `expect()` en `src/` (sí en tests/benches). |
| Hot path | Evitar allocs y copias innecesarias al pasar de turbine → `Engine`; la alineación 4K del WAL es **interna** a nvme. |
| Tests | TDD antes de implementar. |
| Target | Linux x86_64, kernel 6.8+. |

---

## Mapa mental (Solana → estos repos)

```text
Solana (simplificado)              Este stack de aprendizaje
─────────────────────              ─────────────────────────
TVU: recibir / reenviar shreds  →  mini-solana-turbine
Pegar etapas del validador      →  solana-pipeline-unified  (este repo)
AccountsDB / estado KV          →  nvme-state-db
Blockstore (ledger de shreds)   →  fuera de alcance (por ahora)
Replay / ejecución de txs       →  fuera de alcance (por ahora)
```

Flujo objetivo del orquestador (cuando las fases lo permitan):

```text
UDP (turbine) → ingest/FEC (turbine) → [cola acotada] → put/flush (nvme Engine)
                      ↘ reenvío Turbine (turbine), si aplica
```

---

## Estructura de directorios prevista

```text
solana-pipeline-unified/
├── Cargo.toml
├── FASES.md                 # este archivo
├── .cursorrules
├── src/
│   ├── lib.rs
│   ├── main.rs
│   ├── error.rs
│   └── pipeline/
│       ├── mod.rs
│       ├── bridge.rs        # cola / entrega hacia Engine
│       └── orchestrator.rs  # arranque y ciclo de vida
├── tests/
└── benches/
    └── e2e_throughput.rs
```

---

## Fase 0 — Plan y reglas (roles claros)

**Estado:** completa (al escribir este archivo y alinear `.cursorrules`).

**Objetivo de aprendizaje:** delimitar responsabilidades antes de escribir código; evitar que el orquestador “sea también el disco”.

**Alcance**

- `FASES.md` (este archivo).
- Alinear `.cursorrules` de **este** repo y el de **`nvme-state-db`** con la decisión: disco solo en nvme; unificado solo orquesta.
- Dejar constancia del mapa Solana → crates (arriba).

**Fuera de alcance:** `Cargo.toml`, `src/`, deps path, benches.

**Criterio de cierre:** plan por fases + rules sin atribuir WAL/`O_DIRECT` al orquestador.

**Hecho:** `FASES.md`; `.cursorrules` de `solana-pipeline-unified` y de `nvme-state-db` actualizados (roles + estilo de aprendizaje).

---

## Fase 1 — Crate vacío, errores y módulos stub

**Estado:** completa (`cargo test` 5/5, `cargo clippy -D warnings` limpio).

**Objetivo de aprendizaje:** crate lib+bin, `thiserror`, módulos públicos documentados sin lógica aún.

**Alcance**

- `Cargo.toml` (edition 2021+, `thiserror`, `anyhow` solo en el binario).
- Aún **sin** path deps a turbine/nvme (eso es fase 2).
- `src/lib.rs`, `src/main.rs` (main mínimo), `src/error.rs`.
- Stubs: `src/pipeline/{mod,bridge,orchestrator}.rs` con tipos/funciones documentadas que devuelven `Error::Unimplemented` (sin `todo!` en tests).
- Test inicial: el crate compila; `Error` implementa `std::error::Error`.

**Fuera de alcance:** UDP, FEC, `Engine`, colas reales, I/O.

**Criterio de cierre:** `cargo test` y `cargo clippy -D warnings` verdes.

**Hecho:** `Cargo.toml`, `.gitignore`, `src/{lib,main,error}.rs`, `src/pipeline/{mod,bridge,orchestrator}.rs`; variantes placeholder `BridgeSaturated` / `PersistFailed` / `InvalidOrchestratorState` / `PipelineStall` para fases siguientes.

---

## Fase 2 — Dependencias path y tests iniciales de tipos

**Estado:** completa (`cargo test` verde con features default; `--no-default-features --features simd` también; `clippy -D warnings` limpio).

**Objetivo de aprendizaje:** enlazar crates hermanos por `path`, features de turbine (`uring`/`simd`), importar solo API pública.

**Alcance**

- `Cargo.toml`: `mini-solana-turbine` y `nvme-state-db` vía `path = "..."`.
- Features del unificado: `default = ["uring", "simd"]` reenviadas a turbine; sin io_uring: `cargo test --no-default-features --features simd`.
- `tests/deps_iniciales.rs`: tipos públicos, `Engine::open` en tempdir, árbol Turbine, `Pipeline` si `simd`, enlace de `UdpIngress` si `uring` (sin bind).

**Fuera de alcance:** loop de red, `put` de producción, bridge con backpressure.

**Criterio de cierre:** `cargo test` enlaza ambos crates; clippy limpio.

**Hecho:** deps path + features; tests iniciales de integración de tipos; `tempfile` como dev-dep.

---

## Fase 3 — Orchestrator: ciclo de vida

**Estado:** completa (`cargo test` + `clippy -D warnings`).

**Objetivo de aprendizaje:** arrancar y apagar ordenadamente lo que ya existe en los otros crates (abrir `Engine`, preparar Turbine en memoria).

**Alcance**

- `OrchestratorConfig` (data_dir, queue_capacity, MemTable opcional, id/stake/addr locales).
- `start` / `shutdown` / `is_running` / `engine` (+ `pipeline` si `simd`).
- `shutdown` hace `flush` y suelta el `Engine` (su `Drop` espera al hilo de flush).
- Sin UDP ni bridge con cola.

**Fuera de alcance:** benchmark e2e, Blockstore de shreds.

**Criterio de cierre:** tests de arranque/parada / doble start / restart; clippy limpio.

**Hecho:** `orchestrator.rs` real; errores `EngineOpenFailed` / `TurbineSetupFailed`; bridge sigue stub.

---

## Fase 4 — Bridge: de resultado de ingest a `Engine::put`

**Estado:** completa (`cargo test` + `clippy -D warnings`).

**Objetivo de aprendizaje:** frontera orquestador ↔ storage. Mapear un registro de aprendizaje hacia `put`, sin reimplementar WAL.

**Alcance**

- Cola acotada (`crossbeam-channel`) + hilo `pipeline-bridge` que solo llama `Engine::put`.
- `try_send` → `BridgeSaturated` si la cola está llena (no bloquea al productor).
- Convención `learn/v1/` + sufijo (`make_learn_key`); no es AccountsDB real.
- `Orchestrator::submit_record` + shutdown que drena la cola antes del flush.

**Fuera de alcance:** ejecución de transacciones, repair, forks, UDP.

**Criterio de cierre:** N ítems encolados visibles con `get` tras drenar/flush; test de saturación.

**Hecho:** `bridge.rs` real; deps `crossbeam-channel`; errores `EmptyRecordKey` / `BridgeSpawnFailed`.

---

## Fase 5 — Cablear ingestión (turbine) al bridge

**Estado:** completa (`cargo test` + `clippy -D warnings`).

**Objetivo de aprendizaje:** tramo TVU-like en laboratorio: ingest → (plan de forward) → bridge → nvme.

**Alcance**

- `Orchestrator::ingest_bytes` (feature `simd`): llama a `Pipeline::ingest_bytes`, encola shards presentes como `learn/v1/shard/N`.
- `IngestOutcome` (reconstructed, forward_dest_count, records_submitted).
- Test de integración `tests/ingest_to_bridge.rs`: data + code → reconstrucción → ambos shards en el Engine.
- Sin bind/send UDP (el plan de forward se reporta, no se envía).

**Fuera de alcance:** gossip, leader TPU, Blockstore, reenvío real.

**Criterio de cierre:** integración documentada; métricas Turbine comprobadas en el test.

**Hecho:** cable ingest→bridge; error `TurbineIngestFailed`.

---

## Fase 6 — Flush, recuperación y backpressure con disco real

**Estado:** completa (`cargo test` + `clippy -D warnings`).

**Objetivo de aprendizaje:** `needs_flush` / `schedule_flush`, recovery al reabrir, cola llena tipada.

**Alcance**

- `auto_schedule_flush` en config; `maybe_schedule_flush` tras encolar.
- APIs: `needs_flush`, `schedule_flush`, `wait_flush`, `flush`.
- `submit_record`: cola llena → intento de flush → `PipelineStall`.
- `apply_backpressure` documenta la política.
- Tests: `tests/flush_recovery.rs` (recovery, memtable full, saturación).

**Fuera de alcance:** compactación SST.

**Criterio de cierre:** re-`open` ve datos; test de cola llena; clippy limpio.

**Hecho:** política de flush + recovery + stall tipado.

---

## Fase 7 — Binario demo y documentación de recorrido

**Estado:** pendiente.

**Objetivo de aprendizaje:** un `main` que se pueda correr en laboratorio y un README corto “cómo seguir el flujo”.

**Alcance**

- CLI mínima (dirs, bind addr, flags).
- README de aprendizaje (pasos, limitaciones vs Solana real).

**Fuera de alcance:** cluster multi-nodo de producción.

**Criterio de cierre:** demo documentada; `cargo clippy` limpio.

---

## Fase 8 — Bench e2e (criterion)

**Estado:** pendiente.

**Objetivo de aprendizaje:** medir el camino paquete/registro → `put` (latencia / throughput de laboratorio), sin confundirlo con Gbps de mainnet.

**Alcance**

- `benches/e2e_throughput.rs`.
- Línea base anotada en este archivo al cerrar.

**Fuera de alcance:** optimizar nvme o FEC dentro de este crate (se sube issue/fase en el repo dueño).

**Criterio de cierre:** bench ejecutable; números anotados con hardware/FS usados.

---

## Fuera de alcance del proyecto (salvo nueva autorización)

- Blockstore / ledger completo de shreds.
- Replay y ejecución de transacciones (Banking / SVM).
- Gossip, repair, voting, leader schedule.
- Compactación LSM avanzada (pertenece a `nvme-state-db`).
- Reimplementar Turbine o el WAL en este repo.

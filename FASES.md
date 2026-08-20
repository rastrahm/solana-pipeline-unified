# Plan de fases — solana-pipeline-unified

Proyecto de **aprendizaje**: orquestador que une `mini-solana-turbine` (red + shreds + FEC + reenvío) con `nvme-state-db` (única persistencia a disco).

Cada fase requiere autorización explícita (`autoriza fase N`) antes de empezar. Al cerrar una fase se explica qué se hizo, qué se aprendió y se pide permiso para la siguiente.

Estado del repositorio al crear este archivo: **Fase 0 en curso** (plan + reglas). Sin `Cargo.toml` ni `src/` todavía.

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

**Estado:** pendiente (requiere `autoriza fase 1`).

**Objetivo de aprendizaje:** crate lib+bin, `thiserror`, módulos públicos documentados sin lógica aún.

**Alcance**

- `Cargo.toml` (edition 2021+, `thiserror`, `anyhow` solo en el binario).
- Aún **sin** path deps a turbine/nvme (eso es fase 2), o stubs de features documentados — se fijará al autorizar.
- `src/lib.rs`, `src/main.rs` (main mínimo), `src/error.rs`.
- Stubs: `src/pipeline/{mod,bridge,orchestrator}.rs` con tipos/funciones documentadas y `todo!` / `unimplemented` acotados solo si hace falta compilar tests de humo (preferir stubs que compilen sin panics en tests).
- Test de humo: el crate compila; `Error` implementa `std::error::Error`.

**Fuera de alcance:** UDP, FEC, `Engine`, colas reales, I/O.

**Criterio de cierre:** `cargo test` y `cargo clippy -D warnings` verdes.

---

## Fase 2 — Dependencias path y humo de integración de tipos

**Estado:** pendiente.

**Objetivo de aprendizaje:** enlazar crates hermanos por `path`, features de turbine (`uring`/`simd`), importar solo API pública.

**Alcance**

- `Cargo.toml`: `mini-solana-turbine` y `nvme-state-db` vía `path = "..."`.
- Tests que construyen/tocan símbolos públicos mínimos (p. ej. tipos reexportados o `EngineOptions` / `Error` de cada lado) sin pipeline completo.
- Ajustar features si el entorno no tiene `io_uring`.

**Fuera de alcance:** loop de red, `put` de producción, bridge con backpressure.

**Criterio de cierre:** `cargo test` enlaza ambos crates; clippy limpio.

---

## Fase 3 — Orchestrator: ciclo de vida

**Estado:** pendiente.

**Objetivo de aprendizaje:** arrancar y apagar ordenadamente lo que ya existe en los otros crates (p. ej. abrir `Engine`, preparar estructuras de turbine).

**Alcance**

- `orchestrator.rs`: configuración (rutas de datos, capacidad de colas), `start` / `shutdown`.
- Sin ingestión UDP completa todavía si la fase 4/5 la cubren; puede ser wiring en memoria.

**Fuera de alcance:** benchmark e2e, persistencia de shreds crudos como Blockstore.

**Criterio de cierre:** tests de arranque/parada; sin fugas obvias de hilos del flush de nvme al dropear.

---

## Fase 4 — Bridge: de resultado de ingest a `Engine::put`

**Estado:** pendiente.

**Objetivo de aprendizaje:** frontera orquestador ↔ storage. Mapear un **registro de aprendizaje** (clave/valor derivados del shred o shard reconstruido) hacia `put`, sin reimplementar WAL.

**Alcance**

- `bridge.rs`: cola acotada (`crossbeam-channel`) + consumidor que llama `Engine::put`.
- Política de saturación: error `thiserror` (no bloquear forever el productor de red).
- Convención documentada de clave/valor (educativa; no es el encoding real de Solana AccountsDB).

**Fuera de alcance:** ejecución de transacciones, repair, forks.

**Criterio de cierre:** test: N ítems encolados → visibles con `Engine::get` tras flush si la fase lo requiere.

---

## Fase 5 — Cablear ingestión (turbine) al bridge

**Estado:** pendiente.

**Objetivo de aprendizaje:** un tramo TVU-like: recv/ingest → (opcional forward) → bridge → nvme.

**Alcance**

- Usar `UdpIngress` / `Pipeline` / `slot_queue` según features.
- Un camino feliz en test (loopback o bytes sintéticos vía `ingest_bytes` si UDP complica el CI).

**Fuera de alcance:** gossip, leader TPU completo, Blockstore.

**Criterio de cierre:** test de integración documentado; métricas mínimas opcionales.

---

## Fase 6 — Flush, recuperación y backpressure con disco real

**Estado:** pendiente.

**Objetivo de aprendizaje:** `needs_flush` / `schedule_flush`, qué pasa si el disco es más lento que la red, y recovery al reabrir `Engine`.

**Alcance**

- Política de flush en el orquestador (cuándo llamar).
- Tests con directorio temporal.
- Errores de backpressure / stall tipados.

**Fuera de alcance:** compactación de SST (sigue en nvme como fuera de alcance).

**Criterio de cierre:** reinicio del proceso (re-`open`) ve datos esperados; test de cola llena.

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

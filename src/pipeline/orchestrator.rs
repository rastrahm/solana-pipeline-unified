//! Orchestrator: ciclo de vida del pipeline (fases 3–5).
//!
//! Abre el [`nvme_state_db::Engine`], arranca el [`crate::pipeline::Bridge`]
//! y, con feature `simd`, ingeriere shreds vía [`mini_solana_turbine::Pipeline`]
//! en memoria (bytes sintéticos). El envío UDP de reenvío queda fuera de esta
//! fase; el plan de forward sí se calcula y se reporta.

use crate::error::Error;
use crate::pipeline::bridge::Bridge;
#[cfg(feature = "simd")]
use crate::pipeline::bridge::make_learn_key;
use crate::pipeline::outcome::IngestOutcome;
use mini_solana_turbine::{Node, NodeId, Stake};
use nvme_state_db::{Engine, EngineOptions};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "simd")]
use mini_solana_turbine::{pipeline::MAX_SHARDS, turbine::tree::build, Pipeline};

/// Capacidad por defecto de la cola del bridge.
pub const DEFAULT_QUEUE_CAPACITY: usize = 128;

/// Configuración de arranque del orquestador.
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Directorio de datos del [`Engine`] (WAL / SST).
    pub data_dir: PathBuf,
    /// Capacidad de la cola acotada hacia el bridge.
    pub queue_capacity: usize,
    /// Capacidad de MemTable en bytes; `None` = default del motor.
    pub mem_capacity_bytes: Option<usize>,
    /// Identidad local en el árbol Turbine de aprendizaje.
    pub self_id: NodeId,
    /// Stake local (solo ordena el árbol; no es economía real).
    pub self_stake: Stake,
    /// Dirección anunciada para reenvío futuro; no se hace bind en fase 5.
    pub self_addr: SocketAddr,
}

impl OrchestratorConfig {
    /// Purpose: config mínima para tests o demos locales.
    ///
    /// Inputs:
    /// - `data_dir`: path del motor de estado.
    /// - `self_addr`: addr lógica del nodo (p. ej. `127.0.0.1:0`).
    ///
    /// Returns: config con cola por defecto e id/stake fijos de aprendizaje.
    pub fn local(data_dir: impl Into<PathBuf>, self_addr: SocketAddr) -> Self {
        Self {
            data_dir: data_dir.into(),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            mem_capacity_bytes: None,
            self_id: NodeId::new(1),
            self_stake: Stake::new(100),
            self_addr,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    Running,
}

/// Coordina Engine, Bridge y (si `simd`) Pipeline.
pub struct Orchestrator {
    config: OrchestratorConfig,
    phase: Phase,
    engine: Option<Arc<Engine>>,
    bridge: Option<Bridge>,
    #[cfg(feature = "simd")]
    pipeline: Option<Pipeline>,
}

impl Orchestrator {
    /// Purpose: crea un orquestador en reposo (sin abrir disco ni Turbine).
    ///
    /// Inputs: `config` — rutas y parámetros de aprendizaje.
    ///
    /// Returns: instancia en fase Idle.
    pub fn new(config: OrchestratorConfig) -> Self {
        Self {
            config,
            phase: Phase::Idle,
            engine: None,
            bridge: None,
            #[cfg(feature = "simd")]
            pipeline: None,
        }
    }

    /// Purpose: ¿está en ejecución tras un `start` exitoso?
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: `true` si hay recursos vivos.
    pub fn is_running(&self) -> bool {
        self.phase == Phase::Running
    }

    /// Purpose: acceso al motor abierto (solo mientras corre).
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: referencia al [`Engine`], o `None` si Idle.
    pub fn engine(&self) -> Option<&Engine> {
        self.engine.as_deref()
    }

    /// Purpose: capacidad de cola configurada.
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: enteros ≥ 1 según config.
    pub fn queue_capacity(&self) -> usize {
        self.config.queue_capacity.max(1)
    }

    /// Purpose: encola un registro hacia el bridge → `Engine::put`.
    ///
    /// Inputs: `key` / `value` (ver convención en `bridge`).
    ///
    /// Returns: errores del bridge, o estado inválido si no está Running.
    pub fn submit_record(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        match &self.bridge {
            Some(bridge) => bridge.submit_record(key, value),
            None => Err(Error::InvalidOrchestratorState),
        }
    }

    /// Purpose: abre el motor, arranca el bridge y prepara Turbine en memoria.
    ///
    /// Inputs: ninguno (usa la config del constructor).
    ///
    /// Returns: `Ok` si pasó a Running; errores de open/spawn/Turbine si falla.
    pub fn start(&mut self) -> Result<(), Error> {
        if self.phase == Phase::Running {
            return Err(Error::InvalidOrchestratorState);
        }

        let opts = match self.config.mem_capacity_bytes {
            Some(n) => EngineOptions {
                mem_capacity_bytes: n,
            },
            None => EngineOptions::default(),
        };
        let engine = Arc::new(
            Engine::open_with(&self.config.data_dir, opts).map_err(|_| Error::EngineOpenFailed)?,
        );

        let bridge = Bridge::start(Arc::clone(&engine), self.queue_capacity())?;

        #[cfg(feature = "simd")]
        let pipeline = {
            let node = Node::new(
                self.config.self_id,
                self.config.self_stake,
                self.config.self_addr,
            );
            let tree = build(&[node], 2).map_err(|_| Error::TurbineSetupFailed)?;
            Pipeline::with_defaults(tree, self.config.self_id)
                .map_err(|_| Error::TurbineSetupFailed)?
        };

        #[cfg(not(feature = "simd"))]
        {
            let _ = Node::new(
                self.config.self_id,
                self.config.self_stake,
                self.config.self_addr,
            );
        }

        self.engine = Some(engine);
        self.bridge = Some(bridge);
        #[cfg(feature = "simd")]
        {
            self.pipeline = Some(pipeline);
        }
        self.phase = Phase::Running;
        Ok(())
    }

    /// Purpose: cierra el bridge (drena cola), flush del motor y suelta recursos.
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: `Ok` si Idle (no-op) o tras apagar; error de bridge/flush si falla.
    pub fn shutdown(&mut self) -> Result<(), Error> {
        if self.phase == Phase::Idle {
            return Ok(());
        }

        if let Some(bridge) = self.bridge.take() {
            bridge.shutdown()?;
        }

        if let Some(engine) = self.engine.as_ref() {
            engine.flush().map_err(|_| Error::PersistFailed)?;
        }

        #[cfg(feature = "simd")]
        {
            self.pipeline = None;
        }
        self.engine = None;
        self.phase = Phase::Idle;
        Ok(())
    }

    /// Purpose: pipeline Turbine preparado (solo con feature `simd`).
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: referencia si Running y `simd`.
    #[cfg(feature = "simd")]
    pub fn pipeline(&self) -> Option<&Pipeline> {
        self.pipeline.as_ref()
    }

    /// Purpose: ingiere un shred en bytes (camino de laboratorio, sin UDP).
    ///
    /// Tras un ingest OK, encola al bridge todos los data shards ya presentes
    /// en el scratch (`learn/v1/shard/N`). El reenvío UDP no se hace aquí; solo
    /// se informa cuántos destinos calcularía Turbine.
    ///
    /// Inputs: `bytes` — paquete shred completo (mismo contrato que turbine).
    ///
    /// Returns: [`IngestOutcome`], o error de estado / Turbine / bridge.
    #[cfg(feature = "simd")]
    pub fn ingest_bytes(&mut self, bytes: &[u8]) -> Result<IngestOutcome, Error> {
        if self.phase != Phase::Running {
            return Err(Error::InvalidOrchestratorState);
        }
        let result = {
            let pipe = self
                .pipeline
                .as_mut()
                .ok_or(Error::InvalidOrchestratorState)?;
            pipe.ingest_bytes(bytes)
                .map_err(|_| Error::TurbineIngestFailed)?
        };
        let records_submitted = self.persist_available_shards()?;
        Ok(IngestOutcome {
            reconstructed: result.reconstructed(),
            forward_dest_count: result.forward().len(),
            records_submitted,
        })
    }

    /// Purpose: encola cada `original_shard` disponible hacia el bridge.
    ///
    /// Inputs: ninguno (usa pipeline + bridge vivos).
    ///
    /// Returns: cantidad de `put` encolados.
    #[cfg(feature = "simd")]
    fn persist_available_shards(&self) -> Result<usize, Error> {
        let pipe = self
            .pipeline
            .as_ref()
            .ok_or(Error::InvalidOrchestratorState)?;
        let bridge = self
            .bridge
            .as_ref()
            .ok_or(Error::InvalidOrchestratorState)?;
        let mut submitted = 0usize;
        for index in 0..MAX_SHARDS {
            let Ok(shard) = pipe.original_shard(index) else {
                continue;
            };
            let key = shard_learn_key(index)?;
            bridge.submit_record(&key, shard)?;
            submitted += 1;
        }
        Ok(submitted)
    }
}

/// Purpose: clave educativa para el data shard `index` (`learn/v1/shard/N`).
///
/// Inputs: `index` — `0..99`.
///
/// Returns: clave, o [`Error::EmptyRecordKey`] si el índice no cabe.
#[cfg(feature = "simd")]
fn shard_learn_key(index: usize) -> Result<Vec<u8>, Error> {
    if index >= 100 {
        return Err(Error::EmptyRecordKey);
    }
    let mut suffix = [0u8; 10];
    let prefix = b"shard/";
    suffix[..prefix.len()].copy_from_slice(prefix);
    let len = if index >= 10 {
        suffix[prefix.len()] = b'0' + (index / 10) as u8;
        suffix[prefix.len() + 1] = b'0' + (index % 10) as u8;
        prefix.len() + 2
    } else {
        suffix[prefix.len()] = b'0' + index as u8;
        prefix.len() + 1
    };
    make_learn_key(&suffix[..len])
}

#[cfg(test)]
mod tests {
    use super::{Orchestrator, OrchestratorConfig};
    use crate::error::Error;
    use crate::pipeline::bridge::make_learn_key;
    use std::net::SocketAddr;

    fn cfg(dir: &std::path::Path) -> OrchestratorConfig {
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
        OrchestratorConfig::local(dir, addr)
    }

    /// Purpose: start → running → shutdown → idle.
    /// Inputs: tempdir.
    /// Returns: panics si el ciclo falla.
    #[test]
    fn start_then_shutdown_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut orch = Orchestrator::new(cfg(dir.path()));
        assert!(!orch.is_running());
        assert!(orch.engine().is_none());

        orch.start().expect("start");
        assert!(orch.is_running());
        assert!(orch.engine().is_some());
        #[cfg(feature = "simd")]
        assert!(orch.pipeline().is_some());

        orch.shutdown().expect("shutdown");
        assert!(!orch.is_running());
        assert!(orch.engine().is_none());
    }

    /// Purpose: dos `start` seguidos no son válidos.
    /// Inputs: tempdir.
    /// Returns: panics si el segundo start no es inválido.
    #[test]
    fn double_start_is_invalid() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut orch = Orchestrator::new(cfg(dir.path()));
        orch.start().expect("start");
        assert_eq!(orch.start(), Err(Error::InvalidOrchestratorState));
        orch.shutdown().expect("shutdown");
    }

    /// Purpose: `shutdown` en Idle es no-op seguro.
    /// Inputs: tempdir sin start.
    /// Returns: panics si shutdown falla.
    #[test]
    fn shutdown_while_idle_is_ok() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut orch = Orchestrator::new(cfg(dir.path()));
        orch.shutdown().expect("shutdown idle");
    }

    /// Purpose: tras apagar se puede volver a arrancar.
    /// Inputs: tempdir.
    /// Returns: panics si el segundo ciclo falla.
    #[test]
    fn restart_after_shutdown() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut orch = Orchestrator::new(cfg(dir.path()));
        orch.start().expect("start1");
        orch.shutdown().expect("stop1");
        orch.start().expect("start2");
        assert!(orch.is_running());
        orch.shutdown().expect("stop2");
    }

    /// Purpose: la capacidad de cola queda guardada.
    /// Inputs: config con capacidad 8.
    /// Returns: panics si no coincide.
    #[test]
    fn stores_queue_capacity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut c = cfg(dir.path());
        c.queue_capacity = 8;
        let orch = Orchestrator::new(c);
        assert_eq!(orch.queue_capacity(), 8);
    }

    /// Purpose: submit vía orquestador llega al Engine.
    /// Inputs: un registro `learn/v1/…`.
    /// Returns: panics si `get` no ve el valor.
    #[test]
    fn submit_record_through_orchestrator() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut orch = Orchestrator::new(cfg(dir.path()));
        orch.start().expect("start");

        let key = make_learn_key(b"demo").expect("key");
        orch.submit_record(&key, b"hola").expect("submit");
        orch.shutdown().expect("shutdown");

        let engine = nvme_state_db::Engine::open(dir.path()).expect("reopen");
        let got = engine.get(&key).expect("get");
        assert_eq!(got.as_bytes(), Some(b"hola".as_ref()));
    }
}

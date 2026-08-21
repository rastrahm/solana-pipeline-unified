//! Orchestrator: ciclo de vida del pipeline (fase 3).
//!
//! Abre el [`nvme_state_db::Engine`] (única persistencia) y, con feature `simd`,
//! prepara un [`mini_solana_turbine::Pipeline`] en memoria. Todavía no hay UDP
//! ni bridge con cola (fases 4–5).

use crate::error::Error;
use mini_solana_turbine::{Node, NodeId, Stake};
use nvme_state_db::{Engine, EngineOptions};
use std::net::SocketAddr;
use std::path::PathBuf;

#[cfg(feature = "simd")]
use mini_solana_turbine::{turbine::tree::build, Pipeline};

/// Capacidad por defecto de la cola del bridge (se usa en fase 4).
pub const DEFAULT_QUEUE_CAPACITY: usize = 128;

/// Configuración de arranque del orquestador.
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Directorio de datos del [`Engine`] (WAL / SST).
    pub data_dir: PathBuf,
    /// Capacidad de la cola acotada hacia el bridge (fase 4); se guarda ya.
    pub queue_capacity: usize,
    /// Capacidad de MemTable en bytes; `None` = default del motor.
    pub mem_capacity_bytes: Option<usize>,
    /// Identidad local en el árbol Turbine de aprendizaje.
    pub self_id: NodeId,
    /// Stake local (solo ordena el árbol; no es economía real).
    pub self_stake: Stake,
    /// Dirección anunciada para reenvío futuro (fase 5); no se hace bind aquí.
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

/// Coordina arranque y apagado de Engine (+ Pipeline si `simd`).
pub struct Orchestrator {
    config: OrchestratorConfig,
    phase: Phase,
    engine: Option<Engine>,
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
        self.engine.as_ref()
    }

    /// Purpose: capacidad de cola configurada (para el bridge en fase 4).
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: enteros ≥ 1 según config.
    pub fn queue_capacity(&self) -> usize {
        self.config.queue_capacity.max(1)
    }

    /// Purpose: abre el motor y prepara Turbine en memoria.
    ///
    /// Inputs: ninguno (usa la config del constructor).
    ///
    /// Returns:
    /// - `Ok(())` si pasó a Running.
    /// - [`Error::InvalidOrchestratorState`] si ya estaba Running.
    /// - [`Error::EngineOpenFailed`] / [`Error::TurbineSetupFailed`] si falla el setup.
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
        let engine = Engine::open_with(&self.config.data_dir, opts)
            .map_err(|_| Error::EngineOpenFailed)?;

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

        // Sin feature `simd` aún usamos Node para no dejar la config muerta
        // y documentar el wiring; el árbol real llega con `simd`.
        #[cfg(not(feature = "simd"))]
        {
            let _ = Node::new(
                self.config.self_id,
                self.config.self_stake,
                self.config.self_addr,
            );
        }

        self.engine = Some(engine);
        #[cfg(feature = "simd")]
        {
            self.pipeline = Some(pipeline);
        }
        self.phase = Phase::Running;
        Ok(())
    }

    /// Purpose: flush del motor (si aplica) y suelta recursos; el `Drop` de
    /// [`Engine`] espera al hilo de flush interno.
    ///
    /// Inputs: ninguno.
    ///
    /// Returns:
    /// - `Ok(())` si Idle (no-op) o tras apagar Running.
    /// - [`Error::PersistFailed`] si `flush` del motor falla.
    pub fn shutdown(&mut self) -> Result<(), Error> {
        if self.phase == Phase::Idle {
            return Ok(());
        }

        if let Some(engine) = self.engine.as_ref() {
            engine.flush().map_err(|_| Error::PersistFailed)?;
        }

        #[cfg(feature = "simd")]
        {
            self.pipeline = None;
        }
        // Drop del Engine hace join del worker `nvme-sst-flush`.
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
}

#[cfg(test)]
mod tests {
    use super::{Orchestrator, OrchestratorConfig};
    use crate::error::Error;
    use std::net::SocketAddr;

    fn cfg(dir: &std::path::Path) -> OrchestratorConfig {
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
        OrchestratorConfig::local(dir, addr)
    }

    /// Purpose: start → running → shutdown → idle, sin dejar el motor abierto.
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
    /// Returns: panics si el segundo start no es `InvalidOrchestratorState`.
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

    /// Purpose: tras apagar se puede volver a arrancar (reopen del Engine).
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

    /// Purpose: la capacidad de cola queda guardada para la fase 4.
    /// Inputs: config con capacidad 8.
    /// Returns: panics si `queue_capacity` no respeta el valor.
    #[test]
    fn stores_queue_capacity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut c = cfg(dir.path());
        c.queue_capacity = 8;
        let orch = Orchestrator::new(c);
        assert_eq!(orch.queue_capacity(), 8);
    }
}

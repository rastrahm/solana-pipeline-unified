//! Orquestador de aprendizaje: cablea red/FEC (`mini-solana-turbine`) con
//! persistencia (`nvme-state-db`).
//!
//! Fase 3: ciclo de vida del [`Orchestrator`] (abre `Engine`, prepara Turbine
//! en memoria). El bridge con cola llega en la fase 4.

#![deny(missing_docs)]

pub mod error;
pub mod pipeline;

pub use error::Error;
pub use pipeline::{Bridge, Orchestrator, OrchestratorConfig, DEFAULT_QUEUE_CAPACITY};

#[cfg(test)]
mod tests {
    use super::{Bridge, Error, Orchestrator, OrchestratorConfig};
    use std::net::SocketAddr;

    /// Purpose: el bridge sigue siendo stub; el orchestrator ya arranca de verdad.
    /// Inputs: tempdir.
    /// Returns: panics si el bridge no es stub o el ciclo de vida falla.
    #[test]
    fn bridge_still_stub_orchestrator_runs() {
        let bridge = Bridge::new();
        assert!(matches!(
            bridge.submit_record(b"k", b"v"),
            Err(Error::Unimplemented { module: "bridge" })
        ));

        let dir = tempfile::tempdir().expect("tempdir");
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
        let mut orch = Orchestrator::new(OrchestratorConfig::local(dir.path(), addr));
        orch.start().expect("start");
        assert!(orch.is_running());
        orch.shutdown().expect("shutdown");
    }

    /// Purpose: `Error` es usable como `std::error::Error`.
    /// Inputs: ninguno.
    /// Returns: panics si no se puede tratar como trait object de error.
    #[test]
    fn error_implements_std_error() {
        let err: Error = Error::EngineOpenFailed;
        let _: &dyn std::error::Error = &err;
        assert!(!err.to_string().is_empty());
    }
}

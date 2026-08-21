//! Orquestador de aprendizaje: cablea red/FEC (`mini-solana-turbine`) con
//! persistencia (`nvme-state-db`).
//!
//! Fase 4: [`Bridge`] con cola acotada hacia `Engine::put`. La ingestión UDP
//! llega en la fase 5.

#![deny(missing_docs)]

pub mod error;
pub mod pipeline;

pub use error::Error;
pub use pipeline::{
    bridge_channel, make_learn_key, Bridge, BridgeReceiver, BridgeSender, Orchestrator,
    OrchestratorConfig, StateRecord, DEFAULT_QUEUE_CAPACITY, LEARN_KEY_PREFIX,
};

#[cfg(test)]
mod tests {
    use super::{make_learn_key, Error, Orchestrator, OrchestratorConfig};
    use std::net::SocketAddr;

    /// Purpose: orquestador encola y persiste un registro de aprendizaje.
    /// Inputs: tempdir.
    /// Returns: panics si el valor no sobrevive al shutdown.
    #[test]
    fn orchestrator_persists_learn_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
        let mut orch = Orchestrator::new(OrchestratorConfig::local(dir.path(), addr));
        orch.start().expect("start");
        let key = make_learn_key(b"lib").expect("key");
        orch.submit_record(&key, b"ok").expect("submit");
        orch.shutdown().expect("shutdown");

        let engine = nvme_state_db::Engine::open(dir.path()).expect("reopen");
        assert_eq!(
            engine.get(&key).expect("get").as_bytes(),
            Some(b"ok".as_ref())
        );
    }

    /// Purpose: `Error` es usable como `std::error::Error`.
    /// Inputs: ninguno.
    /// Returns: panics si no se puede tratar como trait object de error.
    #[test]
    fn error_implements_std_error() {
        let err: Error = Error::BridgeSaturated;
        let _: &dyn std::error::Error = &err;
        assert!(!err.to_string().is_empty());
    }
}

//! Orquestador de aprendizaje: cablea red/FEC (`mini-solana-turbine`) con
//! persistencia (`nvme-state-db`).
//!
//! Fase 2: deps `path` a los crates hermanos y tests iniciales de tipos públicos.
//! El cableado real (orchestrator / bridge) llega en fases posteriores.

#![deny(missing_docs)]

pub mod error;
pub mod pipeline;

pub use error::Error;
pub use pipeline::{Bridge, Orchestrator};

#[cfg(test)]
mod tests {
    use super::{Bridge, Error, Orchestrator};

    /// Purpose: comprueba que la API pública reexportada compila y tipa.
    /// Inputs: ninguno.
    /// Returns: panics si los stubs no devuelven `Unimplemented`.
    #[test]
    fn public_api_stubs_return_unimplemented() {
        let bridge = Bridge::new();
        assert!(matches!(
            bridge.submit_record(b"k", b"v"),
            Err(Error::Unimplemented { module: "bridge" })
        ));

        let orch = Orchestrator::new();
        assert!(matches!(
            orch.start(),
            Err(Error::Unimplemented {
                module: "orchestrator"
            })
        ));
        assert!(matches!(
            orch.shutdown(),
            Err(Error::Unimplemented {
                module: "orchestrator"
            })
        ));
    }

    /// Purpose: `Error` es usable como `std::error::Error`.
    /// Inputs: ninguno.
    /// Returns: panics si no se puede tratar como trait object de error.
    #[test]
    fn error_implements_std_error() {
        let err: Error = Error::Unimplemented { module: "bridge" };
        let _: &dyn std::error::Error = &err;
        assert!(!err.to_string().is_empty());
    }
}

//! Orchestrator: ciclo de vida del pipeline (fase 3+).
//!
//! En fase 1 solo expone `start` / `shutdown` como stubs documentados.

use crate::error::Error;

/// Coordina arranque y apagado de las piezas del pipeline unificado.
///
/// No abre `Engine` ni sockets hasta fases posteriores.
#[derive(Debug, Default)]
pub struct Orchestrator;

impl Orchestrator {
    /// Purpose: construye un orquestador vacío (stub).
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: instancia sin recursos adquiridos.
    pub fn new() -> Self {
        Self
    }

    /// Purpose: inicia workers / recursos del pipeline.
    ///
    /// Inputs: ninguno (la config llegará en fases posteriores).
    ///
    /// Returns: [`Error::Unimplemented`] hasta la fase de ciclo de vida.
    pub fn start(&self) -> Result<(), Error> {
        Err(Error::Unimplemented {
            module: "orchestrator",
        })
    }

    /// Purpose: detiene el pipeline de forma ordenada.
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: [`Error::Unimplemented`] hasta la fase de ciclo de vida.
    pub fn shutdown(&self) -> Result<(), Error> {
        Err(Error::Unimplemented {
            module: "orchestrator",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Orchestrator;
    use crate::error::Error;

    /// Purpose: stubs de ciclo de vida no paniquean.
    /// Inputs: ninguno.
    /// Returns: panics si el error no nombra `orchestrator`.
    #[test]
    fn start_and_shutdown_are_stubs() {
        let orch = Orchestrator::new();
        assert_eq!(
            orch.start(),
            Err(Error::Unimplemented {
                module: "orchestrator"
            })
        );
        assert_eq!(
            orch.shutdown(),
            Err(Error::Unimplemented {
                module: "orchestrator"
            })
        );
    }
}

//! Errores del orquestador (`thiserror`).
//!
//! La biblioteca expone un [`Error`] enumerable. El binario (`main.rs`) usa
//! `anyhow` solo para contexto de arranque.

use thiserror::Error;

/// Fallos recuperables del pipeline unificado.
///
/// Las variantes se amplían en fases posteriores (colas, motor, stalls).
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// El módulo todavía es un stub de fase 1.
    #[error("no implementado: {module}")]
    Unimplemented {
        /// Nombre del módulo stub (`bridge`, `orchestrator`, …).
        module: &'static str,
    },

    /// La cola acotada del bridge está llena (fase 4+).
    #[error("cola del bridge saturada")]
    BridgeSaturated,

    /// No se pudo crear el hilo consumidor del bridge.
    #[error("no se pudo crear el hilo del bridge")]
    BridgeSpawnFailed,

    /// Fallo al persistir en el motor de estado (fase 4+).
    #[error("fallo al persistir en el motor de estado")]
    PersistFailed,

    /// Clave de registro vacía (no se encola).
    #[error("clave de registro vacía")]
    EmptyRecordKey,

    /// El orquestador no está en el estado esperado para la operación (fase 3+).
    #[error("estado del orquestador inválido para esta operación")]
    InvalidOrchestratorState,

    /// No se pudo abrir o inicializar `nvme-state-db::Engine` (fase 3+).
    #[error("no se pudo abrir el motor de estado")]
    EngineOpenFailed,

    /// Falló la preparación en memoria del pipeline Turbine (fase 3+).
    #[error("no se pudo preparar el pipeline Turbine")]
    TurbineSetupFailed,

    /// Falló el parseo / FEC al ingerir un shred (fase 5+).
    #[error("falló la ingestión Turbine")]
    TurbineIngestFailed,

    /// El pipeline se detuvo por backpressure o stall prolongado (fase 6+).
    #[error("pipeline en stall / backpressure")]
    PipelineStall,
}

#[cfg(test)]
mod tests {
    use super::Error;

    /// Purpose: variantes placeholder son `Copy` y muestran mensaje.
    /// Inputs: ninguno.
    /// Returns: panics si el mensaje está vacío.
    #[test]
    fn unimplemented_displays_module() {
        let err = Error::Unimplemented { module: "bridge" };
        assert!(err.to_string().contains("bridge"));
        let _ = err;
    }
}

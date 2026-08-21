//! Resultado de una ingestión orquestada (fase 5+).

/// Resumen de una ingestión: FEC + lo encolado al bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestOutcome {
    /// Data shards que el FEC acaba de rellenar (0 si aún no alcanza).
    pub reconstructed: usize,
    /// Destinos lógicos de reenvío Turbine (sin enviar UDP en esta fase).
    pub forward_dest_count: usize,
    /// Cuántos originales presentes se encolaron hacia `Engine::put`.
    pub records_submitted: usize,
}

//! Bridge: cola / entrega hacia `nvme-state-db::Engine` (fase 4+).
//!
//! En fase 1 solo existe el tipo y un método stub que no realiza I/O.

use crate::error::Error;

/// Canal lógico entre ingestión (turbine) y persistencia (nvme).
///
/// Hoy no posee cola ni `Engine`; las fases siguientes lo cablean.
#[derive(Debug, Default)]
pub struct Bridge;

impl Bridge {
    /// Purpose: construye un bridge vacío (stub).
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: instancia sin estado persistente.
    pub fn new() -> Self {
        Self
    }

    /// Purpose: encola o escribe un registro clave/valor hacia el motor.
    ///
    /// Inputs:
    /// - `_key`: clave del registro de aprendizaje.
    /// - `_value`: valor asociado.
    ///
    /// Returns: [`Error::Unimplemented`] hasta la fase del bridge real.
    pub fn submit_record(&self, _key: &[u8], _value: &[u8]) -> Result<(), Error> {
        Err(Error::Unimplemented { module: "bridge" })
    }
}

#[cfg(test)]
mod tests {
    use super::Bridge;
    use crate::error::Error;

    /// Purpose: stub de submit no paniquea y tipa el error.
    /// Inputs: ninguno.
    /// Returns: panics si no es `Unimplemented { bridge }`.
    #[test]
    fn submit_record_is_stub() {
        let bridge = Bridge::new();
        assert_eq!(
            bridge.submit_record(b"a", b"b"),
            Err(Error::Unimplemented { module: "bridge" })
        );
    }
}

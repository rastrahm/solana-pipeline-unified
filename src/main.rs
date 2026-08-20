//! Punto de entrada del binario `solana-pipeline-unified`.
//!
//! `anyhow::Result` es el error de **aplicación** (contexto en el `main`).
//! La biblioteca expone [`solana_pipeline_unified::Error`] (`thiserror`).

use anyhow::Result;

/// Purpose: entrada mínima de fase 1 (sin I/O ni orquestación real).
///
/// Inputs: ninguno (argv se usará en fases posteriores).
///
/// Returns: `Ok(())` siempre en esta fase.
fn main() -> Result<()> {
    Ok(())
}

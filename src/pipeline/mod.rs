//! Subsistema de orquestación (stubs en fase 1).
//!
//! - [`bridge`]: entrega de registros hacia el motor de estado (futuro).
//! - [`orchestrator`]: arranque y apagado del pipeline (futuro).

pub mod bridge;
pub mod orchestrator;

pub use bridge::Bridge;
pub use orchestrator::Orchestrator;

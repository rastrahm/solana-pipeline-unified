//! Subsistema de orquestación.
//!
//! - [`bridge`]: entrega hacia el motor (stub hasta fase 4).
//! - [`orchestrator`]: arranque y apagado (fase 3).

pub mod bridge;
pub mod orchestrator;

pub use bridge::Bridge;
pub use orchestrator::{Orchestrator, OrchestratorConfig, DEFAULT_QUEUE_CAPACITY};

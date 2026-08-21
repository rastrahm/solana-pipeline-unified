//! Subsistema de orquestación.
//!
//! - [`bridge`]: cola acotada → `Engine::put` (fase 4).
//! - [`orchestrator`]: ciclo de vida + ingest → bridge (fases 3–5).
//! - [`outcome`]: resultado tipado de una ingestión.

pub mod bridge;
pub mod orchestrator;
pub mod outcome;

pub use bridge::{
    bridge_channel, make_learn_key, Bridge, BridgeReceiver, BridgeSender, StateRecord,
    LEARN_KEY_PREFIX,
};
pub use orchestrator::{Orchestrator, OrchestratorConfig, DEFAULT_QUEUE_CAPACITY};
pub use outcome::IngestOutcome;

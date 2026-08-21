//! Subsistema de orquestación.
//!
//! - [`bridge`]: cola acotada → `Engine::put` (fase 4).
//! - [`orchestrator`]: arranque / apagado (fases 3–4).

pub mod bridge;
pub mod orchestrator;

pub use bridge::{
    bridge_channel, make_learn_key, Bridge, BridgeReceiver, BridgeSender, StateRecord,
    LEARN_KEY_PREFIX,
};
pub use orchestrator::{Orchestrator, OrchestratorConfig, DEFAULT_QUEUE_CAPACITY};

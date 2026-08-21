//! Fase 6: flush automático, recuperación al reabrir, cola saturada.

use solana_pipeline_unified::{
    bridge_channel, make_learn_key, Error, Orchestrator, OrchestratorConfig,
};
use std::net::SocketAddr;

fn cfg(dir: &std::path::Path) -> OrchestratorConfig {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
    OrchestratorConfig::local(dir, addr)
}

/// Purpose: datos sobreviven a shutdown + nuevo `Engine::open` (recovery WAL/SST).
/// Inputs: tempdir compartido entre dos ciclos de vida.
/// Returns: panics si el valor no aparece tras reabrir.
#[test]
fn recovery_after_orchestrator_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = make_learn_key(b"recover").expect("key");

    {
        let mut orch = Orchestrator::new(cfg(dir.path()));
        orch.start().expect("start");
        orch.submit_record(&key, b"persistente").expect("submit");
        orch.shutdown().expect("shutdown");
    }

    let engine = nvme_state_db::Engine::open(dir.path()).expect("reopen");
    assert_eq!(
        engine.get(&key).expect("get").as_bytes(),
        Some(b"persistente".as_ref())
    );
}

/// Purpose: MemTable pequeña ⇒ `needs_flush` ⇒ `schedule_flush` + `wait_flush`.
/// Inputs: capacidad 32 bytes, varios puts.
/// Returns: panics si no se programa flush o falla wait.
#[test]
fn auto_schedule_flush_when_memtable_full() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut c = cfg(dir.path());
    c.mem_capacity_bytes = Some(32);
    c.auto_schedule_flush = true;

    let mut orch = Orchestrator::new(c);
    orch.start().expect("start");

    for i in 0..8_u8 {
        let key = make_learn_key(&[b'k', i]).expect("key");
        let val = [i; 8];
        orch.submit_record(&key, &val).expect("submit");
    }

    // Tras puts, o bien ya se programó flush, o needs_flush sigue true.
    if orch.needs_flush().expect("needs") {
        orch.schedule_flush().expect("schedule");
    }
    orch.wait_flush().expect("wait");
    orch.shutdown().expect("shutdown");

    let engine = nvme_state_db::Engine::open(dir.path()).expect("reopen");
    let key = make_learn_key(&[b'k', 0]).expect("key");
    assert_eq!(
        engine.get(&key).expect("get").as_bytes(),
        Some([0u8; 8].as_slice())
    );
}

/// Purpose: cola acotada sin consumidor ⇒ `BridgeSaturated` (backpressure de cola).
/// Inputs: capacidad 1.
/// Returns: panics si el segundo submit no satura.
#[test]
fn bounded_queue_reports_bridge_saturated() {
    let (tx, _rx) = bridge_channel(1);
    tx.submit_record(b"a", b"1").expect("first");
    assert_eq!(tx.submit_record(b"b", b"2"), Err(Error::BridgeSaturated));
    assert_eq!(
        Orchestrator::apply_backpressure(Err(Error::BridgeSaturated), true),
        Err(Error::PipelineStall)
    );
}

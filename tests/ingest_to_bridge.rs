//! Integración fase 5: shreds sintéticos → `ingest_bytes` → bridge → Engine.
//!
//! Sin UDP: mismo camino que un TVU mínimo en laboratorio (parse + FEC + put).

#![cfg(feature = "simd")]

use mini_solana_turbine::shred::{self, CodeShredHeader, DataShredHeader, ShredHeader};
use mini_solana_turbine::{FecEngine, DEFAULT_SHARD_BYTES, PACKET_SIZE};
use solana_pipeline_unified::{make_learn_key, Orchestrator, OrchestratorConfig};
use std::net::SocketAddr;

/// Purpose: rellena un shard con un patrón estable.
fn fill(dest: &mut [u8], tag: u8) {
    for (i, b) in dest.iter_mut().enumerate() {
        *b = tag.wrapping_add(i as u8);
    }
}

/// Purpose: data0 + code reconstruye data1 y ambos shards quedan en nvme.
///
/// Inputs: tempdir vía orquestador.
///
/// Returns: panics si no hay reconstrucción o los valores no coinciden.
#[test]
fn ingest_reconstruct_persists_shards() {
    let dir = tempfile::tempdir().expect("tempdir");
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
    let mut orch = Orchestrator::new(OrchestratorConfig::local(dir.path(), addr));
    orch.start().expect("start");

    let mut d0 = [0u8; DEFAULT_SHARD_BYTES];
    let mut d1 = [0u8; DEFAULT_SHARD_BYTES];
    fill(&mut d0, 1);
    fill(&mut d1, 2);
    let mut r0 = [0u8; DEFAULT_SHARD_BYTES];
    FecEngine::new(2, 1, DEFAULT_SHARD_BYTES)
        .expect("fec")
        .encode(&[&d0, &d1], &mut [&mut r0])
        .expect("encode");

    let mut pkt0 = [0u8; PACKET_SIZE];
    let n0 = shred::encode_data(
        &mut pkt0,
        ShredHeader::data(1, 0, 0, 1),
        DataShredHeader::new(1, 0),
        &d0,
    )
    .expect("enc d0");

    let first = orch.ingest_bytes(&pkt0[..n0]).expect("ingest d0");
    assert_eq!(first.reconstructed, 0);
    assert_eq!(first.records_submitted, 1);

    let mut pktc = [0u8; PACKET_SIZE];
    let nc = shred::encode_code(
        &mut pktc,
        ShredHeader::code(1, 0, 2, 1),
        CodeShredHeader::new(2, 1, 0),
        &r0,
    )
    .expect("enc code");

    let second = orch.ingest_bytes(&pktc[..nc]).expect("ingest code");
    assert_eq!(second.reconstructed, 1);
    assert_eq!(second.records_submitted, 2);

    let metrics = orch.pipeline().expect("pipe").metrics();
    assert_eq!(metrics.received(), 2);
    assert_eq!(metrics.reconstructed(), 1);
    assert_eq!(metrics.dropped(), 0);

    orch.shutdown().expect("shutdown");

    let engine = nvme_state_db::Engine::open(dir.path()).expect("reopen");
    let k0 = make_learn_key(b"shard/0").expect("k0");
    let k1 = make_learn_key(b"shard/1").expect("k1");
    assert_eq!(engine.get(&k0).expect("g0").as_bytes(), Some(d0.as_slice()));
    assert_eq!(engine.get(&k1).expect("g1").as_bytes(), Some(d1.as_slice()));
}

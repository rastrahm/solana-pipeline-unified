//! Bench e2e de laboratorio: registro/paquete → bridge → `Engine::put`.
//!
//! **No es Gbps de mainnet.** Dominado por WAL `O_SYNC`. Correr:
//!
//! ```bash
//! cargo bench --bench e2e_throughput --features simd
//! ```

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use mini_solana_turbine::shred::{self, CodeShredHeader, DataShredHeader, ShredHeader};
use mini_solana_turbine::{FecEngine, DEFAULT_SHARD_BYTES, PACKET_SIZE};
use solana_pipeline_unified::{make_learn_key, Error, Orchestrator, OrchestratorConfig};
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn bench_root() -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("bench-e2e");
    fs::create_dir_all(&base).expect("mkdir bench-e2e");
    base
}

fn fresh_dir(label: &str) -> PathBuf {
    let dir = bench_root().join(format!(
        "{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    fs::create_dir_all(&dir).expect("mkdir run");
    dir
}

fn local_cfg(dir: &std::path::Path) -> OrchestratorConfig {
    let addr: SocketAddr = "127.0.0.1:9001".parse().expect("addr");
    let mut c = OrchestratorConfig::local(dir, addr);
    c.queue_capacity = 65_536;
    c
}

fn submit_or_wait(orch: &Orchestrator, key: &[u8], value: &[u8]) {
    for _ in 0..50_000 {
        match orch.submit_record(key, value) {
            Ok(()) => return,
            Err(Error::PipelineStall) | Err(Error::BridgeSaturated) => {
                let _ = orch.maybe_schedule_flush();
                std::thread::sleep(Duration::from_micros(500));
            }
            Err(e) => panic!("submit: {e}"),
        }
    }
    panic!("submit: cola no drena");
}

/// Encola y espera a que `get` vea el valor (e2e durable vía bridge).
fn submit_durable(orch: &Orchestrator, key: &[u8], value: &[u8]) {
    submit_or_wait(orch, key, value);
    let engine = orch.engine().expect("engine");
    for _ in 0..50_000 {
        if let Ok(lookup) = engine.get(key) {
            if lookup.as_bytes() == Some(value) {
                return;
            }
        }
        std::thread::sleep(Duration::from_micros(50));
    }
    panic!("submit_durable: el worker no aplicó el put a tiempo");
}

fn percentile(sorted_ns: &[u64], p: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted_ns.len() as f64 - 1.0)).round() as usize;
    sorted_ns[idx.min(sorted_ns.len() - 1)]
}

fn print_percentiles(label: &str, mut samples_ns: Vec<u64>) {
    samples_ns.sort_unstable();
    let p50 = percentile(&samples_ns, 50.0);
    let p99 = percentile(&samples_ns, 99.0);
    let n = samples_ns.len();
    let sum: u128 = samples_ns.iter().map(|&x| u128::from(x)).sum();
    let mean = if n == 0 {
        0
    } else {
        u64::try_from(sum / n as u128).unwrap_or(u64::MAX)
    };
    eprintln!("{label}: n={n} mean={mean} ns p50={p50} ns p99={p99} ns");
    if mean > 0 {
        let ops = 1_000_000_000u128 / u128::from(mean);
        eprintln!("{label}: ~{ops} ops/s (1/mean; laboratorio, no red)");
    }
}

fn fill(dest: &mut [u8], tag: u8) {
    for (i, b) in dest.iter_mut().enumerate() {
        *b = tag.wrapping_add(i as u8);
    }
}

fn encode_demo_packets() -> ([u8; PACKET_SIZE], usize, [u8; PACKET_SIZE], usize) {
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
    let mut pktc = [0u8; PACKET_SIZE];
    let nc = shred::encode_code(
        &mut pktc,
        ShredHeader::code(1, 0, 2, 1),
        CodeShredHeader::new(2, 1, 0),
        &r0,
    )
    .expect("enc code");
    (pkt0, n0, pktc, nc)
}

/// Orquestador vivo: mide encolar+put (el worker va detrás; cola grande).
fn bench_submit_record(c: &mut Criterion) {
    let dir = fresh_dir("submit");
    let mut orch = Orchestrator::new(local_cfg(&dir));
    orch.start().expect("start");

    // Warm-up corto.
    for j in 0..4u64 {
        let key = make_learn_key(&j.to_be_bytes()).expect("key");
        submit_durable(&orch, &key, b"0123456789abcdef0123456789abcdef");
    }

    let mut group = c.benchmark_group("e2e_submit_record");
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(4));
    group.warm_up_time(Duration::from_millis(500));

    let mut i = 100u64;
    group.bench_function("submit_32b_durable", |b| {
        b.iter(|| {
            let key = make_learn_key(&i.to_be_bytes()).expect("key");
            i = i.wrapping_add(1);
            submit_durable(
                &orch,
                black_box(&key),
                black_box(b"0123456789abcdef0123456789abcdef"),
            );
        });
    });
    group.finish();

    let mut samples = Vec::with_capacity(40);
    for j in 0..40u64 {
        let key = make_learn_key(&(10_000 + j).to_be_bytes()).expect("key");
        let t0 = Instant::now();
        submit_durable(&orch, &key, b"0123456789abcdef0123456789abcdef");
        samples.push(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    print_percentiles("submit_durable manual", samples);

    orch.shutdown().expect("shutdown");
}

/// Cada muestra: nuevo orquestador + par data/code (pocas reps: open/flush pesan).
fn bench_ingest_reconstruct(c: &mut Criterion) {
    let (pkt0, n0, pktc, nc) = encode_demo_packets();

    let mut group = c.benchmark_group("e2e_ingest_pair");
    group.throughput(Throughput::Elements(2));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(4));
    group.warm_up_time(Duration::from_millis(500));

    group.bench_function("data_plus_code_reconstruct", |b| {
        b.iter(|| {
            let dir = fresh_dir("ingest");
            let mut orch = Orchestrator::new(local_cfg(&dir));
            orch.start().expect("start");
            let a = orch.ingest_bytes(black_box(&pkt0[..n0])).expect("d0");
            let r = orch.ingest_bytes(black_box(&pktc[..nc])).expect("code");
            black_box((a.reconstructed, r.reconstructed, r.records_submitted));
            orch.shutdown().expect("stop");
        });
    });
    group.finish();

    let mut samples = Vec::with_capacity(8);
    for _ in 0..8 {
        let dir = fresh_dir("ingest-m");
        let mut orch = Orchestrator::new(local_cfg(&dir));
        orch.start().expect("start");
        let t0 = Instant::now();
        orch.ingest_bytes(&pkt0[..n0]).expect("d0");
        orch.ingest_bytes(&pktc[..nc]).expect("code");
        samples.push(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
        orch.shutdown().expect("stop");
    }
    print_percentiles("ingest_data+code manual", samples);
}

criterion_group!(benches, bench_submit_record, bench_ingest_reconstruct);
criterion_main!(benches);

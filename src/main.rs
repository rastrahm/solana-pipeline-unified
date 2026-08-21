//! Punto de entrada del binario `solana-pipeline-unified`.
//!
//! Demo de laboratorio: abre el orquestador, opcionalmente ingeriere shreds
//! sintéticos y persiste shards vía `nvme-state-db`. Errores de aplicación:
//! `anyhow`. La librería usa `solana_pipeline_unified::Error`.

use anyhow::{bail, Context, Result};
use solana_pipeline_unified::{make_learn_key, Orchestrator, OrchestratorConfig};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

#[cfg(feature = "simd")]
use mini_solana_turbine::shred::{self, CodeShredHeader, DataShredHeader, ShredHeader};
#[cfg(feature = "simd")]
use mini_solana_turbine::{FecEngine, DEFAULT_SHARD_BYTES, PACKET_SIZE};

/// Opciones parseadas de argv (CLI mínima, sin crates extra).
struct Cli {
    data_dir: PathBuf,
    addr: SocketAddr,
    queue_cap: Option<usize>,
    demo: bool,
    help: bool,
}

/// Purpose: imprime uso del binario de laboratorio.
fn print_usage() {
    eprintln!(
        "\
Uso:
  solana-pipeline-unified --data-dir DIR [opciones]

Opciones:
  --data-dir DIR     Directorio del motor nvme-state-db (obligatorio salvo --help)
  --addr HOST:PORT   Addr lógica del nodo Turbine (default 127.0.0.1:9001)
  --queue-cap N      Capacidad de la cola del bridge (default 128)
  --demo             Ingiere shreds sintéticos (data+code) y persiste shards
  --help             Esta ayuda

Ejemplo:
  cargo run --features simd -- --data-dir /tmp/spu-demo --demo
"
    );
}

/// Purpose: parsea argv a [`Cli`].
///
/// Inputs: iterator de argumentos (sin el nombre del programa).
///
/// Returns: opciones, o error de uso.
fn parse_args<I, S>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut data_dir: Option<PathBuf> = None;
    let mut addr: SocketAddr = "127.0.0.1:9001"
        .parse()
        .map_err(|_| anyhow::anyhow!("addr default inválido"))?;
    let mut queue_cap = None;
    let mut demo = false;
    let mut help = false;

    let mut it = args.into_iter().peekable();
    while let Some(raw) = it.next() {
        let arg = raw.as_ref();
        match arg {
            "--help" | "-h" => help = true,
            "--demo" => demo = true,
            "--data-dir" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--data-dir requiere un path"))?;
                data_dir = Some(PathBuf::from(v.as_ref()));
            }
            "--addr" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--addr requiere HOST:PORT"))?;
                addr = v
                    .as_ref()
                    .parse()
                    .context("parsear --addr")?;
            }
            "--queue-cap" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--queue-cap requiere un entero"))?;
                queue_cap = Some(
                    v.as_ref()
                        .parse()
                        .context("parsear --queue-cap")?,
                );
            }
            other if other.starts_with('-') => {
                bail!("opción desconocida: {other}");
            }
            other => {
                // Posicional: data-dir si aún no se fijó.
                if data_dir.is_none() {
                    data_dir = Some(PathBuf::from(other));
                } else {
                    bail!("argumento posicional inesperado: {other}");
                }
            }
        }
    }

    Ok(Cli {
        data_dir: data_dir.unwrap_or_default(),
        addr,
        queue_cap,
        demo,
        help,
    })
}

#[cfg(feature = "simd")]
fn fill(dest: &mut [u8], tag: u8) {
    for (i, b) in dest.iter_mut().enumerate() {
        *b = tag.wrapping_add(i as u8);
    }
}

/// Purpose: demo FEC → bridge → Engine (requiere feature `simd`).
#[cfg(feature = "simd")]
fn run_demo(orch: &mut Orchestrator) -> Result<()> {
    let mut d0 = [0u8; DEFAULT_SHARD_BYTES];
    let mut d1 = [0u8; DEFAULT_SHARD_BYTES];
    fill(&mut d0, 1);
    fill(&mut d1, 2);
    let mut r0 = [0u8; DEFAULT_SHARD_BYTES];
    FecEngine::new(2, 1, DEFAULT_SHARD_BYTES)
        .map_err(|e| anyhow::anyhow!("FecEngine: {e}"))?
        .encode(&[&d0, &d1], &mut [&mut r0])
        .map_err(|e| anyhow::anyhow!("encode FEC: {e}"))?;

    let mut pkt0 = [0u8; PACKET_SIZE];
    let n0 = shred::encode_data(
        &mut pkt0,
        ShredHeader::data(1, 0, 0, 1),
        DataShredHeader::new(1, 0),
        &d0,
    )
    .map_err(|e| anyhow::anyhow!("encode data: {e}"))?;

    let first = orch
        .ingest_bytes(&pkt0[..n0])
        .context("ingest data shred")?;
    println!(
        "ingest data: reconstructed={}, submitted={}, forward_dests={}",
        first.reconstructed, first.records_submitted, first.forward_dest_count
    );

    let mut pktc = [0u8; PACKET_SIZE];
    let nc = shred::encode_code(
        &mut pktc,
        ShredHeader::code(1, 0, 2, 1),
        CodeShredHeader::new(2, 1, 0),
        &r0,
    )
    .map_err(|e| anyhow::anyhow!("encode code: {e}"))?;

    let second = orch
        .ingest_bytes(&pktc[..nc])
        .context("ingest code shred")?;
    println!(
        "ingest code: reconstructed={}, submitted={}, forward_dests={}",
        second.reconstructed, second.records_submitted, second.forward_dest_count
    );

    if let Some(pipe) = orch.pipeline() {
        let m = pipe.metrics();
        println!(
            "métricas turbine: received={}, reconstructed={}, dropped={}",
            m.received(),
            m.reconstructed(),
            m.dropped()
        );
    }
    Ok(())
}

#[cfg(not(feature = "simd"))]
fn run_demo(_orch: &mut Orchestrator) -> Result<()> {
    bail!("--demo requiere compilar con feature `simd` (cargo run --features simd …)");
}

/// Purpose: tras `shutdown` (cola drenada + flush), comprueba shards en disco.
fn verify_persisted_shards(data_dir: &std::path::Path) -> Result<()> {
    let engine = nvme_state_db::Engine::open(data_dir).context("reabrir Engine")?;
    let k0 = make_learn_key(b"shard/0").context("clave shard/0")?;
    let k1 = make_learn_key(b"shard/1").context("clave shard/1")?;
    let len0 = engine
        .get(&k0)
        .context("get shard/0")?
        .as_bytes()
        .map(|b| b.len());
    let len1 = engine
        .get(&k1)
        .context("get shard/1")?
        .as_bytes()
        .map(|b| b.len());
    println!("tras shutdown: get shard/0 len={len0:?} shard/1 len={len1:?}");
    if len0 != Some(64) || len1 != Some(64) {
        bail!("demo incompleta: se esperaban ambos shards de 64 bytes en disco");
    }
    Ok(())
}

/// Purpose: arranca el orquestador según CLI y corre la demo opcional.
fn run(cli: Cli) -> Result<()> {
    if cli.help {
        print_usage();
        return Ok(());
    }
    if cli.data_dir.as_os_str().is_empty() {
        print_usage();
        bail!("falta --data-dir DIR");
    }

    let mut config = OrchestratorConfig::local(&cli.data_dir, cli.addr);
    if let Some(n) = cli.queue_cap {
        config.queue_capacity = n;
    }

    println!("data_dir={}", config.data_dir.display());
    println!("addr={}", config.self_addr);
    println!("queue_cap={}", config.queue_capacity.max(1));

    let mut orch = Orchestrator::new(config);
    orch.start().context("orchestrator start")?;
    println!("orquestador: running");

    if cli.demo {
        run_demo(&mut orch)?;
    } else {
        println!("sin --demo: solo open/close del motor (usa --demo para FEC→put)");
    }

    orch.shutdown().context("orchestrator shutdown")?;
    println!("orquestador: stopped");

    if cli.demo {
        verify_persisted_shards(&cli.data_dir)?;
    }
    Ok(())
}

fn main() -> ExitCode {
    let cli = match parse_args(env::args().skip(1)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e:#}");
            print_usage();
            return ExitCode::from(2);
        }
    };

    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_args;

    #[test]
    fn parses_demo_and_data_dir() {
        let cli = parse_args(["--data-dir", "/tmp/x", "--demo", "--queue-cap", "8"])
            .expect("parse");
        assert!(cli.demo);
        assert_eq!(cli.data_dir.as_os_str(), "/tmp/x");
        assert_eq!(cli.queue_cap, Some(8));
        assert!(!cli.help);
    }

    #[test]
    fn help_flag() {
        let cli = parse_args(["--help"]).expect("parse");
        assert!(cli.help);
    }
}

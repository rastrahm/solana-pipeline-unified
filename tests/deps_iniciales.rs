//! Tests iniciales de fase 2: comprobar que los crates hermanos enlazan
//! y que sus tipos públicos se pueden construir.
//!
//! Aún no hay pipeline completo, bridge real ni `put` de orquestación.

use mini_solana_turbine::{
    parse_addr, Error as TurbineError, Node, NodeId, PacketArena, Stake, PACKET_SIZE,
};
use nvme_state_db::{Engine, EngineOptions, Error as NvmeError, Key, WAL_ALIGN};
use std::net::SocketAddr;

/// Purpose: constantes y constructores básicos de ambos crates compilan.
/// Inputs: ninguno.
/// Returns: panics si tamaños/opciones no son los esperados.
#[test]
fn sibling_public_types_compile() {
    const { assert!(PACKET_SIZE > 0) };
    assert_eq!(WAL_ALIGN, 4096);

    let opts = EngineOptions::default();
    assert!(opts.mem_capacity_bytes > 0);

    let key = Key::new(b"fase2").expect("clave no vacía");
    assert_eq!(key.as_bytes(), b"fase2");

    let _arena = PacketArena::<2>::new();
    let _addr: SocketAddr = parse_addr("127.0.0.1:0").expect("addr");

    let _t_err: TurbineError = TurbineError::TurbineEmptyCluster;
    let _n_err: NvmeError = NvmeError::EmptyKey;
}

/// Purpose: `Engine::open` enlaza el motor real (sin writes de orquestador).
/// Inputs: directorio temporal.
/// Returns: panics si open falla.
#[test]
fn nvme_engine_opens_tempdir() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Engine::open(dir.path()).expect("open");
    assert_eq!(engine.dir(), dir.path());
    drop(engine);
}

/// Purpose: árbol Turbine mínimo (API de routing, sin UDP).
/// Inputs: un nodo local.
/// Returns: panics si `build` falla.
#[test]
fn turbine_tree_builds_one_node() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().expect("parse");
    let node = Node::new(NodeId::new(1), Stake::new(100), addr);
    let tree = mini_solana_turbine::turbine::tree::build(&[node], 2).expect("tree");
    let mut out = [NodeId::new(0); 4];
    let n = tree.children_of(NodeId::new(1), &mut out).expect("children");
    assert_eq!(n, 0);
}

#[cfg(feature = "simd")]
mod with_simd {
    use mini_solana_turbine::{
        turbine::tree::build, Node, NodeId, Pipeline, Stake, DEFAULT_SHARD_BYTES,
    };
    use std::net::SocketAddr;

    /// Purpose: `Pipeline` (feature `simd`) se construye con defaults.
    /// Inputs: árbol de un nodo.
    /// Returns: panics si `with_defaults` falla.
    #[test]
    fn pipeline_builds_with_defaults() {
        const { assert!(DEFAULT_SHARD_BYTES > 0) };
        let addr: SocketAddr = "127.0.0.1:8001".parse().expect("parse");
        let node = Node::new(NodeId::new(1), Stake::new(100), addr);
        let tree = build(&[node], 2).expect("tree");
        let _pipeline = Pipeline::with_defaults(tree, NodeId::new(1)).expect("pipeline");
    }
}

#[cfg(feature = "uring")]
mod with_uring {
    use mini_solana_turbine::UdpIngress;

    /// Purpose: el tipo `UdpIngress` existe con feature `uring` (sin bind).
    /// Inputs: ninguno.
    /// Returns: fuerza el enlace del símbolo; no abre sockets.
    #[test]
    fn udp_ingress_type_is_linked() {
        let size = std::mem::size_of::<UdpIngress>();
        let _ = size;
    }
}

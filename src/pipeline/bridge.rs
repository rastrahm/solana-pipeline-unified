//! Bridge: cola acotada entre ingestión y [`nvme_state_db::Engine`].
//!
//! ## Convención de registro (aprendizaje)
//!
//! No es el encoding de AccountsDB de Solana. Es un KV didáctico:
//!
//! - **Clave:** preferir [`make_learn_key`]: prefijo ASCII `learn/v1/` + sufijo no vacío
//!   (p. ej. id de shred/slot de laboratorio).
//! - **Valor:** bytes opacos (payload reconstruido o fragmento de prueba).
//!
//! El productor usa [`BridgeSender::submit_record`] con `try_send`: si la cola
//! está llena → [`Error::BridgeSaturated`] (no bloquea al llamador).
//! El consumidor ([`BridgeReceiver::drain_into`]) llama solo a `Engine::put`.

use crate::error::Error;
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use nvme_state_db::Engine;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

/// Prefijo ASCII de claves educativas.
pub const LEARN_KEY_PREFIX: &[u8] = b"learn/v1/";

/// Registro poseído que cruza el canal (alloc consciente: fase de aprendizaje).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRecord {
    /// Clave (p. ej. salida de [`make_learn_key`]).
    pub key: Vec<u8>,
    /// Valor opaco.
    pub value: Vec<u8>,
}

/// Extremo productor del bridge (ingest → cola).
#[derive(Debug)]
pub struct BridgeSender {
    tx: Sender<StateRecord>,
}

/// Extremo consumidor del bridge (cola → `Engine::put`).
#[derive(Debug)]
pub struct BridgeReceiver {
    rx: Receiver<StateRecord>,
}

/// Purpose: arma una clave educativa `learn/v1/` + `suffix`.
///
/// Inputs: `suffix` — no vacío.
///
/// Returns: bytes de clave, o [`Error::EmptyRecordKey`].
pub fn make_learn_key(suffix: &[u8]) -> Result<Vec<u8>, Error> {
    if suffix.is_empty() {
        return Err(Error::EmptyRecordKey);
    }
    let mut key = Vec::with_capacity(LEARN_KEY_PREFIX.len() + suffix.len());
    key.extend_from_slice(LEARN_KEY_PREFIX);
    key.extend_from_slice(suffix);
    Ok(key)
}

/// Purpose: crea cola acotada productor/consumidor sin arrancar hilos.
///
/// Inputs: `capacity` — tamaño máximo de la cola (≥ 1 tras clamp).
///
/// Returns: (`BridgeSender`, `BridgeReceiver`).
pub fn bridge_channel(capacity: usize) -> (BridgeSender, BridgeReceiver) {
    let cap = capacity.max(1);
    let (tx, rx) = bounded(cap);
    (BridgeSender { tx }, BridgeReceiver { rx })
}

impl BridgeSender {
    /// Purpose: intenta encolar un registro sin bloquear.
    ///
    /// Inputs:
    /// - `key` — no vacía.
    /// - `value` — bytes del valor (puede ser vacío).
    ///
    /// Returns:
    /// - `Ok(())` si entró en la cola.
    /// - [`Error::EmptyRecordKey`] si `key` está vacía.
    /// - [`Error::BridgeSaturated`] si la cola está llena.
    /// - [`Error::InvalidOrchestratorState`] si el consumidor ya no existe.
    pub fn submit_record(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if key.is_empty() {
            return Err(Error::EmptyRecordKey);
        }
        let record = StateRecord {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        match self.tx.try_send(record) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Error::BridgeSaturated),
            Err(TrySendError::Disconnected(_)) => Err(Error::InvalidOrchestratorState),
        }
    }
}

impl BridgeReceiver {
    /// Purpose: drena la cola escribiendo cada registro con `Engine::put`.
    ///
    /// Inputs: `engine` — motor ya abierto (dueño del disco).
    ///
    /// Returns: `Ok(())` cuando el productor se cerró y no quedan mensajes;
    /// [`Error::PersistFailed`] si algún `put` falla.
    ///
    /// Sale del bucle cuando todos los [`BridgeSender`] se dropean.
    pub fn drain_into(self, engine: &Engine) -> Result<(), Error> {
        while let Ok(record) = self.rx.recv() {
            engine
                .put(&record.key, &record.value)
                .map_err(|_| Error::PersistFailed)?;
        }
        Ok(())
    }
}

/// Bridge activo: sender + hilo consumidor.
pub struct Bridge {
    tx: Option<BridgeSender>,
    worker: Option<JoinHandle<Result<(), Error>>>,
}

impl Bridge {
    /// Purpose: arranca el consumidor en un hilo y conserva el productor.
    ///
    /// Inputs:
    /// - `engine` — compartido con el orquestador.
    /// - `capacity` — cola acotada.
    ///
    /// Returns: bridge listo para [`Bridge::submit_record`], o error de spawn.
    pub fn start(engine: Arc<Engine>, capacity: usize) -> Result<Self, Error> {
        let (tx, rx) = bridge_channel(capacity);
        let worker = thread::Builder::new()
            .name("pipeline-bridge".into())
            .spawn(move || rx.drain_into(&engine))
            .map_err(|_| Error::BridgeSpawnFailed)?;
        Ok(Self {
            tx: Some(tx),
            worker: Some(worker),
        })
    }

    /// Purpose: encola hacia el worker (misma semántica que [`BridgeSender`]).
    ///
    /// Inputs: `key`, `value`.
    ///
    /// Returns: errores de submit, o estado inválido si ya se hizo shutdown.
    pub fn submit_record(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        match &self.tx {
            Some(tx) => tx.submit_record(key, value),
            None => Err(Error::InvalidOrchestratorState),
        }
    }

    /// Purpose: cierra el productor, espera al worker y propaga error de `put`.
    ///
    /// Inputs: ninguno.
    ///
    /// Returns: error del drenado, o `Ok` si el hilo terminó bien.
    pub fn shutdown(mut self) -> Result<(), Error> {
        self.tx = None;
        if let Some(handle) = self.worker.take() {
            match handle.join() {
                Ok(result) => result,
                Err(_) => Err(Error::PersistFailed),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.tx = None;
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{bridge_channel, make_learn_key, Bridge, LEARN_KEY_PREFIX};
    use crate::error::Error;
    use nvme_state_db::Engine;
    use std::sync::Arc;

    /// Purpose: prefijo educativo y rechazo de sufijo vacío.
    /// Inputs: ninguno.
    /// Returns: panics si el formato no coincide.
    #[test]
    fn learn_key_format() {
        let key = make_learn_key(b"slot-1").expect("key");
        assert!(key.starts_with(LEARN_KEY_PREFIX));
        assert!(key.ends_with(b"slot-1"));
        assert_eq!(make_learn_key(b""), Err(Error::EmptyRecordKey));
    }

    /// Purpose: cola llena → `BridgeSaturated` sin bloquear (sin hilo).
    /// Inputs: capacidad 1.
    /// Returns: panics si el segundo submit no satura.
    #[test]
    fn saturated_queue_without_consumer() {
        let (tx, _rx) = bridge_channel(1);
        tx.submit_record(b"k1", b"v1").expect("first");
        assert_eq!(
            tx.submit_record(b"k2", b"v2"),
            Err(Error::BridgeSaturated)
        );
    }

    /// Purpose: N registros encolados son visibles con `get` tras drenar + flush.
    /// Inputs: tempdir, 8 registros.
    /// Returns: panics si falta algún valor.
    #[test]
    fn records_reach_engine_via_bridge() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Arc::new(Engine::open(dir.path()).expect("open"));
        let bridge = Bridge::start(Arc::clone(&engine), 8).expect("bridge");

        for i in 0..8_u8 {
            let key = make_learn_key(&[i]).expect("key");
            let val = [b'v', i];
            bridge.submit_record(&key, &val).expect("submit");
        }

        bridge.shutdown().expect("bridge stop");
        engine.flush().expect("flush");

        for i in 0..8_u8 {
            let key = make_learn_key(&[i]).expect("key");
            let got = engine.get(&key).expect("get");
            assert_eq!(got.as_bytes(), Some([b'v', i].as_slice()));
        }
    }
}

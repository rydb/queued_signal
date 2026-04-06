use arc_swap::ArcSwap;
use dioxus::prelude::*;
use flume::{Receiver, Sender};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;


#[derive(Clone)]
struct QueuedState<T> {
    current: Arc<ArcSwap<T>>,
    notify_rx: Receiver<()>,
}

impl<T> QueuedState<T> {
    fn read(&self) -> Arc<T> {
        self.current.load_full()
    }

    fn get_notify_rx(&self) -> Receiver<()> {
        self.notify_rx.clone()
    }
}

#[derive(Clone)]
struct QueuedWriter<T> {
    command_tx: Sender<Box<dyn FnOnce(&mut T) + Send + 'static>>,
    _handle: Arc<Option<thread::JoinHandle<()>>>,
}

type Mutation<T> = Box<dyn FnOnce(&mut T) + Send + 'static>;

impl<T: Send + Sync + 'static + Clone> QueuedWriter<T> {
    fn start(initial: T, interval: Duration) -> (QueuedState<T>, Self) {
        let current = Arc::new(ArcSwap::new(Arc::new(initial.clone())));
        let (notify_tx, notify_rx) = flume::unbounded();
        let (tx, rx) = flume::unbounded::<Mutation<T>>();

        let state = QueuedState {
            current: current.clone(),
            notify_rx,
        };
        let handle = thread::spawn(move || {
            let mut private_state = initial;
            loop {
                match rx.recv_timeout(interval) {
                    Ok(cmd) => {
                        cmd(&mut private_state);
                        for cmd in rx.drain() {
                            cmd(&mut private_state);
                        }
                    }
                    Err(flume::RecvTimeoutError::Timeout) => {}
                    Err(flume::RecvTimeoutError::Disconnected) => break,
                }
                current.store(Arc::new(private_state.clone()));
                let _ = notify_tx.send(());
            }
        });
        let writer = Self {
            command_tx: tx,
            _handle: Arc::new(Some(handle)),
        };
        (state, writer)
    }

    fn mutate<F>(&self, f: F)
    where
        F: FnOnce(&mut T) + Send + 'static,
    {
        let _ = self.command_tx.send(Box::new(f));
    }
}

struct QueuedSignalInner<T> {
    state: QueuedState<T>,
    writer: QueuedWriter<T>,
}

#[derive(Clone)]
pub struct QueuedSignal<T> {
    inner: Arc<OnceLock<QueuedSignalInner<T>>>,
}

impl<T: Send + Sync + 'static + Clone> QueuedSignal<T> {
    pub fn new_uninitialized() -> Self {
        Self {
            inner: Arc::new(OnceLock::new()),
        }
    }

    pub fn new(initial: T, interval: Duration) -> Self {
        let signal = Self::new_uninitialized();
        signal.initialize(initial, interval).unwrap();
        signal
    }

    pub fn initialize(&self, initial: T, interval: Duration) -> Result<(), &'static str> {
        let (state, writer) = QueuedWriter::start(initial, interval);
        self.inner
            .set(QueuedSignalInner { state, writer })
            .map_err(|_| "QueuedSignal already initialized")
    }

    pub fn is_initialized(&self) -> bool {
        self.inner.get().is_some()
    }

    pub fn read(&self) -> Result<Arc<T>, &'static str> {
        let inner = self.inner.get().ok_or("QueuedSignal not initialized")?;
        Ok(inner.state.read())
    }

    pub fn mutate<F>(&self, f: F) -> Result<(), &'static str>
    where
        F: FnOnce(&mut T) + Send + 'static,
    {
        let inner = self.inner.get().ok_or("QueuedSignal not initialized")?;
        inner.writer.mutate(f);
        Ok(())
    }

    /// Attaches the signal to the current component.
    /// Returns a reactive `Signal<Option<Arc<T>>>`.
    pub fn attach_signal(&self) -> Signal<Option<Arc<T>>> {
        let inner_ref = use_hook(|| self.inner.clone());
        let mut value_signal = use_signal(|| {
            inner_ref.get().map(|inner| inner.state.read())
        });
        use_future(move || {
            let inner = inner_ref.clone();
            async move {
                while inner.get().is_none() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let inner = inner.get().unwrap();
                let rx = inner.state.get_notify_rx();
                let state = inner.state.clone();
                while rx.recv_async().await.is_ok() {
                    value_signal.set(Some(state.read()));
                }
            }
        });
        value_signal
    }
}

impl<T: Send + Sync + 'static + Clone> Default for QueuedSignal<T> {
    fn default() -> Self {
        Self::new_uninitialized()
    }
}

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone)]
pub struct QueuedSignalRegistry {
    map: Arc<Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>>,
}

impl QueuedSignalRegistry {
    pub fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_signal<T: Send + Sync + 'static + Clone + Default>(
        &self,
    ) -> Arc<QueuedSignal<T>> {
        let type_id = TypeId::of::<T>();
        // First, try to read without locking (we can't, because we need to check existence).
        // We must lock the mutex. But this lock is taken only once per type, then cached.
        let mut map = self.map.lock().unwrap();
        if let Some(entry) = map.get(&type_id) {
            return entry
                .downcast_ref::<Arc<QueuedSignal<T>>>()
                .expect("Type mismatch")
                .clone();
        }
        let signal = Arc::new(QueuedSignal::new(T::default(), DEFAULT_FLUSH_INTERVAL));
        map.insert(type_id, Box::new(signal.clone()));
        signal
    }

    pub fn register_signal<T: Send + Sync + 'static + Clone>(
        &self,
        signal: Arc<QueuedSignal<T>>,
    ) {
        let type_id = TypeId::of::<T>();
        let mut map = self.map.lock().unwrap();
        map.insert(type_id, Box::new(signal));
    }
}

impl Default for QueuedSignalRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct QueuedSignalHandle<T> {
    read_signal: Signal<Option<Arc<T>>>,
    signal: Arc<QueuedSignal<T>>,
}

impl<T: Send + Sync + 'static + Clone> QueuedSignalHandle<T> {
    pub fn mutate<F>(&self, f: F) -> Result<(), &'static str>
    where
        F: FnOnce(&mut T) + Send + 'static,
    {
        self.signal.mutate(f)
    }

    pub fn read_signal(&self) -> Signal<Option<Arc<T>>> {
        self.read_signal
    }
}

pub fn use_queued_signal<T: Send + Sync + 'static + Clone + Default>() -> QueuedSignalHandle<T> {
    let registry = use_context::<QueuedSignalRegistry>();
    let signal = registry.get_signal::<T>();
    let read_signal = signal.attach_signal();
    QueuedSignalHandle { read_signal, signal }
}
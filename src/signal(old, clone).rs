use arc_swap::ArcSwap;
use dioxus::prelude::*;
use flume::{Receiver, Sender};
use std::ops::Deref;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// -----------------------------------------------------------------------------
// QueuedState / QueuedWriter
// -----------------------------------------------------------------------------

#[derive(Clone)]
struct QueuedState<T> {
    current: Arc<ArcSwap<T>>,
    notify_rx: Receiver<()>,
    notify_tx: Sender<()>,
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
            notify_rx: notify_rx.clone(),
            notify_tx: notify_tx.clone(),
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

// -----------------------------------------------------------------------------
// QueuedSignal
// -----------------------------------------------------------------------------

struct QueuedSignalInner<T> {
    state: QueuedState<T>,
    writer: QueuedWriter<T>,
}

#[derive(Clone)]
pub struct QueuedSignal<T> {
    inner: Arc<std::sync::OnceLock<QueuedSignalInner<T>>>,
}

impl<T: Send + Sync + 'static + Clone> QueuedSignal<T> {
    pub fn new_uninitialized() -> Self {
        Self {
            inner: Arc::new(std::sync::OnceLock::new()),
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

    pub fn set_value(&self, value: T) -> Result<(), &'static str> {
        let inner = self.inner.get().ok_or("QueuedSignal not initialized")?;
        inner.state.current.store(Arc::new(value));
        let _ = inner.state.notify_tx.send(());
        Ok(())
    }

    pub fn attach_signal(&self) -> Signal<Option<Arc<T>>> {
        let inner_ref = use_hook(|| self.inner.clone());
        let mut value_signal = use_signal(|| inner_ref.get().map(|inner| inner.state.read()));

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

// -----------------------------------------------------------------------------
// QueuedResource (registry‑agnostic)
// -----------------------------------------------------------------------------

#[derive(Clone)]
pub struct QueuedResource<T> {
    pub signal: QueuedSignal<T>,
}

impl<T: Send + Sync + 'static + Clone> QueuedResource<T> {
    pub fn read(&self) -> Result<Arc<T>, &'static str> {
        self.signal.read()
    }

    pub fn mutate<F>(&self, f: F) -> Result<(), &'static str>
    where
        F: FnOnce(&mut T) + Send + 'static,
    {
        self.signal.mutate(f)
    }

    pub fn attach(&self) -> Signal<Option<Arc<T>>> {
        self.signal.attach_signal()
    }
}

// -----------------------------------------------------------------------------
// Reactive Handle (combines Signal + mutation)
// -----------------------------------------------------------------------------

/// A handle that behaves like a Dioxus `Signal` for a queued resource.
/// It implements `Deref<Target = Signal<Option<Arc<T>>>>`, so you can use it
/// directly in RSX as `{handle.map(|arc| arc.field)}` or call `handle()`.
#[derive(Clone)]
pub struct QueuedSignalHandle<T> {
    signal: Signal<Option<Arc<T>>>,
    resource: QueuedResource<T>,
}

impl<T: Send + Sync + 'static + Clone> QueuedSignalHandle<T> {
    /// Creates a new handle from a `QueuedResource`. This should only be called
    /// inside a Dioxus component because it uses `attach()` which relies on hooks.
    pub fn new(resource: QueuedResource<T>) -> Self {
        let signal = resource.attach();
        Self { signal, resource }
    }

    /// Schedules a mutation to be applied asynchronously.
    pub fn mutate<F>(&self, f: F)
    where
        F: FnOnce(&mut T) + Send + 'static,
    {
        let _ = self.resource.mutate(f);
    }

    /// Provides direct access to the underlying `Signal`.
    pub fn signal(&self) -> &Signal<Option<Arc<T>>> {
        &self.signal
    }
}

impl<T> Deref for QueuedSignalHandle<T> {
    type Target = Signal<Option<Arc<T>>>;

    fn deref(&self) -> &Self::Target {
        &self.signal
    }
}
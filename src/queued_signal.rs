use arc_swap::ArcSwap;
use dioxus::prelude::*;
use flume::{Receiver, Sender};
use std::sync::Arc;
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

        let state = QueuedState { current: current.clone(), notify_rx };
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

#[derive(Clone)]
pub struct QueuedSignal<T> {
    state: QueuedState<T>,
    writer: QueuedWriter<T>,
}

impl<T: Send + Sync + 'static + Clone> QueuedSignal<T> {
    /// Create a new queued signal outside of any Dioxus component.
    /// The signal can be shared across multiple components and DOMs.
    pub fn new(initial: T, interval: Duration) -> Self {
        let (state, writer) = QueuedWriter::start(initial, interval);
        Self { state, writer }
    }

    /// Mutate the queued state. The mutation will be applied in FIFO order
    /// at the next flush interval.
    pub fn mutate<F>(&self, f: F)
    where
        F: FnOnce(&mut T) + Send + 'static,
    {
        self.writer.mutate(f);
    }

    /// Attach this queued signal to a Dioxus component.
    /// Returns a Dioxus `Signal<Arc<T>>` that automatically updates when the queued state flushes.
    /// This must be called inside a component.
    pub fn use_attached(&self) -> Signal<Arc<T>> {
        let mut value_signal = use_signal(|| self.state.read());
        let state_clone = use_hook(|| self.state.clone());
        use_future(move || {
            let rx = state_clone.get_notify_rx();
            let state = state_clone.clone();
            async move {
                while rx.recv_async().await.is_ok() {
                    value_signal.set(state.read());
                }
            }
        });
        value_signal
    }
}

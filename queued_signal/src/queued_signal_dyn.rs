//! Erased queued signal views with rebindable reads and writes.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use flume::Sender;
use kovan::Atom;
use tokio::sync::watch;

use crate::atom_coerce_dyn::{AtomCoerceDyn, EraseMut};
use crate::state::{
    HealthStatus, MutationOp, QueuedSignal, QueuedStateDyn, ReaderRegistry, TrackedReadGuardDyn,
};

/// Type-erased write routing for an erased queued signal view.
pub struct ErasedQueuedWrite<D: ?Sized + 'static> {
    mutate: unsafe fn(&ErasedQueuedWrite<D>, Arc<dyn Fn(&mut D) + Send + Sync>),
    mutate_set: unsafe fn(&ErasedQueuedWrite<D>, Arc<dyn Fn(&mut D) + Send + Sync>),
    set_value: unsafe fn(
        &ErasedQueuedWrite<D>,
        Box<dyn Any + Send + Sync>,
    ) -> Result<(), Box<dyn Any + Send + Sync>>,
    context: Arc<dyn Any + Send + Sync>,
    marker: PhantomData<fn() -> D>,
}

/// Typed sender context used when routing into a typed signal.
struct TypedWrite<T: Clone + Send + Sync + 'static, D: ?Sized + 'static> {
    add_tx: Sender<MutationOp<T, D>>,
    set_tx: Sender<MutationOp<T, D>>,
    marker: PhantomData<fn() -> D>,
}

impl<D: ?Sized + 'static> ErasedQueuedWrite<D> {
    /// Assembles erased write routing from raw parts.
    pub fn from_raw_parts(
        mutate: unsafe fn(&ErasedQueuedWrite<D>, Arc<dyn Fn(&mut D) + Send + Sync>),
        mutate_set: unsafe fn(&ErasedQueuedWrite<D>, Arc<dyn Fn(&mut D) + Send + Sync>),
        set_value: unsafe fn(
            &ErasedQueuedWrite<D>,
            Box<dyn Any + Send + Sync>,
        ) -> Result<(), Box<dyn Any + Send + Sync>>,
        context: Arc<dyn Any + Send + Sync>,
    ) -> ErasedQueuedWrite<D> {
        ErasedQueuedWrite {
            mutate,
            mutate_set,
            set_value,
            context,
            marker: PhantomData,
        }
    }

    /// Builds write routing into a typed queued signal.
    pub fn from_typed<T>(signal: &QueuedSignal<T, D>) -> ErasedQueuedWrite<D>
    where
        T: Clone + Send + Sync + 'static + EraseMut<D>,
    {
        let context: Arc<dyn Any + Send + Sync> = Arc::new(TypedWrite {
            add_tx: signal.add_tx(),
            set_tx: signal.set_tx(),
            marker: PhantomData,
        });
        ErasedQueuedWrite {
            mutate: mutate_typed::<T, D>,
            mutate_set: mutate_set_typed::<T, D>,
            set_value: set_value_unsupported::<D>,
            context,
            marker: PhantomData,
        }
    }

    /// Runtime data used by the write functions.
    pub fn context(&self) -> &(dyn Any + Send + Sync) {
        &*self.context
    }
}

unsafe fn mutate_typed<T, D>(write: &ErasedQueuedWrite<D>, f: Arc<dyn Fn(&mut D) + Send + Sync>)
where
    T: Clone + Send + Sync + 'static + EraseMut<D>,
    D: ?Sized + 'static,
{
    let ctx = write
        .context()
        .downcast_ref::<TypedWrite<T, D>>()
        .expect("typed write context mismatch");
    let _ = ctx.add_tx.send(MutationOp::Untyped(f));
}

unsafe fn mutate_set_typed<T, D>(
    write: &ErasedQueuedWrite<D>,
    f: Arc<dyn Fn(&mut D) + Send + Sync>,
) where
    T: Clone + Send + Sync + 'static + EraseMut<D>,
    D: ?Sized + 'static,
{
    let ctx = write
        .context()
        .downcast_ref::<TypedWrite<T, D>>()
        .expect("typed write context mismatch");
    let _ = ctx.set_tx.send(MutationOp::Untyped(f));
}

unsafe fn set_value_unsupported<D: ?Sized + 'static>(
    _write: &ErasedQueuedWrite<D>,
    value: Box<dyn Any + Send + Sync>,
) -> Result<(), Box<dyn Any + Send + Sync>> {
    Err(value)
}

/// An erased holder for a queued signal with rebindable reads and writes.
pub struct QueuedSignalDyn<D: ?Sized + 'static> {
    state: QueuedStateDyn<D>,
    holder: AtomCoerceDyn<D>,
    write: Arc<Atom<ErasedQueuedWrite<D>>>,
}

impl<D: ?Sized + 'static> QueuedSignalDyn<D> {
    /// Assembles an erased signal from its read and write parts.
    pub fn from_parts(
        holder: AtomCoerceDyn<D>,
        notify_rx: watch::Receiver<u64>,
        health_rx: watch::Receiver<HealthStatus>,
        registry: Arc<ReaderRegistry>,
        write: ErasedQueuedWrite<D>,
    ) -> QueuedSignalDyn<D> {
        let state = QueuedStateDyn {
            view: holder.handle_dyn(),
            notify_rx,
            health_rx,
            registry,
        };
        QueuedSignalDyn {
            state,
            holder,
            write: Arc::new(Atom::new(write)),
        }
    }

    /// Builds an erased signal bound to a typed signal.
    pub fn from_typed<T>(signal: &QueuedSignal<T, D>) -> QueuedSignalDyn<D>
    where
        T: Clone + Send + Sync + 'static + EraseMut<D>,
    {
        let holder = AtomCoerceDyn::bound(signal.state.cell_dyn());
        let state = QueuedStateDyn {
            view: holder.handle_dyn(),
            notify_rx: signal.state.notify_rx(),
            health_rx: signal.state.health_rx.clone(),
            registry: signal.state.registry.clone(),
        };
        QueuedSignalDyn {
            state,
            holder,
            write: Arc::new(Atom::new(ErasedQueuedWrite::from_typed(signal))),
        }
    }

    /// Rebinds the erased reads and writes to a typed signal.
    pub fn bind<T>(&self, signal: &QueuedSignal<T, D>)
    where
        T: Clone + Send + Sync + 'static + EraseMut<D>,
    {
        self.holder.bind_handle(signal.state.cell_dyn());
        self.write.store(ErasedQueuedWrite::from_typed(signal));
    }

    /// A cloneable handle over this erased signal.
    pub fn handle_dyn(&self) -> QueuedSignalDynHandle<D> {
        QueuedSignalDynHandle {
            inner: self.clone(),
        }
    }

    /// Read the current value as an erased view.
    pub fn read(&self) -> TrackedReadGuardDyn<D> {
        self.state.read()
    }

    /// Current health status of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        self.state.health()
    }

    /// Enqueue a relative mutation through the erased view.
    pub fn mutate(&self, f: Arc<dyn Fn(&mut D) + Send + Sync>) {
        let slot = self.write.load();
        let write: &ErasedQueuedWrite<D> = &*slot;
        // SAFETY: the write functions match the context in the slot.
        unsafe { (write.mutate)(write, f) };
    }

    /// Enqueue an authoritative mutation through the erased view.
    pub fn mutate_set(&self, f: Arc<dyn Fn(&mut D) + Send + Sync>) {
        let slot = self.write.load();
        let write: &ErasedQueuedWrite<D> = &*slot;
        // SAFETY: the write functions match the context in the slot.
        unsafe { (write.mutate_set)(write, f) };
    }

    /// Replace the value through the erased view.
    pub fn set_value(
        &self,
        value: Box<dyn Any + Send + Sync>,
    ) -> Result<(), Box<dyn Any + Send + Sync>> {
        let slot = self.write.load();
        let write: &ErasedQueuedWrite<D> = &*slot;
        // SAFETY: the write functions match the context in the slot.
        unsafe { (write.set_value)(write, value) }
    }
}

impl<D: ?Sized + 'static> Clone for QueuedSignalDyn<D> {
    fn clone(&self) -> Self {
        QueuedSignalDyn {
            state: self.state.clone(),
            holder: self.holder.clone(),
            write: self.write.clone(),
        }
    }
}

/// A cloneable handle over an erased queued signal.
pub struct QueuedSignalDynHandle<D: ?Sized + 'static> {
    inner: QueuedSignalDyn<D>,
}

impl<D: ?Sized + 'static> QueuedSignalDynHandle<D> {
    /// Read the current value as an erased view.
    pub fn read(&self) -> TrackedReadGuardDyn<D> {
        self.inner.read()
    }

    /// Current health status of the underlying signal.
    pub fn health(&self) -> HealthStatus {
        self.inner.health()
    }

    /// Enqueue a relative mutation through the erased view.
    pub fn mutate(&self, f: Arc<dyn Fn(&mut D) + Send + Sync>) {
        self.inner.mutate(f);
    }

    /// Enqueue an authoritative mutation through the erased view.
    pub fn mutate_set(&self, f: Arc<dyn Fn(&mut D) + Send + Sync>) {
        self.inner.mutate_set(f);
    }

    /// Replace the value through the erased view.
    pub fn set_value(
        &self,
        value: Box<dyn Any + Send + Sync>,
    ) -> Result<(), Box<dyn Any + Send + Sync>> {
        self.inner.set_value(value)
    }
}

impl<D: ?Sized + 'static> Clone for QueuedSignalDynHandle<D> {
    fn clone(&self) -> Self {
        QueuedSignalDynHandle {
            inner: self.inner.clone(),
        }
    }
}

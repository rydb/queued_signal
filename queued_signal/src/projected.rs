//! Borrowed projections over part of a queued value.
//!
//! A projection stores one shared and one mutable path as function pointers, so reads borrow the
//! pinned value and writes mutate it in place. Construction validates the path once, after that
//! reads are total.

use std::any::Any;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;

use crate::atom_coerce_dyn::EraseMut;
use crate::queued_signal_dyn::QueuedSignalDyn;
use crate::state::{QueuedSignal, TrackedReadGuardDyn};

/// Shared and mutable access to one part of a value.
#[derive(Clone, Copy)]
pub struct Accessor<S, T> {
    get: fn(&S) -> Option<&T>,
    get_mut: fn(&mut S) -> Option<&mut T>,
}

impl<S, T> Accessor<S, T> {
    /// Creates an accessor from a shared and a mutable path.
    pub const fn new(get: fn(&S) -> Option<&T>, get_mut: fn(&mut S) -> Option<&mut T>) -> Self {
        Self { get, get_mut }
    }
}

/// Two function pointers copied out of an accessor.
type AccessorBits = [usize; 2];

/// Reads a projection through the bits of an accessor.
///
/// # Safety
///
/// The bits must come from the `Accessor<S, T>` that created this shim.
unsafe fn read_shim<S: 'static, T: 'static>(
    bits: *const AccessorBits,
    root: &dyn Any,
) -> Option<*const ()> {
    // SAFETY: the caller guarantees the bits belong to an Accessor<S, T>.
    let accessor: Accessor<S, T> = unsafe { std::mem::transmute_copy(&*bits) };
    let typed: &S = root.downcast_ref()?;
    (accessor.get)(typed).map(|value| value as *const T as *const ())
}

/// Writes a projection through the bits of an accessor.
///
/// # Safety
///
/// The bits must come from the `Accessor<S, T>` that created this shim.
unsafe fn write_shim<S: 'static, T: 'static>(
    bits: *const AccessorBits,
    root: &mut dyn Any,
) -> Option<*mut ()> {
    // SAFETY: the caller guarantees the bits belong to an Accessor<S, T>.
    let accessor: Accessor<S, T> = unsafe { std::mem::transmute_copy(&*bits) };
    let typed: &mut S = root.downcast_mut()?;
    (accessor.get_mut)(typed).map(|value| value as *mut T as *mut ())
}

/// Identity of a projection, used to compare handles.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct ProjectionKey {
    cell: usize,
    get: usize,
    get_mut: usize,
}

/// Projected handle over a queued signal, independent of the root type.
pub struct ProjectedSignal<T: 'static> {
    source: QueuedSignalDyn<dyn Any>,
    read: unsafe fn(*const AccessorBits, &dyn Any) -> Option<*const ()>,
    write: unsafe fn(*const AccessorBits, &mut dyn Any) -> Option<*mut ()>,
    accessor: AccessorBits,
    key: ProjectionKey,
    _target: PhantomData<fn() -> T>,
}

impl<T: 'static> ProjectedSignal<T> {
    /// Creates a projection from a reference that proves the path is present.
    pub fn new<S>(signal: &QueuedSignal<S>, accessor: Accessor<S, T>, _proof: &T) -> Self
    where
        S: Clone + Send + Sync + 'static + EraseMut<dyn Any>,
    {
        assert_eq!(
            std::mem::size_of::<Accessor<S, T>>(),
            std::mem::size_of::<AccessorBits>(),
            "an accessor must stay two function pointers"
        );

        let key = ProjectionKey {
            cell: std::ptr::from_ref(&**signal.state.cell.shared()) as usize,
            get: accessor.get as usize,
            get_mut: accessor.get_mut as usize,
        };
        // SAFETY: both fields are function pointers and the sizes are checked above.
        let accessor: AccessorBits = unsafe { std::mem::transmute_copy(&accessor) };

        Self {
            source: QueuedSignalDyn::from_typed(signal),
            read: read_shim::<S, T>,
            write: write_shim::<S, T>,
            accessor,
            key,
            _target: PhantomData,
        }
    }

    /// Borrows the projected value, subscribing to publishes.
    pub fn read(&self) -> ProjectedRead<T> {
        ProjectedRead {
            guard: self.source.read(),
            read: self.read,
            accessor: self.accessor,
            _target: PhantomData,
        }
    }

    /// Enqueues a relative mutation of the projected value.
    pub fn mutate(&self, f: impl Fn(&mut T) + Send + Sync + 'static) {
        self.enqueue(f, false);
    }

    /// Enqueues an authoritative mutation of the projected value.
    pub fn mutate_set(&self, f: impl Fn(&mut T) + Send + Sync + 'static) {
        self.enqueue(f, true);
    }

    /// Current health status of the underlying signal.
    pub fn health(&self) -> crate::state::HealthStatus {
        self.source.health()
    }

    fn enqueue(&self, f: impl Fn(&mut T) + Send + Sync + 'static, authoritative: bool) {
        let write = self.write;
        let accessor = self.accessor;
        let op = std::sync::Arc::new(move |root: &mut dyn Any| {
            // SAFETY: the shim and the bits were created together for the same root type.
            let pointer = unsafe { write(&accessor, root) };
            let Some(pointer) = pointer else {
                return;
            };
            // SAFETY: the pointer targets the root that the driver is mutating.
            let value: &mut T = unsafe { &mut *(pointer as *mut T) };
            f(value);
        });
        if authoritative {
            self.source.mutate_set(op);
        } else {
            self.source.mutate(op);
        }
    }
}

impl<T: 'static> Clone for ProjectedSignal<T> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            read: self.read,
            write: self.write,
            accessor: self.accessor,
            key: self.key,
            _target: PhantomData,
        }
    }
}

impl<T: 'static> PartialEq for ProjectedSignal<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<T: 'static> Eq for ProjectedSignal<T> {}

impl<T: 'static> fmt::Debug for ProjectedSignal<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectedSignal")
            .field("key", &self.key)
            .finish()
    }
}

/// Pinned read over one projected part of a queued value.
pub struct ProjectedRead<T: 'static> {
    guard: TrackedReadGuardDyn<dyn Any>,
    read: unsafe fn(*const AccessorBits, &dyn Any) -> Option<*const ()>,
    accessor: AccessorBits,
    _target: PhantomData<fn() -> T>,
}

impl<T: 'static> Deref for ProjectedRead<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // Note the explicit deref: unsizing the guard itself to `dyn Any` would coerce the
        // wrapper rather than the value it borrows.
        let root: &dyn Any = &*self.guard;
        // SAFETY: the shim and the bits were created together for the same root type.
        let pointer = unsafe { (self.read)(&self.accessor, root) }
            .expect("projected signal was validated at construction");
        // SAFETY: the pointer targets the pinned root that the guard keeps alive.
        unsafe { &*(pointer as *const T) }
    }
}

impl<T: 'static + fmt::Display> fmt::Display for ProjectedRead<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value: &T = self;
        fmt::Display::fmt(value, f)
    }
}

impl<T: 'static + fmt::Debug> fmt::Debug for ProjectedRead<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value: &T = self;
        fmt::Debug::fmt(value, f)
    }
}

/// Creates a projected signal from a matched reference and one pattern.
///
/// The reference proves the path is present, and its type must match the pattern's payload, so a
/// pattern for a different variant is a compile error.
#[macro_export]
macro_rules! project {
    ($signal:expr, $proof:expr, $pattern:pat => $value:ident) => {{
        #[allow(unreachable_patterns)]
        let accessor = $crate::projected::Accessor::new(
            |root| match root {
                $pattern => Some($value),
                _ => None,
            },
            |root| match root {
                $pattern => Some($value),
                _ => None,
            },
        );
        $crate::projected::ProjectedSignal::new(&$signal, accessor, $proof)
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::WriterDriver;
    use std::time::Duration;

    #[derive(Clone, Debug, PartialEq)]
    enum Custom<T> {
        Value(T),
    }

    #[derive(Clone, Debug, PartialEq)]
    struct Numbered(u32);

    #[derive(Clone, Debug, PartialEq)]
    struct Counter(Option<Result<Custom<Numbered>, ()>>);

    #[derive(Clone, Debug, PartialEq)]
    struct Tint(u8);

    fn signal(driver: &WriterDriver<Counter>) -> QueuedSignal<Counter> {
        QueuedSignal::from_parts(driver.queued_state.clone(), None, driver.op_tx.clone())
    }

    fn tint_signal(driver: &WriterDriver<Tint>) -> QueuedSignal<Tint> {
        QueuedSignal::from_parts(driver.queued_state.clone(), None, driver.op_tx.clone())
    }

    fn present(value: u32) -> Counter {
        Counter(Some(Ok(Custom::Value(Numbered(value)))))
    }

    fn projected(source: &QueuedSignal<Counter>) -> ProjectedSignal<Numbered> {
        let pinned = source.state.cell.read_typed();
        let proof = match &pinned.0 {
            Some(Ok(Custom::Value(value))) => value,
            _ => panic!("value is present"),
        };
        project!(
            source,
            proof,
            Counter(Some(Ok(Custom::Value(value)))) => value
        )
    }

    fn tinted(source: &QueuedSignal<Tint>) -> ProjectedSignal<u8> {
        let pinned = source.state.cell.read_typed();
        let proof = &pinned.0;
        project!(source, proof, Tint(value) => value)
    }

    #[test]
    fn projects_through_nested_options() {
        let driver = WriterDriver::new(present(7));
        assert_eq!(*projected(&signal(&driver)).read(), Numbered(7));
    }

    #[test]
    fn works_for_unrelated_root_types() {
        let driver = WriterDriver::new(Tint(9));
        assert_eq!(*tinted(&tint_signal(&driver)).read(), 9);
    }

    #[test]
    fn projected_mutation_reaches_the_root() {
        let mut driver = WriterDriver::new(present(1));
        let number = projected(&signal(&driver));

        number.mutate(|value: &mut Numbered| value.0 = 9);
        driver.tick(Duration::ZERO);

        assert_eq!(*number.read(), Numbered(9));
    }

    #[test]
    fn projected_mutation_set_replaces_the_field() {
        let mut driver = WriterDriver::new(present(1));
        let number = projected(&signal(&driver));

        number.mutate_set(|value: &mut Numbered| value.0 = 4);
        driver.tick(Duration::ZERO);

        assert_eq!(*number.read(), Numbered(4));
    }

    #[test]
    fn writes_are_skipped_when_the_path_vanishes() {
        let mut driver = WriterDriver::new(present(1));
        let number = projected(&signal(&driver));

        driver.set_value(Counter(None));
        driver.tick(Duration::ZERO);
        number.mutate(|value: &mut Numbered| value.0 = 9);
        driver.tick(Duration::ZERO);

        assert_eq!(*driver.queued_state.read(), Counter(None));
    }

    #[test]
    fn rebuilt_projections_compare_equal() {
        let driver = WriterDriver::new(present(3));
        let first = projected(&signal(&driver));
        let second = projected(&signal(&driver));

        assert_eq!(first, second);
        assert_eq!(first, first.clone());
    }

    #[test]
    fn projections_into_different_paths_differ() {
        let driver = WriterDriver::new(present(3));
        let number = projected(&signal(&driver));
        let driver_two = WriterDriver::new(present(3));
        let other = projected(&signal(&driver_two));

        assert_ne!(number, other);
    }

    #[test]
    fn reads_follow_the_published_generation() {
        let mut driver = WriterDriver::new(present(1));
        let number = projected(&signal(&driver));

        driver.set_value(present(6));
        driver.tick(Duration::ZERO);

        assert_eq!(*number.read(), Numbered(6));
    }
}

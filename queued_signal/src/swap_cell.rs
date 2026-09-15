use std::{ops::Deref, ptr::NonNull, sync::{Arc, atomic::{AtomicUsize, Ordering}}};

/// Single-buffer cell shared between one writer and many readers.
pub struct SwapCellSync<T: Send + Sync> {
    buffer1: NonNull<T>,
    readers: Arc<AtomicUsize>,
}

/// Borrow of the stored value, blocking writes until dropped.
pub struct ReadGuard<T: Send + Sync> {
    ptr: NonNull<T>,
    readers: Arc<AtomicUsize>,
}

impl<T: Send + Sync> Deref for ReadGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard owns an active reader count, so the buffer
        // cannot be dropped or swapped while this guard is alive.
        unsafe { self.ptr.as_ref() }
    }
}

impl<T: Send + Sync> Drop for ReadGuard<T> {
    fn drop(&mut self) {
        self.readers.fetch_sub(1, Ordering::Release);
    }
}

/// Shared read handle cloned for readers.
pub struct ReadHandle<T: Send + Sync> {
    ptr: NonNull<T>,
    readers: Arc<AtomicUsize>,
}

impl<T: Send + Sync> Clone for ReadHandle<T> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            readers: self.readers.clone(),
        }
    }
}

impl<T: Send + Sync> ReadHandle<T> {
    /// Borrows the stored value, blocking writes until the guard drops.
    pub fn read(&self) -> ReadGuard<T> {
        self.readers.fetch_add(1, Ordering::Acquire);
        ReadGuard {
            ptr: self.ptr,
            readers: self.readers.clone(),
        }
    }
}

impl<T: Send + Sync> SwapCellSync<T> {
    pub fn new(value: T) -> Self {
        // Box::into_raw hands ownership of the heap allocation to the returned
        // raw pointer, which keeps a Unique tag and survives the struct move.
        let buffer1 = unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(value))) };
        Self {
            buffer1,
            readers: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns a shared read handle for readers.
    pub fn share(&self) -> ReadHandle<T> {
        ReadHandle {
            ptr: self.buffer1,
            readers: self.readers.clone(),
        }
    }

    /// Attempts to swap the pointed-to value with `with`, requiring no
    /// active readers.
    pub fn try_swap(&mut self, with: &mut T) -> Result<(), usize> {
        let count = self.readers.load(Ordering::Acquire);
        if count == 0 {
            // SAFETY: buffer1 owns a valid heap allocation and does not
            // alias the caller's with reference.
            unsafe { std::ptr::swap(self.buffer1.as_ptr(), with as *mut T) };
            Ok(())
        } else {
            Err(count)
        }
    }

    pub fn read(&self) -> ReadGuard<T> {
        self.readers.fetch_add(1, Ordering::Acquire);
        ReadGuard {
            ptr: self.buffer1,
            readers: self.readers.clone(),
        }
    }

    /// Returns a mutable reference to the stored value when no readers exist.
    pub fn get_mut(&mut self) -> Result<&mut T, usize> {
        let count = self.readers.load(Ordering::Acquire);
        if count == 0 {
            // SAFETY: no active readers exist, so buffer1 can be handed
            // out mutably.
            Ok(unsafe { &mut *self.buffer1.as_ptr() })
        } else {
            Err(count)
        }
    }
}

impl<T: Send + Sync> Drop for SwapCellSync<T> {
    fn drop(&mut self) {
        // SAFETY: buffer1 owns the allocation returned by Box::into_raw and is
        // freed exactly once here.
        unsafe { drop(Box::from_raw(self.buffer1.as_ptr())) };
    }
}

// SAFETY: the buffer owns a uniquely held allocation. Readers only produce
// shared references, which is sound because T is Sync, and writes only occur
// through &mut self.
unsafe impl<T: Send + Sync> Send for SwapCellSync<T> {}
unsafe impl<T: Send + Sync> Sync for SwapCellSync<T> {}

// SAFETY: the guard holds an active reader count and only dereferences the
// pointer as &T, which is sound because T is Sync.
unsafe impl<T: Send + Sync> Send for ReadGuard<T> {}
unsafe impl<T: Send + Sync> Sync for ReadGuard<T> {}

// SAFETY: the read handle only ever dereferences the pointer as &T.
unsafe impl<T: Send + Sync> Send for ReadHandle<T> {}
unsafe impl<T: Send + Sync> Sync for ReadHandle<T> {}

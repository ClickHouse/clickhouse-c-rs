use core::alloc::GlobalAlloc;
use core::ffi::c_void;

use crate::sys;

/// Allocator used for memory owned by clickhouse-c.
///
/// Each owning handle retains its allocator and uses it when releasing memory.
/// Allocator contains only function pointers and can be copied or shared
/// between threads.
#[derive(Clone, Copy)]
pub struct Allocator {
    pub(crate) raw: sys::chc_alloc,
}

impl Allocator {
    /// Returns clickhouse-c allocator backed by `malloc`, `realloc`, and `free`.
    pub fn stdlib() -> Self {
        let raw = unsafe { sys::chc_alloc_stdlib() };
        Self { raw }
    }

    /// Creates a clickhouse-c allocator backed by a Rust [`GlobalAlloc`].
    ///
    /// Allocator reference is stored in C user data and must remain valid for
    /// every object that uses it. Allocations use `align_of::<u128>()`, which
    /// matches alignment provided by [`stdlib`](Self::stdlib).
    pub fn global<A: GlobalAlloc + Sync>(a: &'static A) -> Self {
        Self {
            raw: sys::chc_alloc {
                ud: a as *const A as *mut c_void,
                alloc: Some(vtable::alloc::<A>),
                realloc: Some(vtable::realloc::<A>),
                free: Some(vtable::free::<A>),
            },
        }
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const sys::chc_alloc {
        &self.raw
    }
}

impl Default for Allocator {
    fn default() -> Self {
        Self::stdlib()
    }
}

unsafe impl Send for Allocator {}
unsafe impl Sync for Allocator {}

mod vtable {
    use core::alloc::{GlobalAlloc, Layout};
    use core::ffi::c_void;

    // Match alignment used by clickhouse-c for 16-byte scalar values
    const ALIGN: usize = core::mem::align_of::<u128>();

    #[inline]
    fn layout(bytes: usize) -> Option<Layout> {
        Layout::from_size_align(bytes.max(1), ALIGN).ok()
    }

    pub extern "C" fn alloc<A: GlobalAlloc + Sync>(ud: *mut c_void, bytes: usize) -> *mut c_void {
        let a = unsafe { &*ud.cast::<A>() };
        let Some(layout) = layout(bytes) else {
            return core::ptr::null_mut();
        };
        unsafe { a.alloc(layout).cast() }
    }

    pub extern "C" fn realloc<A: GlobalAlloc + Sync>(
        ud: *mut c_void,
        p: *mut c_void,
        old_bytes: usize,
        new_bytes: usize,
    ) -> *mut c_void {
        if p.is_null() {
            return alloc::<A>(ud, new_bytes);
        }
        let a = unsafe { &*ud.cast::<A>() };
        let Some(old_layout) = layout(old_bytes) else {
            return core::ptr::null_mut();
        };
        if new_bytes == 0 {
            unsafe { a.dealloc(p.cast(), old_layout) };
            return core::ptr::null_mut();
        }
        unsafe { a.realloc(p.cast(), old_layout, new_bytes).cast() }
    }

    pub extern "C" fn free<A: GlobalAlloc + Sync>(ud: *mut c_void, p: *mut c_void, bytes: usize) {
        if p.is_null() {
            return;
        }
        let a = unsafe { &*ud.cast::<A>() };
        let Some(layout) = layout(bytes) else {
            return;
        };
        unsafe { a.dealloc(p.cast(), layout) }
    }
}

#[cfg(test)]
mod tests {
    use core::alloc::{GlobalAlloc, Layout};
    use core::ffi::c_void;
    use core::sync::atomic::{AtomicBool, Ordering};
    use std::alloc::System;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::Allocator;
    use crate::sys;
    use crate::{BlockBuilder, ColumnBuilder, TypeAst};

    /// Allocator that records every live block and the layout it was given.
    struct CheckedAlloc {
        live: Mutex<HashMap<usize, Layout>>,
        invalid_layout: AtomicBool,
    }

    impl CheckedAlloc {
        /// Leaked because [`Allocator::global`] stores the reference in C user
        /// data. Each test owns its own instance, so a deliberate mismatch in
        /// one cannot be read by another.
        fn leaked() -> &'static Self {
            Box::leak(Box::new(Self {
                live: Mutex::new(HashMap::new()),
                invalid_layout: AtomicBool::new(false),
            }))
        }

        fn take(&self, ptr: *mut u8, layout: Layout) -> Option<Layout> {
            let actual = self
                .live
                .lock()
                .expect("checked allocator lock")
                .remove(&(ptr as usize));
            match actual {
                Some(actual) if actual == layout => Some(actual),
                Some(actual) => {
                    self.invalid_layout.store(true, Ordering::Relaxed);
                    Some(actual)
                }
                None => {
                    self.invalid_layout.store(true, Ordering::Relaxed);
                    None
                }
            }
        }

        fn is_clean(&self) -> bool {
            !self.invalid_layout.load(Ordering::Relaxed)
                && self.live.lock().expect("checked allocator lock").is_empty()
        }
    }

    unsafe impl GlobalAlloc for CheckedAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                self.live
                    .lock()
                    .expect("checked allocator lock")
                    .insert(ptr as usize, layout);
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            let Some(actual) = self.take(ptr, layout) else {
                return;
            };
            unsafe { System.dealloc(ptr, actual) };
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let Some(actual) = self.take(ptr, layout) else {
                return core::ptr::null_mut();
            };
            let new_ptr = unsafe { System.realloc(ptr, actual, new_size) };
            let mut live = self.live.lock().expect("checked allocator lock");
            if new_ptr.is_null() {
                live.insert(ptr as usize, actual);
            } else {
                let new_layout = Layout::from_size_align(new_size, actual.align()).expect("layout");
                live.insert(new_ptr as usize, new_layout);
            }
            new_ptr
        }
    }

    fn ud(a: &'static CheckedAlloc) -> *mut c_void {
        (a as *const CheckedAlloc).cast_mut().cast()
    }

    // C calls these through the table, so tests do too
    fn call_alloc(raw: &sys::chc_alloc, ud: *mut c_void, bytes: usize) -> *mut c_void {
        unsafe { raw.alloc.expect("alloc callback")(ud, bytes) }
    }

    fn call_realloc(
        raw: &sys::chc_alloc,
        ud: *mut c_void,
        p: *mut c_void,
        old_bytes: usize,
        new_bytes: usize,
    ) -> *mut c_void {
        unsafe { raw.realloc.expect("realloc callback")(ud, p, old_bytes, new_bytes) }
    }

    fn call_free(raw: &sys::chc_alloc, ud: *mut c_void, p: *mut c_void, bytes: usize) {
        unsafe { raw.free.expect("free callback")(ud, p, bytes) }
    }

    #[test]
    fn default_allocator_serves_c_requests() {
        let raw = Allocator::default().raw;
        assert!(raw.ud.is_null());
        let p = call_alloc(&raw, raw.ud, 32);
        assert!(!p.is_null());
        call_free(&raw, raw.ud, p, 32);
    }

    #[test]
    fn global_allocator_preserves_layouts() {
        let checked = CheckedAlloc::leaked();
        let alloc = Allocator::global(checked);
        let ty = TypeAst::parse("UInt32", alloc).expect("UInt32");
        let data = 7u32.to_le_bytes();
        let col = ColumnBuilder::fixed(&data, ty.view().elem_size(), 1).expect("fixed");
        let mut builder = BlockBuilder::new();
        builder.append("x", ty.view(), &col).expect("append");
        drop(builder);
        drop(ty);
        assert!(checked.is_clean());
    }

    // C reallocates its own buffers; wrapper must route growth and release
    #[test]
    fn realloc_grows_shrinks_and_releases() {
        let checked = CheckedAlloc::leaked();
        let raw = Allocator::global(checked).raw;
        let ud = ud(checked);

        let p = call_alloc(&raw, ud, 16);
        assert!(!p.is_null());
        let grown = call_realloc(&raw, ud, p, 16, 64);
        assert!(!grown.is_null());
        let shrunk = call_realloc(&raw, ud, grown, 64, 8);
        assert!(!shrunk.is_null());
        call_free(&raw, ud, shrunk, 8);
        assert!(checked.is_clean());
    }

    // C may hand a null pointer to either wrapper
    #[test]
    fn null_pointer_reallocates_as_a_fresh_block() {
        let checked = CheckedAlloc::leaked();
        let raw = Allocator::global(checked).raw;
        let ud = ud(checked);

        let p = call_realloc(&raw, ud, core::ptr::null_mut(), 0, 32);
        assert!(!p.is_null());
        call_free(&raw, ud, p, 32);
        call_free(&raw, ud, core::ptr::null_mut(), 32);
        assert!(checked.is_clean());
    }

    // Zero-byte requests still need a nonzero layout, releasing frees instead
    #[test]
    fn zero_sized_requests_round_up_and_release() {
        let checked = CheckedAlloc::leaked();
        let raw = Allocator::global(checked).raw;
        let ud = ud(checked);

        let p = call_alloc(&raw, ud, 0);
        assert!(!p.is_null());
        assert!(call_realloc(&raw, ud, p, 1, 0).is_null());
        assert!(checked.is_clean());
    }

    // A corrupt length field must reach the OOM path, not an invalid layout
    #[test]
    fn unrepresentable_length_returns_null() {
        let checked = CheckedAlloc::leaked();
        let raw = Allocator::global(checked).raw;
        let ud = ud(checked);

        assert!(call_alloc(&raw, ud, usize::MAX).is_null());
        let p = call_alloc(&raw, ud, 16);
        assert!(call_realloc(&raw, ud, p, usize::MAX, 16).is_null());
        // Bogus length leaves the block alone rather than freeing it wrongly
        call_free(&raw, ud, p, usize::MAX);
        call_free(&raw, ud, p, 16);
        assert!(checked.is_clean());
    }

    // Harness itself must notice a wrapper that loses a layout
    #[test]
    fn checked_allocator_reports_a_layout_mismatch() {
        let checked = CheckedAlloc::leaked();
        let layout = Layout::from_size_align(16, 16).expect("layout");
        let p = unsafe { checked.alloc(layout) };
        unsafe { checked.dealloc(p, Layout::from_size_align(8, 16).expect("layout")) };
        assert!(!checked.is_clean());
    }

    #[test]
    fn checked_allocator_reports_an_unknown_block() {
        let checked = CheckedAlloc::leaked();
        let layout = Layout::from_size_align(16, 16).expect("layout");
        let mut stack = 0u128;
        let stray = (&mut stack as *mut u128).cast::<u8>();
        assert!(unsafe { checked.realloc(stray, layout, 32) }.is_null());
        unsafe { checked.dealloc(stray, layout) };
        assert!(!checked.is_clean());
    }

    // Failed growth must leave the original block registered
    #[test]
    fn failed_growth_keeps_the_original_block() {
        let checked = CheckedAlloc::leaked();
        let layout = Layout::from_size_align(16, 16).expect("layout");
        let p = unsafe { checked.alloc(layout) };
        // Large enough to fail, small enough to stay a valid layout
        let unservable = isize::MAX as usize / 2;
        assert!(unsafe { checked.realloc(p, layout, unservable) }.is_null());
        unsafe { checked.dealloc(p, layout) };
        assert!(checked.is_clean());
    }

    #[test]
    fn stdlib_vtable_round_trips_through_c() {
        let raw = Allocator::stdlib().raw;
        let p = call_alloc(&raw, raw.ud, 8);
        assert!(!p.is_null());
        let grown = call_realloc(&raw, raw.ud, p, 8, 24);
        assert!(!grown.is_null());
        call_free(&raw, raw.ud, grown, 24);
    }
}

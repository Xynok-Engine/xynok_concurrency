#![cfg(not(loom))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use xynok_concurrency::thread_pool::ThreadPool;
use xynok_concurrency::thread_pool::cfg::CfgThreadPool;

struct CountingAllocator;
static TRACK: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator
{
    unsafe fn alloc(&self, layout: Layout) -> *mut u8
    {
        if TRACK.load(Ordering::Relaxed)
        {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8
    {
        if TRACK.load(Ordering::Relaxed)
        {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8
    {
        if TRACK.load(Ordering::Relaxed)
        {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout)
    {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn batch_dispatch_does_not_allocate_on_caller_or_workers()
{
    let pool = ThreadPool::new(CfgThreadPool::new("allocation-test", 4).with_capacity(128, 128));
    let completed = AtomicUsize::new(0);
    // Let OS worker startup and parking finish before measuring dispatch.
    std::thread::sleep(std::time::Duration::from_millis(50));
    // Include the first batch: inbox storage must already exist at pool construction.
    TRACK.store(true, Ordering::SeqCst);
    for _ in 0..64
    {
        pool.run_batch((0..64).map(|_| {
            || {
                completed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    TRACK.store(false, Ordering::SeqCst);
    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), 0);
    assert_eq!(completed.load(Ordering::Relaxed), 64 * 64);
}

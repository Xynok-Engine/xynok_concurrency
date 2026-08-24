//! src: https://doc.rust-lang.org/std/task/struct.RawWakerVTable.html
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, MaybeUninit};

use crate::sync::cell::UnsafeCell;

// TODO: need a test with a 128-byte value for x86_64
const INLINE_BYTES: usize = 48;

#[repr(C, align(16))]
struct FnBuffer
{
    buffer: [MaybeUninit<u8>; INLINE_BYTES],
}
#[repr(C)]
pub struct InlineFn
{
    vtable:    &'static VTable,
    fn_buffer: UnsafeCell<FnBuffer>,
}
unsafe impl Send for InlineFn {}

struct VTable
{
    runner:  fn(*mut FnBuffer),
    dropper: fn(*mut FnBuffer),
}
struct VTableAlias<F>(PhantomData<F>);
pub trait Runnable: FnOnce() + Send + 'static {}
impl<F> Runnable for F where F: FnOnce() + Send + 'static {}

impl InlineFn
{
    pub const fn is_fit<F>() -> bool
    {
        size_of::<F>() <= INLINE_BYTES && align_of::<F>() <= align_of::<FnBuffer>()
    }
    #[inline]
    pub fn new<F: Runnable>(f: F) -> Self
    {
        let mut f_box = FnBuffer::new();

        let v_table = unsafe {
            match Self::is_fit::<F>()
            {
                true =>
                {
                    f_box.as_mut_ptr().cast::<F>().write(f);
                    VTableAlias::<F>::INLINE
                }
                false =>
                {
                    f_box.as_mut_ptr().cast::<Box<F>>().write(Box::new(f));
                    VTableAlias::<F>::BOXED
                }
            }
        };
        Self {
            vtable:    v_table,
            fn_buffer: UnsafeCell::new(f_box),
        }
    }
    #[inline]
    pub fn run_once(self)
    {
        let this = ManuallyDrop::new(self);
        (this.vtable.runner)(this.fn_buffer.with_mut(|p| p))
    }
    //#[inline]
    //pub fn run(&self)
    //{
    //    (self.v_table.runner)(self.fn_boxed.with_mut(|p| p))
    //}
}

impl Drop for InlineFn
{
    fn drop(&mut self)
    {
        (self.vtable.dropper)(self.fn_buffer.with_mut(|p| p));
    }
}
impl std::fmt::Debug for InlineFn
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("InlineFn").finish_non_exhaustive()
    }
}

impl<F: Runnable> VTableAlias<F>
{
    const INLINE: &VTable = &VTable {
        runner:  Self::run_inline,
        dropper: Self::drop_inline,
    };
    const BOXED: &VTable = &VTable {
        runner:  Self::run_boxed,
        dropper: Self::drop_boxed,
    };
    fn run_inline(f_box: *mut FnBuffer)
    {
        let f = unsafe { f_box.cast::<F>().read() };
        f();
    }
    fn drop_inline(f_box: *mut FnBuffer)
    {
        unsafe {
            f_box.cast::<F>().drop_in_place();
        }
    }
    fn run_boxed(f_box: *mut FnBuffer)
    {
        let f = unsafe { f_box.cast::<Box<F>>().read() };
        f();
    }
    fn drop_boxed(f_box: *mut FnBuffer)
    {
        unsafe {
            f_box.cast::<Box<F>>().drop_in_place();
        }
    }
}
impl FnBuffer
{
    #[inline]
    pub fn new() -> Self
    {
        Self {
            buffer: [MaybeUninit::uninit(); INLINE_BYTES],
        }
    }
    #[inline]
    fn as_mut_ptr(&mut self) -> *mut u8
    {
        self.buffer.as_mut_ptr().cast()
    }
}

#[cfg(all(test, not(loom)))]
mod test
{
    use super::*;
    use crate::sync::{Arc, AtomicUsize, Ordering};
    use crate::utils::queue_batching::QueueBatching;

    fn empty_fn() {}
    fn params_fn(_a: u64) {}
    fn params_fn2(_a: u64, _b: u64) {}
    #[test]
    fn size_check()
    {
        println!("size_of_val(&empty_fn):   {} bytes (fn item, ZST)", size_of_val(&empty_fn));
        println!("size_of_val(&params_fn):  {} bytes (fn item, ZST)", size_of_val(&params_fn));
        println!("size_of_val(&params_fn2): {} bytes (fn item, ZST)", size_of_val(&params_fn2));

        let p0: fn() = empty_fn;
        let p1: fn(u64) = params_fn;
        let p2: fn(u64, u64) = params_fn2;
        let p3 = || {
            let x: u64 = 10;
            let y: [u64; 20] = [1u64; 20];
            for i in y
            {
                println!("x {}: - i:{}", x, i);
            }
        };
        println!("size_of::<fn()>():        {} bytes (fn pointer)", size_of_val(&p0));
        println!("size_of::<fn(u64)>():     {} bytes (fn pointer)", size_of_val(&p1));
        println!("size_of::<fn(u64,u64)>(): {} bytes (fn pointer)", size_of_val(&p2));
        println!("size_of::<fn_closure>():  {} bytes (fn pointer)", size_of_val(&p3));

        println!("size of FnBox:   {} bytes", size_of::<FnBuffer>());
        println!("size of Vtable:  {} bytes", size_of::<VTable>());
    }

    #[test]
    fn mot_cap_cache_line()
    {
        assert_eq!(size_of::<InlineFn>(), 64);
        assert_eq!(align_of::<InlineFn>(), 16);

        assert_eq!(align_of::<FnBuffer>() + INLINE_BYTES, 64, "vtable + đệm + buffer phải lấp kín 64 byte");
    }

    #[test]
    fn closure_nho_khong_vao_heap()
    {
        assert!(InlineFn::is_fit::<[u8; INLINE_BYTES]>());
        assert!(!InlineFn::is_fit::<[u8; INLINE_BYTES + 1]>());

        let id = 7u32;
        let closure = move || assert_eq!(id, 7);
        assert!(
            size_of_val(&closure) <= INLINE_BYTES,
            "closure bắt một u32 mà không nằm tại chỗ thì hỏng hết ý nghĩa"
        );
        InlineFn::new(closure).run_once();
    }

    #[test]
    fn closure_to_van_chay_dung()
    {
        let big = [9u64; 32];
        let job = InlineFn::new(move || assert_eq!(big[31], 9));
        job.run_once();
    }

    #[test]
    fn tha_ma_khong_chay()
    {
        struct Bomb(Arc<AtomicUsize>);

        impl Drop for Bomb
        {
            fn drop(&mut self)
            {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        let ran = Arc::new(AtomicUsize::new(0));

        for big in [false, true]
        {
            let bomb = Bomb(Arc::clone(&dropped));
            let ran = Arc::clone(&ran);
            let padding = [0u8; INLINE_BYTES];

            let job = match big
            {
                false => InlineFn::new(move || {
                    let _ = &bomb;
                    ran.fetch_add(1, Ordering::Relaxed);
                }),
                true => InlineFn::new(move || {
                    let _ = (&bomb, &padding);
                    ran.fetch_add(1, Ordering::Relaxed);
                }),
            };
            drop(job);
        }

        assert_eq!(dropped.load(Ordering::Relaxed), 2, "closure bị thả phải kéo theo mọi thứ nó bắt");
        assert_eq!(ran.load(Ordering::Relaxed), 0, "thả thì không được chạy");
    }

    #[test]
    fn chay_qua_queue_batching()
    {
        let counter = Arc::new(AtomicUsize::new(0));
        let queue = QueueBatching::new();

        queue.push_batch((0..100).map(|i| {
            let counter = Arc::clone(&counter);
            InlineFn::new(move || {
                counter.fetch_add(i, Ordering::Relaxed);
            })
        }));

        let mut ran = 0;
        let mut batch = Vec::new();
        while queue.pop_batch(&mut batch, 16) > 0
        {
            for job in batch.drain(..)
            {
                job.run_once();
                ran += 1;
            }
        }

        assert_eq!(ran, 100, "mọi job đẩy vào đều phải được rút ra và chạy đúng một lần");
        assert_eq!(counter.load(Ordering::Relaxed), (0..100).sum::<usize>());
    }

    #[test]
    fn queue_drop_keo_theo_job_chua_chay()
    {
        let alive = Arc::new(AtomicUsize::new(0));

        {
            let queue = QueueBatching::new();
            queue.push_batch((0..50).map(|_| {
                let alive = Arc::clone(&alive);
                alive.fetch_add(1, Ordering::Relaxed);
                InlineFn::new(move || {
                    alive.fetch_sub(1, Ordering::Relaxed);
                })
            }));
        }

        // `Arc` trong closure bị thả theo job, nên strong count về 1 (chỉ còn `alive` ở đây).
        assert_eq!(Arc::strong_count(&alive), 1, "job chưa chạy vẫn phải được thả sạch");
    }
}

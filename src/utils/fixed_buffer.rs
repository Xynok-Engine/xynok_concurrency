use crate::collection::ring_buffer::consts::MAX_CAPACITY;
use crate::sync::cell::UnsafeCell;
use std::mem::MaybeUninit;

pub struct FixedRingBuffer<T>
{
    cells: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask:  u32,
}

impl<T> FixedRingBuffer<T>
{
    #[track_caller]
    pub fn new(capacity: usize) -> Self
    {
        debug_assert!(
            capacity > 0,
            "`{}` capacity `{}` must be greater than 0 !",
            std::any::type_name::<Self>(),
            capacity
        );
        debug_assert!(
            capacity < MAX_CAPACITY,
            "`{}` capacity `{}` exceeds `{}` limit for wrapping u32 indices",
            std::any::type_name::<Self>(),
            capacity,
            MAX_CAPACITY
        );
        debug_assert!(
            capacity.is_power_of_two(),
            "`{}` is initizealed with capacity `{}` is not power of 2 !",
            std::any::type_name::<Self>(),
            capacity
        );
        Self {
            cells: allocate(capacity),
            mask:  (capacity - 1) as u32,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.mask as usize + 1
    }

    #[inline]
    pub fn mask(&self) -> u32
    {
        self.mask
    }

    #[inline]
    fn at(&self, cursor: u32) -> &UnsafeCell<MaybeUninit<T>>
    {
        unsafe { self.cells.get_unchecked((cursor & self.mask) as usize) }
    }

    #[inline]
    pub unsafe fn write(&self, cursor: u32, val: T)
    {
        self.at(cursor).with_mut(|p| unsafe {
            (*p).write(val);
        });
    }

    #[inline]
    pub unsafe fn take_at(&self, cursor: u32) -> T
    {
        self.at(cursor).with(|p| unsafe { (*p).assume_init_read() })
    }

    #[inline]
    pub unsafe fn drop_at(&self, cursor: u32)
    {
        self.at(cursor).with_mut(|p| unsafe { (*p).assume_init_drop() });
    }
}

#[cfg(not(loom))]
fn allocate<T>(total_slots: usize) -> Box<[UnsafeCell<MaybeUninit<T>>]>
{
    unsafe { Box::new_uninit_slice(total_slots).assume_init() }
}

#[cfg(loom)]
fn allocate<T>(total_slots: usize) -> Box<[UnsafeCell<MaybeUninit<T>>]>
{
    (0..total_slots).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect()
}

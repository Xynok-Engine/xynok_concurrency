use crate::sync::cell::UnsafeCell;
use std::mem::MaybeUninit;

pub struct FixedBuffer<T>
{
    cells: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask:  u32,
}

impl<T> FixedBuffer<T>
{
    pub fn new(capacity: u32) -> Self
    {
        debug_assert!(
            capacity.is_power_of_two(),
            "`{}` is initizealed with capacity `{}` is not power of 2 !",
            std::any::type_name::<Self>(),
            capacity
        );
        Self {
            cells: allocate(capacity as usize),
            mask:  capacity - 1,
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
    pub unsafe fn read(&self, cursor: u32) -> T
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

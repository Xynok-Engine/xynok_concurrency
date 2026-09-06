#[rustfmt::skip] #[cfg(not(any(loom, miri)))] pub const SPIN_LIMIT: u32 = 6; // maxmium is 2^6
#[rustfmt::skip] #[cfg(not(any(loom, miri)))] pub const YIELD_LIMIT: u32 = 10;

#[rustfmt::skip] #[cfg(any(loom, miri))] pub const SPIN_LIMIT: u32 = 0;
#[rustfmt::skip] #[cfg(any(loom, miri))] pub const YIELD_LIMIT: u32 = 1;

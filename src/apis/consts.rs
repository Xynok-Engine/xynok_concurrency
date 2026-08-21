#[rustfmt::skip] #[cfg(not(loom))] pub const SPIN_LIMIT: u32 = 6;
#[rustfmt::skip] #[cfg(not(loom))] pub const YIELD_LIMIT: u32 = 10;

#[rustfmt::skip] #[cfg(loom)] pub const SPIN_LIMIT: u32 = 0;
#[rustfmt::skip] #[cfg(loom)] pub const YIELD_LIMIT: u32 = 1;

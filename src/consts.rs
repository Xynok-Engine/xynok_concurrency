pub const SPIN_LIMIT: u32 = 4;

/// How far the CAS loop in [`crate::atomic`] doubles its spin count before it stops growing.
/// `6` caps a single backoff at 64 spins, which is the same order as [`SPIN_LIMIT`].
pub const CAS_BACKOFF_CAP: u32 = 6;

/// After this many failed CAS attempts, spinning has clearly lost and the thread yields instead.
pub const CAS_YIELD_LIMIT: u32 = 8;

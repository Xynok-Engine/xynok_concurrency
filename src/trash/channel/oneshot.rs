use std::mem::MaybeUninit;

use crate::channel::inner::Inner;
use crate::channel::receiver::Receiver;
use crate::channel::sender::Sender;
use crate::sync::cell::UnsafeCell;
use crate::sync::{Arc, AtomicBool, Mutex};

/// Dựng một kênh một lần.
///
/// ```
/// use xynok_concurrency::channel::oneshot;
/// use xynok_concurrency::pool::{Config, ThreadPool};
///
/// let pool = ThreadPool::new(Config {
///     threads: 2,
///     ..Default::default()
/// });
/// let (tx, rx) = oneshot();
///
/// pool.spawn(move || tx.send(6 * 7));
///
/// assert_eq!(rx.recv_in(&pool), Some(42));
/// ```
pub fn oneshot<T>() -> (Sender<T>, Receiver<T>)
{
    let inner = Arc::new(Inner {
        ready:   AtomicBool::new(false),
        dropped: AtomicBool::new(false),
        value:   UnsafeCell::new(MaybeUninit::uninit()),
        waiter:  Mutex::new(None),
    });

    (Sender { inner: Arc::clone(&inner) }, Receiver { inner: inner })
}

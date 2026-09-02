//! ## Một lớp áo chung cho std và loom
//!
//! Crate này được kiểm bằng loom, và loom chỉ soi được những thao tác đồng thời do chính nó cài
//! đặt. Nghĩa là mọi biến nguyên tử, mọi ô nhớ chia sẻ, mọi thao tác thread đều phải đổi sang bản
//! của loom khi chạy kiểm, rồi đổi ngược lại khi build thật.
//!
//! Rải `#[cfg]` khắp nơi để làm việc đó thì code chính sẽ đầy nhiễu, và chỉ cần sót một chỗ là
//! loom nhìn không thấy, kiểm xong vẫn tưởng là sạch.
//!
//! ### Cách hoạt động
//!
//! Cả crate chỉ mượn kiểu từ đây, không mượn thẳng từ std. Chỗ này quyết định một lần duy nhất là
//! lấy bản của ai, phần còn lại không cần biết.
//!
//! > [!IMPORTANT]
//! > Đừng mượn thẳng std trong code của crate. Một chỗ lách thôi là loom mất dấu đúng cái đoạn cần
//! > soi nhất.

#![allow(unused)]

pub(crate) mod cell;
pub(crate) mod hints;
pub(crate) mod thread;

pub(crate) use hints::{spin_loop, yield_now};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{
    AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence,
};
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Condvar, Mutex};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{
    AtomicBool, AtomicI8, AtomicI16, AtomicI32, AtomicI64, AtomicIsize, AtomicPtr, AtomicU8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering, fence,
};
#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Condvar, Mutex};

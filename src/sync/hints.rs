/// Gợi ý cho CPU rằng đây là một vòng chờ, để nó bớt ăn tài nguyên của core anh em.
///
/// Dưới loom thì thành nhường lượt, vì loom cần một điểm cắt thật để thử các thứ tự khác nhau chứ
/// không hiểu gợi ý của phần cứng.
#[inline]
pub(crate) fn spin_loop()
{
    #[cfg(not(loom))]
    std::hint::spin_loop();

    #[cfg(loom)]
    loom::thread::yield_now();
}

/// Trả lượt chạy lại cho hệ điều hành, để thread khác có cơ hội tiến lên.
#[inline]
pub(crate) fn yield_now()
{
    std::thread::yield_now();
}

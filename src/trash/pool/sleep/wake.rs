/// Kết quả một lượt đi ngủ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wake
{
    /// Ngủ thật, và được ai đó đánh thức. Người đánh thức đã tính worker này vào `searching`, nên
    /// nó thức dậy với tư cách người đang lùng việc.
    Notified,
    /// Chưa kịp ngủ: lần ngó lại cuối cùng thấy có việc, nên tự rút tên khỏi danh sách.
    Cancelled,
}

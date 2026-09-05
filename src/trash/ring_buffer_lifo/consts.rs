/// Trần số ô của một ring.
///
/// Con trỏ chạy vòng bằng số 32 bit và mọi phép so sánh đều dựa trên hiệu của hai con trỏ, mà ở
/// đây hiệu ấy còn được đọc như số có dấu để biết ring rỗng hay đã âm. Nên khoảng cách giữa hai
/// con trỏ phải nằm gọn trong một phần tư vòng số.
pub const MAX_SLOTS: u32 = 1 << 30;

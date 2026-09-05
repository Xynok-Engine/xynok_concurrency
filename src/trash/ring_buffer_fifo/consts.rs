/// Trần số ô của một ring.
///
/// Con trỏ chạy vòng bằng số 32 bit và mọi phép so sánh đều dựa trên hiệu của hai con trỏ, nên
/// khoảng cách giữa chúng phải luôn nhỏ hơn nửa vòng số. Vượt qua đó thì không phân biệt được
/// "đi trước" với "đi sau" nữa.
pub const MAX_SLOTS: u32 = 1 << 31;

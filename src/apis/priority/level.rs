/// Hai mức mà mọi nền tảng đều diễn dịch được, sau khi đã gộp các mức bên ngoài lại.
///
/// Giữ riêng với [`Priority`](crate::apis::priority::Priority) để phần đặt mức theo nền tảng chỉ
/// phải quan tâm tới hai trường hợp, thay vì phải biết ý nghĩa của từng mức bên ngoài.
#[derive(Clone, Copy)]
pub enum Level
{
    Interactive,
    Low,
}

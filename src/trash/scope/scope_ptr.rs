use crate::scope::scope::Scope;

/// Con trỏ tới chính scope, đưa được vào job.
///
/// Một `&Scope` thường không đi vào job được: tham chiếu ấy mượn cái scope nằm trên stack của người
/// mở nó, còn job thì phải sống độc lập với khung stack đó về mặt kiểu. Con trỏ thô cắt đứt quan hệ
/// mượn ấy, và thứ giữ cho nó hợp lệ vẫn là lời hứa cũ: scope không trả về khi còn job chưa xong.
pub struct ScopePtr<'scope>(pub(crate) *const Scope<'scope>);
unsafe impl Send for ScopePtr<'_> {}

use crate::apis::priority::apply::apply;
use crate::apis::priority::level::Level;

/// Standing mà một thread xin hệ điều hành.
///
/// > [!IMPORTANT]
/// > Mọi API bên dưới đều chỉ đặt được cho **thread đang gọi**, nên phải gọi từ chính worker chứ
/// > không phải từ thread đã spawn nó ra.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Priority
{
    #[rustfmt::skip]#[default] Frame,
    Background,
    Io,
}

impl Priority
{
    pub fn apply_to_current_thread(self)
    {
        if cfg!(miri)
        {
            return;
        }

        match self
        {
            Self::Frame => apply(Level::Interactive),
            Self::Background | Self::Io => apply(Level::Low),
        }
    }
}

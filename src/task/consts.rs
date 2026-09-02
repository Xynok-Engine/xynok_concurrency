/// Không nằm trong hàng đợi, không ai đang poll. Chờ một lần gọi dậy.
pub const IDLE: u8 = 0;
/// Đang nằm trong hàng đợi của lane dưới dạng một [`Job`].
pub const SCHEDULED: u8 = 1;
/// Một worker đang poll future.
pub const RUNNING: u8 = 2;
/// Có người gọi dậy trong lúc worker đang poll: poll xong phải xếp lại vào hàng.
pub const NOTIFIED: u8 = 3;
/// Future đã xong, hoặc đã panic. Mọi lần gọi dậy sau đó là vô hại.
pub const DONE: u8 = 4;

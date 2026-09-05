//! ## Giao thức ngủ
//!
//! Worker hết việc thì đi ngủ thế nào mà không bỏ lỡ việc vừa tới.
//!
//! ### Cái sai phải tránh
//!
//! Đây là phần dễ sai nhất của cả pool, nên nói thẳng cái sai trước. Một worker đi ngủ dựa trên thứ
//! nó thấy *lúc nãy* thì mất việc:
//!
//! ```text
//! worker:   tìm việc -> không có
//!                          người đẩy: đẩy việc vào
//!                          người đẩy: gọi dậy      <- không ai đang ngủ, lời gọi rơi vào hư không
//! worker:   ngủ                                    <- và ngủ luôn qua việc vừa được đẩy vào
//! ```
//!
//! ### Ba mảnh bịt cái lỗ đó
//!
//! 1. **Một ô nhớ gói ba con số:** số lần có việc mới, số người đang đi lùng, số người đang thức.
//!    Người đẩy việc cộng vào con số đầu; worker đọc nó *trước* khi đi tìm và chỉ được ngủ nếu nó
//!    chưa nhúc nhích. Việc tới trong lúc mình đang tìm thì con số đó nói ra điều ấy.
//! 2. **Danh sách người đang ngủ,** để gọi dậy đúng một người thay vì hô cả pool dậy tranh nhau một
//!    việc. Dùng như một ngăn xếp: ai ngủ sau cùng được gọi dậy trước, vì cache của người đó còn
//!    nóng nhất.
//! 3. **Người lùng cuối cùng ngó lại một lần nữa** sau khi đã ghi tên vào danh sách ngủ. Mảnh này
//!    không cần cho tính đúng đắn, nó chỉ cắt bớt một lần ngủ rồi dậy ngay.
//!
//! ### Vì sao ba con số phải nằm chung một ô
//!
//! Hai phía đọc chéo nhau: người đẩy việc nhích con số rồi hỏi "có ai ngủ không", worker ghi tên
//! mình rồi hỏi "con số có đổi không". Trên hai ô nhớ riêng biệt thì cả hai đều có thể đọc phải giá
//! trị cũ, và kết cục là việc nằm im còn worker ngủ tiếp.
//!
//! Gói chung một ô mới chỉ là một nửa. Nửa còn lại: **cả hai phía phải vừa đọc vừa ghi ô đó**, chứ
//! không phải một bên vừa đọc vừa ghi còn một bên chỉ đọc. Mọi thao tác đọc-sửa-ghi trên cùng một
//! địa chỉ nằm trong một thứ tự duy nhất, nên kẻ đến sau chắc chắn nhìn thấy kẻ đến trước: hoặc
//! người đẩy việc thấy có người vừa ghi tên đi ngủ, hoặc worker thấy con số đã nhích.
//!
//! > [!IMPORTANT]
//! > Một lần đọc thường thì được phép trả về giá trị cũ tuỳ thích, và loom dựng lại đúng cảnh đó
//! > chỉ trong vài chục lần thử. Bản đầu tiên của module này đọc thường ở phía gọi dậy, và mô hình
//! > loom của pool treo ngay ở cảnh một worker.

pub mod consts;
pub mod params;
pub mod sleep;
pub mod split;
pub mod wake;

pub(crate) use sleep::Sleep;
pub(crate) use wake::Wake;

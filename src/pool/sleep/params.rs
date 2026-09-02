/// Tham số cho một lượt đi ngủ của worker.
///
/// Gom lại thành một chỗ vì bốn giá trị này phải đi cùng nhau mới có nghĩa: chúng cùng mô tả tình
/// trạng của worker tại đúng thời điểm nó quyết định ngủ.
pub(crate) struct ParamsPark<R>
where R: Fn() -> bool
{
    /// Chỗ đứng của worker trong pool, dùng để tìm ô trạng thái riêng của nó.
    pub index:        usize,
    /// Worker này có đang được tính vào nhóm người đi lùng việc hay không.
    pub is_searching: bool,
    /// Con số sự kiện mà worker đọc được **trước** khi đi tìm việc. Nó nhích lên nghĩa là có việc
    /// mới tới trong lúc tìm, và khi đó không được ngủ.
    pub seen:         u32,
    /// Lần ngó lại cuối cùng, chạy sau khi đã ghi tên vào danh sách ngủ. Thấy có việc thì rút tên
    /// ra và không ngủ nữa.
    pub recheck:      R,
}

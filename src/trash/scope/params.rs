/// Tham số cho phép gộp song song một khoảng chỉ số.
///
/// Gom lại thành một chỗ vì năm giá trị này chỉ có nghĩa khi đi cùng nhau, và vì cùng một bộ tham
/// số được nhận ở hai nơi: trên vùng chia việc, và trên tay cầm pool.
pub struct ParamsParReduce<I, F, J>
{
    /// Gộp trên khoảng `0..n`.
    pub n:        usize,
    /// Mỗi lô bao nhiêu chỉ số.
    ///
    /// Nhắm tới quãng **20 micro giây một lô**: đủ lớn để chi phí giao việc chỉ chiếm chừng một
    /// phần mười, đủ nhỏ để mọi core vẫn chia đều được việc. Đo thấy mỗi chỉ số tốn 200 nano giây
    /// thì lấy `20µs / 200ns = 100`.
    ///
    /// Đặt bằng hoặc lớn hơn `n` là chạy tuần tự, và đó là cách hợp lệ để tắt song song cho một chỗ
    /// cụ thể mà không phải sửa cấu trúc code.
    pub batch:    usize,
    /// Giá trị khởi đầu của mỗi lô.
    pub identity: I,
    /// Gộp một chỉ số vào giá trị đang tích luỹ.
    pub fold:     F,
    /// Nối hai kết quả từng phần lại với nhau.
    ///
    /// Được áp lên từng nhóm liền kề chứ không theo một cây cố định, nên nó vẫn phải có tính kết
    /// hợp. Bù lại nó không bao giờ gặp một **cách nhóm khác** giữa hai lần chạy.
    pub join:     J,
}

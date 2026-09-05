/// Mức canh lề lớn nhất mà một giá trị được phép đòi. Rộng hơn thế, ví dụ một vector 128 byte giả
/// định, thì phải đi xin bộ cấp phát toàn cục. Vector AVX 32 byte thì vẫn vừa.
pub const MAX_ALIGN: usize = 64;

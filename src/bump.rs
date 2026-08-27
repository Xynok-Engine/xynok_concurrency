//! Bộ nhớ nháp, nơi cấp phát là dịch một con trỏ và giải phóng là quên nó đi.

use std::cell::{Cell, UnsafeCell};

/// Đơn vị của vùng nhớ nền, có cỡ và canh lề sao cho cả buffer canh 64 byte. Đó cũng là mức canh lề
/// chặt nhất mà [`Bump::alloc`] đáp ứng được.
///
/// Đám byte nằm trong một `UnsafeCell` vì [`Bump::alloc`] ghi vào chúng qua `&self`. Một con trỏ
/// dẫn xuất từ `&[Chunk]` trần là chỉ đọc dù có ép kiểu kiểu gì, và ghi qua nó là hành vi không xác
/// định, miri chặn thẳng. `UnsafeCell` chính là thứ cho một tham chiếu chia sẻ mang theo quyền ghi.
/// Dùng `std::cell` chứ không phải bản bọc cho loom: một arena thuộc về đúng một thread, nên ở đây
/// không có interleaving nào để loom kiểm.
#[repr(align(64))]
struct Chunk(#[allow(dead_code)] UnsafeCell<[u8; 64]>);

/// Mức canh lề lớn nhất mà một giá trị được phép đòi. Rộng hơn thế, ví dụ một vector 128 byte giả
/// định, thì phải đi xin bộ cấp phát toàn cục. Vector AVX 32 byte thì vẫn vừa.
pub const MAX_ALIGN: usize = 64;

/// Một arena cỡ cố định, phát bộ nhớ ra bằng cách đẩy một con trỏ về phía trước.
///
/// # Nó để làm gì
///
/// Pool của frame hứa là không bao giờ block, và một lần `malloc` bên trong job lặng lẽ phá lời hứa
/// đó: bộ cấp phát toàn cục lấy khoá, nên một job có thể làm nghẽn mọi thread khác đang cấp phát.
/// Một arena cho mỗi worker bỏ hẳn cái khoá đi: cấp phát là một phép cộng cộng một lần kiểm biên, và
/// không có tranh chấp vì không thread nào khác với tới arena của worker này.
///
/// Dùng nó cho những buffer dùng một lần rồi bỏ mà một frame sinh ra: danh sách entity nhìn thấy
/// được, đám entity một system muốn tạo, đống draw call chờ sắp xếp. Mọi thứ trong đó chết ở
/// [`Self::reset`].
///
/// # Vì sao sức chứa là cố định
///
/// Nới buffer ra thì nó phải dời chỗ, và mọi tham chiếu đã phát ra thành treo lơ lửng. Giữ đúng một
/// vùng cấp phát từ lúc khởi động chính là thứ cho phép [`Self::alloc`] nhận `&self` mà vẫn trả về
/// `&mut T`: các vùng nó phát ra không bao giờ chồng nhau, và bộ nhớ đằng sau chúng không bao giờ
/// dời. Arena đầy thì trả `None` chứ không tự nới, nên hết chỗ là một quyết định của người gọi chứ
/// không phải một lần khựng lặng lẽ.
///
/// # Vì sao `T: Copy`
///
/// Arena giải phóng tất cả một lượt bằng cách kéo con trỏ về đầu, tức là nó không bao giờ chạy
/// destructor. Giới hạn ở kiểu `Copy` làm điều đó an toàn theo cấu trúc chứ không theo quy ước: một
/// `Vec` hay một `String` đặt vào đây sẽ rò vùng heap của nó mỗi frame.
pub struct Bump
{
    /// Cấp phát một lần và không bao giờ dời, đó là toàn bộ cơ sở để phát `&mut` ra từ `&self`.
    buffer: Box<[Chunk]>,
    /// Số byte đã dùng. `Cell` chứ không phải atomic: một arena thuộc về đúng một thread.
    offset: Cell<usize>,
}

impl Bump
{
    /// Cấp một vùng ít nhất `bytes` byte, làm tròn lên bội của 64.
    pub fn with_capacity(bytes: usize) -> Self
    {
        let chunks = bytes.div_ceil(size_of::<Chunk>());
        let buffer = (0..chunks).map(|_| Chunk(UnsafeCell::new([0; 64]))).collect();

        Self {
            buffer: buffer,
            offset: Cell::new(0),
        }
    }

    /// Tổng số byte của arena.
    #[inline]
    pub fn capacity(&self) -> usize
    {
        self.buffer.len() * size_of::<Chunk>()
    }

    /// Số byte đã phát ra kể từ lần [`Self::reset`] gần nhất, tính cả phần đệm canh lề.
    #[inline]
    pub fn used(&self) -> usize
    {
        self.offset.get()
    }

    /// Còn lại bao nhiêu byte.
    #[inline]
    pub fn remaining(&self) -> usize
    {
        self.capacity() - self.used()
    }

    /// Chép `value` vào arena, hoặc trả `None` nếu hết chỗ.
    ///
    /// # Panics
    ///
    /// Nếu `T` đòi canh lề rộng hơn [`MAX_ALIGN`].
    ///
    /// `clippy::mut_from_ref` đúng là cái khuôn mà kiểu này sinh ra để làm, và cũng là lý do nó nhận
    /// `&self`: hai lần cấp phát phải cùng sống được, mà `&mut self` thì cấm chuyện đó. Các vùng
    /// không bao giờ chồng nhau, nên các `&mut` không bao giờ trỏ trùng.
    #[allow(clippy::mut_from_ref)]
    #[inline]
    pub fn alloc<T: Copy>(&self, value: T) -> Option<&mut T>
    {
        let slot = self.alloc_raw(size_of::<T>(), align_of::<T>())?;

        // Safety: `alloc_raw` vừa trả về một vùng đúng cỡ và đúng canh lề mà không tham chiếu nào
        // khác trỏ vào: con trỏ chỉ chạy tới, còn `reset` thì đòi `&mut self`, mà `&mut self` không
        // thể tồn tại chừng nào borrow `&self` này còn sống.
        unsafe {
            let pointer = slot as *mut T;
            pointer.write(value);
            Some(&mut *pointer)
        }
    }

    /// Đặt trước `len` bản sao của `value`, hoặc trả `None` nếu hết chỗ.
    ///
    /// # Panics
    ///
    /// Nếu `T` đòi canh lề rộng hơn [`MAX_ALIGN`], hoặc `len * size_of::<T>()` tràn số.
    #[allow(clippy::mut_from_ref)]
    #[inline]
    pub fn alloc_slice<T: Copy>(&self, len: usize, value: T) -> Option<&mut [T]>
    {
        let bytes = size_of::<T>().checked_mul(len).expect("độ dài lát tràn một usize");
        let slot = self.alloc_raw(bytes, align_of::<T>())?;

        // Safety: như `alloc`, cộng thêm việc vùng này rộng đúng `len` phần tử theo cách nó được
        // xin. Mọi phần tử đều được ghi trước khi lát được phát ra, nên không chỗ nào còn chưa khởi
        // tạo.
        unsafe {
            let pointer = slot as *mut T;
            for i in 0..len
            {
                pointer.add(i).write(value);
            }
            Some(std::slice::from_raw_parts_mut(pointer, len))
        }
    }

    /// Đặt trước `bytes` byte đã canh lề, chưa khởi tạo, và trả về con trỏ tới đó.
    fn alloc_raw(&self, bytes: usize, align: usize) -> Option<*mut u8>
    {
        debug_assert!(
            align <= MAX_ALIGN,
            "một lần cấp phát nháp đòi canh lề {align} byte, nhưng arena chỉ canh tới {MAX_ALIGN}"
        );

        let base = self.base() as usize;
        let start = (base + self.offset.get()).next_multiple_of(align) - base;
        let end = start.checked_add(bytes)?;

        if end > self.capacity()
        {
            return None;
        }
        self.offset.set(end);

        // Safety: `end <= capacity`, nên `start` nằm trong buffer.
        Some(unsafe { self.base().add(start) })
    }

    /// Con trỏ ghi được tới byte đầu tiên của buffer.
    ///
    /// Ghi được vì `Chunk` toàn bộ là `UnsafeCell`, và đó là thứ cho một tham chiếu chia sẻ quyền
    /// sửa cái nó trỏ tới.
    #[inline]
    fn base(&self) -> *mut u8
    {
        self.buffer.as_ptr().cast::<u8>().cast_mut()
    }

    /// Giải phóng tất cả một lượt bằng cách kéo con trỏ về đầu.
    ///
    /// `&mut self` chính là lập luận an toàn: không tham chiếu nào do [`Self::alloc`] phát ra còn
    /// sống được, vì chúng mượn arena ở dạng chia sẻ. Không có gì bị drop cả, xem tài liệu của kiểu
    /// này để biết vì sao thế là an toàn.
    #[inline]
    pub fn reset(&mut self)
    {
        self.offset.set(0);
    }
}

impl std::fmt::Debug for Bump
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        f.debug_struct("Bump").field("used", &self.used()).field("capacity", &self.capacity()).finish()
    }
}

#[cfg(all(test, not(loom)))]
mod test
{
    use super::*;

    #[test]
    fn suc_chua_lam_tron_len_boi_cua_64()
    {
        assert_eq!(Bump::with_capacity(1).capacity(), 64);
        assert_eq!(Bump::with_capacity(64).capacity(), 64);
        assert_eq!(Bump::with_capacity(65).capacity(), 128);
    }

    #[test]
    fn hai_lan_cap_phat_cung_song_va_khong_de_len_nhau()
    {
        let arena = Bump::with_capacity(1 << 10);

        let first = arena.alloc(7u32).expect("arena mới mà đã hết chỗ");
        let second = arena.alloc(9u32).expect("arena mới mà đã hết chỗ");

        *first += 1;
        *second += 1;

        assert_eq!(*first, 8);
        assert_eq!(*second, 10);
    }

    #[test]
    fn lat_duoc_ghi_day_truoc_khi_phat_ra()
    {
        let arena = Bump::with_capacity(1 << 12);
        let slice = arena.alloc_slice(128, 3u16).expect("arena hết chỗ");

        assert_eq!(slice.len(), 128);
        assert!(slice.iter().all(|value| *value == 3));

        slice[10] = 99;
        assert_eq!(slice[10], 99);
    }

    #[test]
    fn cap_phat_giu_dung_canh_le()
    {
        let arena = Bump::with_capacity(1 << 10);

        let _byte = arena.alloc(1u8).unwrap();
        let wide = arena.alloc(1u64).unwrap();

        assert_eq!((wide as *mut u64 as usize) % align_of::<u64>(), 0, "u64 bị đặt lệch canh lề");
    }

    #[test]
    fn het_cho_thi_tra_none_chu_khong_no_ra()
    {
        let arena = Bump::with_capacity(64);
        assert!(arena.alloc_slice(64, 0u8).is_some());
        assert!(arena.alloc(0u8).is_none(), "arena đầy mà vẫn phát tiếp");
        assert_eq!(arena.remaining(), 0);
    }

    #[test]
    fn reset_tra_lai_toan_bo_cho()
    {
        let mut arena = Bump::with_capacity(256);
        let _ = arena.alloc_slice(200, 0u8).unwrap();
        assert!(arena.used() >= 200);

        arena.reset();
        assert_eq!(arena.used(), 0);
        assert!(arena.alloc_slice(200, 0u8).is_some(), "reset rồi mà vẫn không cấp lại được");
    }

    #[test]
    #[should_panic(expected = "canh lề")]
    fn canh_le_qua_rong_thi_panic()
    {
        #[repr(align(128))]
        #[derive(Clone, Copy)]
        struct TooWide(#[allow(dead_code)] u8);

        let arena = Bump::with_capacity(1 << 12);
        let _ = arena.alloc(TooWide(0));
    }
}

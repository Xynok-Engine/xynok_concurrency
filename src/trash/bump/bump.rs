use std::cell::{Cell, UnsafeCell};

use crate::bump::chunk::Chunk;
use crate::bump::consts::MAX_ALIGN;

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

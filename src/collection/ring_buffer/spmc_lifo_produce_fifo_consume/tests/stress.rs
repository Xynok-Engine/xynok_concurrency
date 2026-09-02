use crate::collection::ring_buffer::spmc_lifo_produce_fifo_consume::SpmcRingBufferLifoProduceFifoConsume;
use crate::utils::backoff::Backoff;

type Ring<T> = SpmcRingBufferLifoProduceFifoConsume<T>;

// --- một chủ, nhiều kẻ trộm chạy song song ---

// Đúng mô hình mà `worker_pool` nhắm tới: chỉ chủ sở hữu push vào deque của mình, các worker
// khác vét việc sang deque riêng rồi tự `pop_lifo` ra xử lý.
//
// Chủ cố tình KHÔNG gọi `pop_lifo` trên deque của chính nó ở đây. Đường đó còn tranh chấp
// phần tử cuối với kẻ trộm, trộn vào thì test sẽ chết vì lỗi khác chứ không đo được giao thức
// CAS/publish mà nó định đo.
#[cfg(not(loom))]
#[test]
fn t40_mot_chu_nhieu_ke_trom_khong_mat_khong_trung()
{
    use crate::sync::AtomicBool;
    use crate::sync::Ordering::{Acquire as AcqXong, Release as RelXong};

    const CAP: usize = 64;
    const KE_TROM: u32 = 4;
    const TONG: u32 = 20_000;
    const CHUNK: usize = 8;
    /// Trần cứng cho vòng gom. Nếu ring hỏng và nhả ra vô hạn thì test phải chết ngay tại đây,
    /// đừng để nó gặm hết RAM rồi mới chịu dừng.
    const TRAN_GOM: usize = TONG as usize * 2;

    let victim = Ring::<u32>::new(CAP);
    let xong = AtomicBool::new(false);

    let mut gom = std::thread::scope(|scope| {
        let ke_trom: Vec<_> = (0..KE_TROM)
            .map(|_| {
                let victim = &victim;
                let xong = &xong;
                scope.spawn(move || {
                    // Deque riêng, tạo ngay trong thread này để chính nó làm chủ sở hữu,
                    // nhờ vậy `pop_lifo` bên dưới qua được debug_assert.
                    let cua_toi = Ring::<u32>::new(CAP);
                    let mut ket_qua = Vec::new();
                    let mut backoff = Backoff::new();
                    loop
                    {
                        if victim.consumer_pop_batch_to(CHUNK, &cua_toi) == 0
                        {
                            if xong.load(AcqXong)
                            {
                                break ket_qua;
                            }
                            backoff.snooze();
                            continue;
                        }
                        backoff.reset();
                        while let Some(v) = cua_toi.pop_lifo()
                        {
                            ket_qua.push(v);
                            assert!(ket_qua.len() <= TRAN_GOM, "gom vượt trần, ring đang nhả ra rác");
                        }
                    }
                })
            })
            .collect();

        let mut backoff = Backoff::new();
        for val in 0..TONG
        {
            let mut cho_day = val;
            while let Err(tra_lai) = victim.push(cho_day)
            {
                cho_day = tra_lai;
                backoff.snooze();
            }
            backoff.reset();
        }
        // Tới dòng này chủ đã push đủ TONG phần tử, nên kẻ trộm nào thấy xong=true kèm một lần
        // steal hụt thì chắc chắn ring đã cạn thật, không phải cạn tạm thời.
        xong.store(true, RelXong);

        let mut tat_ca = Vec::new();
        for h in ke_trom
        {
            tat_ca.extend(h.join().unwrap());
        }
        tat_ca
    });

    gom.sort_unstable();
    assert_eq!(gom.len(), TONG as usize, "không được mất hay nhân bản phần tử nào");
    assert!(gom.iter().copied().eq(0..TONG), "phải đúng dãy 0..TONG, mỗi số xuất hiện một lần");
}

// Chọc thẳng vào cửa sổ tranh chấp giữa `pop_lifo` của chủ và kiểu vét theo lô của kẻ trộm.
//
// Nhánh nhanh của `pop_lifo` (khi `filled >= 2`) hạ `tail` rồi lấy ô `tail - 1` mà không hỏi
// `head`. Nó chỉ chắc chân nếu kẻ trộm mỗi lần chỉ nhích `in_stealing` lên một, như Chase-Lev.
// `consumer_pop_batch_to` thì vét cả lô, nên một lô có thể trùm luôn ô `tail - 1` mà chủ đang
// định lấy:
//
//     in_stealing=0, tail=2
//     chủ:   snapshot filled=2, rẽ vào nhánh nhanh
//     trộm:  CAS in_stealing 0→2, ôm cả ô 0 lẫn ô 1
//     chủ:   fetch_sub tail→1, take_at(1)   ← trùng ô với trộm
//
// Ring cố tình để bé và cho kẻ trộm vét trọn lô, để `filled` luôn quanh quẩn 1..3, đúng chỗ
// cửa sổ này hay mở ra nhất.
#[cfg(not(loom))]
#[test]
fn t41_chu_vua_pop_lifo_vua_bi_trom_lay_lo()
{
    use crate::sync::AtomicBool;
    use crate::sync::Ordering::{Acquire as AcqXong, Release as RelXong};

    const CAP: usize = 8;
    const KE_TROM: u32 = 3;
    const TONG: u32 = 2_000;
    /// Race kiểu này không nổ đều, chạy lại nhiều vòng cho nó có cơ hội.
    const VONG: u32 = 20;
    /// Kẻ trộm vét trọn ring, để lô của nó chạm tới sát `tail`.
    const CHUNK: usize = CAP;
    /// Trần cứng cho mọi vòng gom, ring hỏng thì test phải chết ngay chứ đừng gặm hết RAM.
    const TRAN_GOM: usize = TONG as usize * 2;
    /// Trần cho vòng chờ push, phòng khi con trỏ hỏng làm ring vừa báo đầy vừa báo rỗng.
    const TRAN_CHO: u32 = 100_000;

    for vong in 0..VONG
    {
        let victim = Ring::<u32>::new(CAP);
        let xong = AtomicBool::new(false);

        let mut gom = std::thread::scope(|scope| {
            let ke_trom: Vec<_> = (0..KE_TROM)
                .map(|_| {
                    let victim = &victim;
                    let xong = &xong;
                    scope.spawn(move || {
                        let cua_toi = Ring::<u32>::new(CAP);
                        let mut ket_qua = Vec::new();
                        let mut backoff = Backoff::new();
                        loop
                        {
                            if victim.consumer_pop_batch_to(CHUNK, &cua_toi) == 0
                            {
                                if xong.load(AcqXong)
                                {
                                    break ket_qua;
                                }
                                backoff.snooze();
                                continue;
                            }
                            backoff.reset();
                            while let Some(v) = cua_toi.pop_lifo()
                            {
                                ket_qua.push(v);
                                assert!(ket_qua.len() <= TRAN_GOM, "gom vượt trần, ring đang nhả ra rác");
                            }
                        }
                    })
                })
                .collect();

            // Chủ mà panic giữa chừng thì cờ `xong` bên dưới không bao giờ chạy tới, kẻ trộm sẽ
            // treo mãi và `thread::scope` ngồi chờ theo. Guard này đảm bảo cờ được dựng cả trên
            // đường unwind. Nó khai báo sau `ke_trom` nên khi unwind sẽ rụng trước, kịp báo cho
            // kẻ trộm trước lúc scope đứng ra chờ.
            struct CoXong<'a>(&'a AtomicBool);
            impl Drop for CoXong<'_>
            {
                fn drop(&mut self)
                {
                    self.0.store(true, RelXong);
                }
            }
            let _co_xong = CoXong(&xong);

            // Chủ vừa nạp việc vừa tự rút việc ra làm, đúng kiểu một worker chạy job của chính nó.
            let mut cua_chu = Vec::new();
            let mut backoff = Backoff::new();
            for val in 0..TONG
            {
                let mut cho_day = val;
                let mut cho = 0u32;
                while let Err(tra_lai) = victim.push(cho_day)
                {
                    cho_day = tra_lai;
                    // Ring đầy thì chủ tự rút bớt một việc, vừa khỏi kẹt vừa mở thêm cửa sổ tranh chấp.
                    if let Some(v) = victim.pop_lifo()
                    {
                        cua_chu.push(v);
                    }
                    cho += 1;
                    assert!(cho < TRAN_CHO, "kẹt ở push quá lâu, con trỏ ring có vấn đề");
                    backoff.snooze();
                }
                backoff.reset();

                // Chủ tự rút một việc sau mỗi hai lần nạp, giữ cho hàng luôn quanh quẩn 1..3
                // phần tử, đúng chỗ hai đầu chạm nhau.
                if val % 2 == 0
                    && let Some(v) = victim.pop_lifo()
                {
                    cua_chu.push(v);
                }
            }
            // Vét nốt phần chủ còn ôm rồi mới báo xong, để kẻ trộm nào thấy cờ kèm một lần vét
            // hụt thì biết chắc ring đã cạn thật.
            while let Some(v) = victim.pop_lifo()
            {
                cua_chu.push(v);
                assert!(cua_chu.len() <= TRAN_GOM, "gom vượt trần, ring đang nhả ra rác");
            }
            xong.store(true, RelXong);

            for h in ke_trom
            {
                cua_chu.extend(h.join().unwrap());
            }
            cua_chu
        });

        gom.sort_unstable();
        assert_eq!(gom.len(), TONG as usize, "vòng {}: mất hoặc nhân bản phần tử", vong);
        assert!(
            gom.iter().copied().eq(0..TONG),
            "vòng {}: phải đúng dãy 0..TONG, mỗi số xuất hiện một lần",
            vong
        );
    }
}

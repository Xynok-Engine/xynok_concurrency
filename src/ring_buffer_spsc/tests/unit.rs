use super::*;

#[test]
fn suc_chua_lam_tron_len_luy_thua_hai()
{
    assert_eq!(RingBufferSpsc::<u8>::new(0).capacity(), 2);
    assert_eq!(RingBufferSpsc::<u8>::new(3).capacity(), 4);
    assert_eq!(RingBufferSpsc::<u8>::new(256).capacity(), 256);
}

#[test]
#[should_panic(expected = "2^31")]
fn suc_chua_vuot_tran_thi_panic()
{
    let _ = RingBufferSpsc::<u8>::new(MAX_SLOTS + 1);
}

#[test]
fn hai_dau_co_dung_bo_trait_can_thiet()
{
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<RingBufferSpsc<u8>>();
    assert_sync::<RingBufferSpsc<u8>>();
    assert_send::<Sender<'_, u8>>();
    assert_send::<Receiver<'_, u8>>();
}

#[test]
fn ra_dung_thu_tu_vao()
{
    let mut ring = RingBufferSpsc::new(8);
    let (mut tx, mut rx) = ring.split();

    for i in 0..5u32
    {
        assert_eq!(tx.push(i), Ok(()));
    }

    for i in 0..5u32
    {
        assert_eq!(rx.pop(), Some(i));
    }
    assert_eq!(rx.pop(), None);
}

#[test]
fn day_thi_tra_lai_chu_khong_nuot()
{
    let mut ring = RingBufferSpsc::new(4);
    let (mut tx, mut rx) = ring.split();

    assert_eq!(tx.push_iter(0..4u32), 4);
    assert!(tx.is_full());
    assert_eq!(tx.push(99), Err(99));

    // Đọc bớt một cái là có chỗ ngay.
    assert_eq!(rx.pop(), Some(0));
    assert_eq!(tx.push(99), Ok(()));
}

#[test]
fn push_iter_chi_nhan_phan_vua()
{
    let mut ring = RingBufferSpsc::new(4);
    let (mut tx, mut rx) = ring.split();

    assert_eq!(tx.push_iter(0..10u32), 4);
    assert_eq!(tx.len(), 4);

    let mut got = Vec::new();
    assert_eq!(rx.pop_batch(&mut got, 10), 4);
    assert_eq!(got, vec![0, 1, 2, 3]);
}

#[test]
fn pop_batch_lay_dung_so_luong_xin()
{
    let mut ring = RingBufferSpsc::new(16);
    let (mut tx, mut rx) = ring.split();

    assert_eq!(tx.push_iter(0..10u32), 10);

    let mut got = Vec::new();
    assert_eq!(rx.pop_batch(&mut got, 4), 4);
    assert_eq!(got, vec![0, 1, 2, 3]);
    assert_eq!(rx.len(), 6);

    got.clear();
    assert_eq!(rx.drain_with(|val| got.push(val)), 6);
    assert_eq!(got, vec![4, 5, 6, 7, 8, 9]);
    assert!(rx.is_empty());
}

#[test]
fn chi_so_quan_qua_cuoi_mang_van_dung_thu_tu()
{
    let mut ring = RingBufferSpsc::new(4);
    let (mut tx, mut rx) = ring.split();

    // Chạy vài vòng để chỉ số vượt qua biên mảng nhiều lần.
    for round in 0..10u32
    {
        assert_eq!(tx.push_iter(round * 4..round * 4 + 4), 4);
        let mut got = Vec::new();
        assert_eq!(rx.pop_batch(&mut got, 4), 4);
        assert_eq!(got, vec![round * 4, round * 4 + 1, round * 4 + 2, round * 4 + 3]);
    }
}

#[test]
fn rong_thi_pop_tra_none_chu_khong_quay()
{
    let mut ring = RingBufferSpsc::<u32>::new(8);
    let (_tx, mut rx) = ring.split();

    assert_eq!(rx.pop(), None);
    assert_eq!(rx.pop(), None);
    assert_eq!(rx.pop_batch_with(4, |_| panic!("ring rỗng mà vẫn phát ra phần tử")), 0);
}

#[test]
fn drop_tha_moi_phan_tu_chua_doc()
{
    use std::sync::Arc;

    let tracker = Arc::new(());

    {
        let mut ring = RingBufferSpsc::new(8);
        {
            let (mut tx, mut rx) = ring.split();
            for _ in 0..5
            {
                assert_eq!(tx.push(Arc::clone(&tracker)), Ok(()));
            }
            // Đọc hai cái, ba cái còn lại phải do `Drop` của ring lo.
            assert!(rx.pop().is_some());
            assert!(rx.pop().is_some());
        }
        assert_eq!(Arc::strong_count(&tracker), 4);
    }

    assert_eq!(Arc::strong_count(&tracker), 1, "ring bị thả mà không thả phần tử còn trong nó");
}

#[test]
fn lenh_am_thanh_di_qua_dung_nguyen_ven()
{
    // Đúng hình dạng mà thread audio sẽ thấy: lane A đẩy lệnh, callback vét sạch một lượt.
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum AudioCommand
    {
        Play(u32),
        Volume(f32),
        Stop,
    }

    let mut ring = RingBufferSpsc::new(64);
    let (mut tx, mut rx) = ring.split();

    let sent = [AudioCommand::Play(7), AudioCommand::Volume(0.5), AudioCommand::Play(9), AudioCommand::Stop];
    assert_eq!(tx.push_iter(sent), 4);

    let mut got = Vec::new();
    assert_eq!(rx.drain_with(|command| got.push(command)), 4);
    assert_eq!(got, sent);
}

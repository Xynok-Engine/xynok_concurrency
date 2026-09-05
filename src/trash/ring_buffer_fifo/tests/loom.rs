use crate::ring_buffer_fifo::RingBufferFifo;

#[test]
fn t0_chu_va_trom_cung_an_mot_dau_khong_ai_lay_trung()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBufferFifo::<u32>::new(2));

        let mut tx = unsafe { ring.producer() };
        tx.push(1).unwrap();
        tx.push(2).unwrap();

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || {
            let rx = other.consumer();
            let mut got = Vec::new();
            rx.steal_half(&mut got);
            got
        });

        let mut mine = Vec::new();
        tx.pop_batch(&mut mine, 2);

        mine.extend(thief.join().unwrap());
        mine.sort_unstable();
        assert_eq!(mine, vec![1, 2], "hai job, không được mất và không được nhân đôi");
    });
}

#[test]
fn t1_trom_thay_tail_moi_thi_thay_ca_job()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBufferFifo::<u32>::new(2));

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || {
            let rx = other.consumer();
            rx.steal()
        });

        let mut tx = unsafe { ring.producer() };
        tx.push(7).unwrap();

        match thief.join().unwrap()
        {
            Some(val) => assert_eq!(val, 7),
            None => assert_eq!(tx.pop(), Some(7), "trộm không kịp thì job phải còn nguyên"),
        }
    });
}
#[test]
fn t2_chu_khong_ghi_de_len_o_ke_trom_dang_be()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBufferFifo::<u32>::new(2));

        let mut tx = unsafe { ring.producer() };
        tx.push(1).unwrap();
        tx.push(2).unwrap();

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || {
            let rx = other.consumer();
            let mut got = Vec::new();
            rx.steal_half(&mut got);
            got
        });

        let mut mine = Vec::new();
        tx.pop_batch(&mut mine, 2);

        for _ in 0..2
        {
            if tx.push(3).is_ok()
            {
                break;
            }
            loom::thread::yield_now();
        }

        mine.extend(thief.join().unwrap());
        mine.sort_unstable();
        assert!(
            mine == vec![1, 2] || mine == vec![1, 2, 3],
            "job cũ không được mất, không được nhân đôi: {mine:?}"
        );
    });
}

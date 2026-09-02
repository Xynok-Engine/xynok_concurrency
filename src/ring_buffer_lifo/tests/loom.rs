use crate::ring_buffer_lifo::{RingBufferLifo, Steal};

#[test]
fn t0_chu_va_trom_cung_gianh_job_cuoi_khong_ai_lay_trung()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBufferLifo::<u32>::new(2));

        let mut tx = unsafe { ring.producer() };
        tx.push(1).unwrap();

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || other.consumer().steal());

        let mine = tx.pop();

        let mut all: Vec<u32> = mine.into_iter().chain(thief.join().unwrap()).collect();
        all.sort_unstable();
        assert_eq!(all, vec![1], "một job, không được mất và không được nhân đôi");
    });
}

#[test]
fn t1_chu_va_trom_tren_hai_job()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBufferLifo::<u32>::new(2));

        let mut tx = unsafe { ring.producer() };
        tx.push(1).unwrap();
        tx.push(2).unwrap();

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || other.consumer().steal());

        let mut mine = Vec::new();
        while let Some(val) = tx.pop()
        {
            mine.push(val);
        }

        mine.extend(thief.join().unwrap());
        mine.sort_unstable();
        assert_eq!(mine, vec![1, 2], "hai job, không được mất và không được nhân đôi");
    });
}

#[test]
fn t2_trom_thay_bottom_moi_thi_thay_ca_job()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBufferLifo::<u32>::new(2));

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || other.consumer().steal());

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
fn t3_chu_khong_ghi_de_len_o_ke_trom_dang_be()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBufferLifo::<u32>::new(2));

        let mut tx = unsafe { ring.producer() };
        tx.push(1).unwrap();
        tx.push(2).unwrap();

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || other.consumer().steal());

        let mut mine = Vec::new();
        while let Some(val) = tx.pop()
        {
            mine.push(val);
        }

        for _ in 0..2
        {
            if tx.push(3).is_ok()
            {
                break;
            }
            loom::thread::yield_now();
        }

        mine.extend(thief.join().unwrap());
        while let Some(val) = tx.pop()
        {
            mine.push(val);
        }
        mine.sort_unstable();
        assert!(
            mine == vec![1, 2] || mine == vec![1, 2, 3],
            "job cũ không được mất, không được nhân đôi: {mine:?}"
        );
    });
}

#[test]
fn t4_chu_day_them_trong_luc_trom_dang_be()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBufferLifo::<u32>::new(2));

        let mut tx = unsafe { ring.producer() };
        tx.push(1).unwrap();

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || other.consumer().try_steal());

        let mut mine = Vec::new();
        let _ = tx.push(2);
        if let Some(val) = tx.pop()
        {
            mine.push(val);
        }
        let _ = tx.push(3);

        if let Steal::Success(val) = thief.join().unwrap()
        {
            mine.push(val);
        }
        while let Some(val) = tx.pop()
        {
            mine.push(val);
        }

        mine.sort_unstable();
        mine.dedup();
        assert!(mine.contains(&1), "job đầu không được mất: {mine:?}");
    });
}

#[test]
fn t5_chu_day_theo_lo_trong_luc_trom_dang_be()
{
    loom::model(|| {
        let ring = loom::sync::Arc::new(RingBufferLifo::<u32>::new(2));

        let mut tx = unsafe { ring.producer() };
        tx.push(1).unwrap();

        let other = loom::sync::Arc::clone(&ring);
        let thief = loom::thread::spawn(move || other.consumer().try_steal());

        let mut mine = Vec::new();
        if let Some(val) = tx.pop()
        {
            mine.push(val);
        }
        let mut vals = vec![2u32, 3];
        tx.push_batch(&mut vals);

        if let Steal::Success(val) = thief.join().unwrap()
        {
            mine.push(val);
        }
        while let Some(val) = tx.pop()
        {
            mine.push(val);
        }
        mine.sort_unstable();
        mine.dedup();
        assert!(mine.contains(&1), "job đầu không được mất: {mine:?}");
    });
}

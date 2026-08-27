//! Một frame đi qua đủ các lane, từ đầu tới cuối.
//!
//! Chạy: `cargo run --release --example frame`
//!
//! Nó không vẽ gì cả, chỉ dựng lại đúng hình dạng lưu lượng của một frame thật: việc CPU-bound chia
//! cho cả pool, một asset đọc từ đĩa vắt qua nhiều frame, lệnh gửi cho thread audio, và vài lời gọi
//! buộc phải nằm trên main thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use xynok_concurrency::lanes::Lanes;
use xynok_concurrency::ring_buffer_spsc::RingBufferSpsc;

/// Lệnh mà lane compute gửi cho thread audio.
#[derive(Debug, Clone, Copy)]
enum AudioCommand
{
    Play(#[allow(dead_code)] u32),
    Volume(#[allow(dead_code)] f32),
}

const FRAMES: usize = 120;
const ENTITIES: usize = 50_000;
/// Bao nhiêu entity một job. Xem mục 8.3 của tài liệu lane: nhắm mỗi job chừng 20 micro giây.
const BATCH: usize = 512;

fn main()
{
    let lanes = Arc::new(Lanes::from_env());
    println!(
        "lane compute: {} worker cộng thread gọi, lane async: {} worker",
        lanes.compute().worker_threads(),
        lanes.async_lane().worker_threads()
    );

    // Kênh gửi lệnh cho audio. Một người ghi, một người đọc, và phía đọc không bao giờ lấy khoá.
    let mut audio_ring = RingBufferSpsc::<AudioCommand>::new(256);
    let (mut audio_tx, mut audio_rx) = audio_ring.split();

    // Dữ liệu của frame: một mảng vị trí và vận tốc rất tầm thường.
    let mut positions = vec![0.0f32; ENTITIES];
    let velocities: Vec<f32> = (0..ENTITIES).map(|i| (i % 17) as f32 * 0.01).collect();

    // Asset load bắt đầu ở frame 0 và không ai đợi nó. Cả chuỗi "đọc rồi decode" là một task của
    // lane async: giữa hai bước nó `.await`, tức là trả thread lại cho lane chứ không ngồi giữ.
    let loader = Arc::clone(&lanes);
    let mut loading = Some(lanes.spawn_async(async move {
        let raw = loader
            .run_blocking(|| {
                std::thread::sleep(Duration::from_millis(80));
                "scene.pak".to_string()
            })
            .await?;

        let decoded = loader.run_blocking(move || {
            std::thread::sleep(Duration::from_millis(20));
            format!("{raw} (đã decode)")
        });
        decoded.await
    }));

    let audio_callbacks = AtomicU64::new(0);
    let started = Instant::now();

    for frame in 0..FRAMES
    {
        // 1. Việc CPU-bound của frame, chia cho cả pool.
        {
            let velocities = &velocities;
            lanes.compute().scope(|s| {
                for (chunk_index, chunk) in positions.chunks_mut(BATCH).enumerate()
                {
                    s.spawn(move || {
                        let start = chunk_index * BATCH;
                        for (offset, position) in chunk.iter_mut().enumerate()
                        {
                            *position += velocities[start + offset];
                        }
                    });
                }
            });
        }

        // 2. Một phép gộp song song, kết quả không đổi giữa các lần chạy vì thứ tự nối là cố định.
        let total = lanes
            .compute()
            .par_reduce(ENTITIES, BATCH, || 0.0f64, |acc, i| acc + positions[i] as f64, |a, b| a + b);

        // 3. Gửi lệnh cho audio. Ring đầy thì bỏ lệnh, chứ không đứng chờ thread audio.
        let _ = audio_tx.push(AudioCommand::Play(frame as u32));
        if frame % 30 == 0
        {
            let _ = audio_tx.push(AudioCommand::Volume(0.5));
        }

        // 4. Vào vai callback audio: vét sạch một lượt, không cấp phát gì.
        audio_rx.drain_with(|_command| {
            audio_callbacks.fetch_add(1, Ordering::Relaxed);
        });

        // 5. Asset xong lúc nào thì nhận lúc đó, không frame nào phải đợi nó.
        if let Some(pending) = loading.take()
        {
            match pending.try_recv()
            {
                Ok(Some(name)) => println!("frame {frame}: nạp xong {name}"),
                Ok(None) => println!("frame {frame}: nạp hỏng"),
                Err(still_loading) => loading = Some(still_loading),
            }
        }

        // 6. Việc buộc phải nằm trên main thread, vét ở đúng một chỗ cố định.
        let stamp = total;
        lanes.spawn_on_main(move || {
            if stamp.is_nan()
            {
                println!("tổng hỏng ở frame {frame}");
            }
        });
        lanes.run_pending_on_main();

        // 7. Ranh giới frame: mọi arena nháp rỗng trở lại.
        lanes.end_frame();
    }

    let elapsed = started.elapsed();
    let counters = lanes.compute().counters();

    println!("\n{FRAMES} frame trong {elapsed:?}, trung bình {:?} một frame", elapsed / FRAMES as u32);
    println!("lệnh audio đã tiêu thụ: {}", audio_callbacks.load(Ordering::Relaxed));
    println!("job đã chạy:            {}", counters.jobs_run);
    println!(
        "trộm trúng / trượt:     {} / {} ({:.0}% trúng)",
        counters.steal_hits,
        counters.steal_misses,
        counters.steal_hit_rate().unwrap_or(0.0) * 100.0
    );
    println!("lấy từ lane queue:      {}", counters.lane_pops);
    println!("lần xả ring:            {}", counters.spills);
    println!("lần worker đi ngủ:      {}", counters.parks);

    lanes.shutdown();
}

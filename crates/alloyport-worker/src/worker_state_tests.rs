use crate::worker_state::WorkerPersistence;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_persistence_is_bounded_without_stalling_async_work() {
    let persistence = WorkerPersistence::default();
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let mut operations = Vec::new();

    for _ in 0..8 {
        let persistence = persistence.clone();
        let active = Arc::clone(&active);
        let maximum = Arc::clone(&maximum);
        operations.push(tokio::spawn(async move {
            persistence
                .run(move || {
                    let concurrent = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(concurrent, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(50));
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
        }));
    }

    let started = Instant::now();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(
        started.elapsed() < Duration::from_millis(40),
        "blocking persistence stalled the Tokio executor"
    );
    for operation in operations {
        operation
            .await
            .expect("persistence task must join")
            .unwrap();
    }
    assert_eq!(maximum.load(Ordering::SeqCst), 4);
}

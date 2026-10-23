//! Collecting background tasks from the UI loop

use futures::FutureExt;
use tokio::task::{JoinError, JoinHandle};

/// The result of a background task that `is_finished()`, without waiting.
///
/// A plain `handle.now_or_never()` counts against tokio's cooperative budget
/// of the polling task: once that is spent, a finished task reads as pending
/// (`None`). The UI loop drops the handle either way, so the result was lost
/// for good — after ~128 collections a view kept refreshing in the
/// background while its screen stayed frozen. Polling unconstrained never
/// runs out.
pub fn poll_finished<T>(handle: JoinHandle<T>) -> Option<Result<T, JoinError>> {
    tokio::task::unconstrained(handle).now_or_never()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn finished(value: u32) -> JoinHandle<u32> {
        let handle = tokio::spawn(async move { value });
        while !handle.is_finished() {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        handle
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finished_tasks_are_collected_past_the_coop_budget() {
        // Far past tokio's budget of 128 for a task that never yields
        for i in 0..300 {
            let handle = finished(i).await;
            assert_eq!(
                poll_finished(handle).map(Result::unwrap),
                Some(i),
                "task {i}"
            );
        }
    }
}

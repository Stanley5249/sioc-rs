use crate::error::Result;
use tokio::task::JoinHandle;

pub async fn join_tasks<T1, T2>(
    task1: JoinHandle<Result<T1>>,
    task2: JoinHandle<Result<T2>>,
) -> Result<(T1, T2)> {
    let (result1, result2) = tokio::try_join!(task1, task2)?;
    Ok((result1?, result2?))
}

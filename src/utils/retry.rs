use std::future::Future;
use crate::utils::time::sleep_with_jitter;

pub async fn retry_with_backoff<T, F, Fut>(
    mut retries: u32,
    base_delay_ms: u64,
    operation: F,
) -> crate::error::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = crate::error::Result<T>>,
{
    let mut delay = base_delay_ms;
    
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                if retries == 0 {
                    return Err(e);
                }
                
                retries -= 1;
                sleep_with_jitter(delay, delay / 2).await;
                delay *= 2;
            }
        }
    }
}
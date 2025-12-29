use std::fmt::Debug;
use std::future::Future;

use tokio::time::{sleep, Duration};

/// Check if an S3 error is retriable (transient error)
/// Returns true only for transient errors that should be retried.
fn is_retriable_s3_error<E: Debug>(error: &E) -> bool {
    let error_str = format!("{:?}", error);

    // Retriable errors (transient):
    // - Network/timeout errors
    // - Server errors (5xx)
    // - Rate limiting (429)
    //
    // Non-retriable errors (permanent):
    // - NotFound/NoSuchKey (404)
    // - AccessDenied (403)
    // - InvalidRequest (400)

    if error_str.contains("TimeoutError")
        || error_str.contains("DispatchFailure")
        || error_str.contains("ConnectorError")
        || error_str.contains("ConnectionReset")
    {
        return true; // Network issues - retry
    }

    if error_str.contains("NoSuchKey")
        || error_str.contains("NotFound")
        || error_str.contains("AccessDenied")
        || error_str.contains("InvalidRequest")
        || error_str.contains("NoSuchBucket")
    {
        return false; // Permanent errors - don't retry
    }

    // Check for HTTP status codes in error
    if error_str.contains("429") || error_str.contains("TooManyRequests") {
        return true; // Rate limit - retry
    }

    if error_str.contains("500")
        || error_str.contains("502")
        || error_str.contains("503")
        || error_str.contains("504")
        || error_str.contains("InternalError")
        || error_str.contains("ServiceUnavailable")
    {
        return true; // Server errors - retry
    }

    // Default: don't retry unknown errors
    false
}

/// Retry an S3 operation with configurable retry count and delay.
/// Only retries on transient errors (network issues, timeouts, 5xx errors).
/// Does NOT retry on 404 (NotFound), 403 (Forbidden), or other permanent errors.
///
/// Note: this helper intentionally does not log; callers can log around it if desired.
pub async fn retry_s3_operation<F, Fut, T, E>(
    _operation_name: &str,
    retry_count: u32,
    retry_delay_ms: u64,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Debug,
{
    let mut attempts = 0u32;
    let max_attempts = retry_count.max(1);

    loop {
        attempts += 1;
        match operation().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if !is_retriable_s3_error(&e) {
                    return Err(e);
                }
                if attempts >= max_attempts {
                    return Err(e);
                }
                sleep(Duration::from_millis(retry_delay_ms)).await;
            }
        }
    }
}



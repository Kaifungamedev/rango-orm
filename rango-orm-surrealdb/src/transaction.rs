//! Interactive transactions.

use crate::conn::{SurrealPool, SurrealTransaction};
use rango_orm_core::Result;
use std::time::Duration;

/// Run `f` in a transaction: commit on `Ok`, cancel on `Err`.
///
/// Every Rango op accepts the transaction (`&*tx` or `&mut *tx`).
///
/// ```rust,ignore
/// let user = rango::surrealdb::atomic(&db, async |tx| {
///     let user = rango::surrealdb::insert(&*tx, User { .. }).await?;
///     rango::surrealdb::insert(&*tx, Profile { user_id: ForeignKey::new(user.id.0), .. }).await?;
///     Ok(user)
/// }).await?;
/// ```
pub async fn atomic<F, T>(pool: &SurrealPool, f: F) -> Result<T>
where
    F: AsyncFnOnce(&mut SurrealTransaction) -> Result<T>,
{
    let mut tx = pool.begin().await?;
    match f(&mut tx).await {
        Ok(v) => {
            tx.commit().await?;
            Ok(v)
        }
        Err(e) => {
            tx.cancel().await.ok();
            Err(e)
        }
    }
}

/// [`atomic`], retried on `Error::Conflict` (SurrealDB transaction conflicts, raised at
/// commit when another transaction wrote the same keys).
///
/// The closure may run several times, so it must not have side effects outside the
/// transaction. `max_attempts` counts total runs; `0` is treated as `1`. The closure is
/// cloned for each attempt (an async closure is `Clone` when its captures are).
pub async fn atomic_retry<F, T>(pool: &SurrealPool, max_attempts: u32, f: F) -> Result<T>
where
    F: AsyncFnOnce(&mut SurrealTransaction) -> Result<T> + Clone,
{
    let max_attempts = max_attempts.max(1);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match atomic(pool, f.clone()).await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_retryable() && attempt < max_attempts => {
                // ponytail: fixed exponential backoff without jitter, as on Postgres.
                tokio::time::sleep(Duration::from_millis(5u64 << attempt.min(6))).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Alias for [`atomic`].
pub async fn transaction<F, T>(pool: &SurrealPool, f: F) -> Result<T>
where
    F: AsyncFnOnce(&mut SurrealTransaction) -> Result<T>,
{
    atomic(pool, f).await
}

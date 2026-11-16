//! SurrealDB 4 backend for [Rango ORM](https://crates.io/crates/rango-orm).
//!
//! Internal crate: depend on `rango-orm` with feature `surrealdb` (remote servers) or
//! `surrealdb-mem` (plus the embedded in-memory engine).
//!
//! **License note:** this crate is MIT OR Apache-2.0, but it links the SurrealDB 4 Rust
//! SDK, which is BUSL-1.1 (its additional use grant excludes offering it as a database
//! service). Check those terms before you ship a binary built with it.
//!
//! # Storage model
//!
//! - every model is a `SCHEMAFULL` table, defined by `rango makemigrations` / `rango migrate`;
//! - the primary key is the record key (`user:⟨uuid⟩`, `user:42`); `FieldAutoInt` /
//!   `FieldAutoBigInt` keys come from a SurrealDB sequence;
//! - a `ForeignKey<R>` is a `record<r>` link with `REFERENCE ON DELETE ...`, so deletes
//!   cascade (or are rejected) in the database, and existence is checked on write;
//! - SQL NULL is `NONE` (the field is absent);
//! - `select_related` follows the record link (no JOIN), `prefetch_related` binds the keys
//!   as one array.
//!
//! Raw SurrealQL binds `params` as `$p1`, `$p2`, ...
#![forbid(unsafe_code)]

pub use conn::{Conn, SurrealPool, SurrealTransaction};
pub use m2m::M2M;
pub use ops::{
    all, bulk_create, bulk_update, bulk_upsert, delete, get, get_or_create, insert, raw,
    raw_execute, raw_scalar, update, update_or_create,
};
pub use query_builder::{QueryBuilder, RangoFilterExt, SelectAs};
pub use rango_orm_core::WithRelated;
pub use surreal_ext::SurrealQueryExt;
/// The SurrealDB SDK this backend is built on (`SurrealPool` derefs to its client).
pub use surrealdb;
pub use transaction::{atomic, atomic_retry, transaction};
pub use value::FromSurreal;

mod conn;
mod error;
pub mod m2m;
mod ops;
mod query_builder;
mod row;
mod surreal_ext;
mod transaction;
mod url;
mod value;

use crate::error::db_err;
use rango_orm_core::{DatabaseConfig, Error, Result};
use std::time::Duration;
use surrealdb::opt::auth;

/// Connect to SurrealDB and select the namespace and database of `config.url`
/// (see the URL forms in the crate docs of `rango_orm_surrealdb::url`):
///
/// ```text
/// surrealdb://user:pass@host:8000/namespace/database   wss for remote hosts, ws on loopback
/// surrealdb+ws://… surrealdb+wss://… surrealdb+http://… surrealdb+https://…
/// surrealdb+mem://namespace/database                    embedded, feature `kv-mem`
/// ```
///
/// Credentials sign in as a root user, or `?auth=namespace` / `?auth=database` for a
/// namespace or database user (least privilege). `connect_timeout` bounds the connection
/// and sign-in, `slow_query_threshold_ms` sets the slow query WARN, `after_connect`
/// statements run once on the new session. Pool sizes and SQL timeouts do not apply.
pub async fn connect(config: &DatabaseConfig) -> Result<SurrealPool> {
    let target = url::parse(&config.url)?;
    let open = async {
        let db = surrealdb::engine::any::connect(target.endpoint.as_str())
            .await
            .map_err(db_err)?;
        if let Some(c) = &target.credentials {
            let (user, pass) = (c.user.clone(), c.pass.clone());
            let signed = match c.level {
                url::AuthLevel::Root => db
                    .signin(auth::Root {
                        username: user,
                        password: pass,
                    })
                    .await
                    .map(drop),
                url::AuthLevel::Namespace => db
                    .signin(auth::Namespace {
                        namespace: target.namespace.clone(),
                        username: user,
                        password: pass,
                    })
                    .await
                    .map(drop),
                url::AuthLevel::Database => db
                    .signin(auth::Database {
                        namespace: target.namespace.clone(),
                        database: target.database.clone(),
                        username: user,
                        password: pass,
                    })
                    .await
                    .map(drop),
            };
            // Never echo the server's message: it may quote the credentials.
            signed.map_err(|_| Error::Connection("SurrealDB sign-in failed".into()))?;
        }
        db.use_ns(target.namespace.as_str())
            .use_db(target.database.as_str())
            .await
            .map_err(db_err)?;
        Ok::<_, Error>(db)
    };
    let db = match config.connect_timeout {
        0 => open.await?,
        secs => tokio::time::timeout(Duration::from_secs(secs), open)
            .await
            .map_err(|_| Error::Timeout)??,
    };
    let mut pool = SurrealPool::from_client(db)
        .with_slow_query(Duration::from_millis(config.slow_query_threshold_ms));
    if target.endpoint.starts_with("http") {
        pool = pool.without_interactive_transactions();
    }
    for stmt in &config.after_connect {
        conn::Conn::Pool(&pool)
            .run(stmt, Default::default())
            .await?;
    }
    Ok(pool)
}

/// [`connect`] with the default [`DatabaseConfig`] for `url`.
pub async fn connect_url(url: &str) -> Result<SurrealPool> {
    connect(&DatabaseConfig::from_url(url)).await
}

//! Clients, transactions and statement execution.

use crate::error::db_err;
use rango_orm_core::{Error, Result, SqlValue};
use std::ops::Deref;
use std::time::{Duration, Instant};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::method::Transaction;
use surrealdb::types::{Object, QueryError, Value};

/// A connected SurrealDB client, the `PgPool` of this backend.
///
/// SurrealDB multiplexes every request over one connection (WebSocket / HTTP / embedded),
/// so there is no pool to size: clone it freely (each clone is a session on the same
/// connection, with the same namespace, database and credentials).
/// `Deref`s to the SDK client for anything Rango does not wrap.
///
/// Over HTTP each request stands alone: there are no interactive transactions, so
/// [`atomic`](crate::atomic) needs `surrealdb://` (WebSocket) or the embedded engine.
/// Single operations stay atomic on every transport (one `BEGIN ... COMMIT` request).
#[derive(Clone)]
pub struct SurrealPool {
    db: Surreal<Any>,
    slow_query: Duration,
    interactive: bool,
}

impl SurrealPool {
    /// Wrap a client you connected yourself (namespace and database already selected).
    pub fn from_client(db: Surreal<Any>) -> Self {
        Self {
            db,
            slow_query: Duration::from_millis(500),
            interactive: true,
        }
    }

    pub(crate) fn with_slow_query(mut self, threshold: Duration) -> Self {
        self.slow_query = threshold;
        self
    }

    /// Mark a client whose transport cannot hold a transaction open (HTTP).
    pub(crate) fn without_interactive_transactions(mut self) -> Self {
        self.interactive = false;
        self
    }

    /// False over HTTP: [`begin`](Self::begin) and `atomic` fail with `Error::Invalid`.
    pub fn supports_transactions(&self) -> bool {
        self.interactive
    }

    /// The SDK client.
    pub fn client(&self) -> &Surreal<Any> {
        &self.db
    }

    /// Open an interactive transaction. Prefer [`atomic`](crate::atomic), which commits or
    /// cancels it for you.
    pub async fn begin(&self) -> Result<SurrealTransaction> {
        if !self.interactive {
            return Err(Error::Invalid(
                "SurrealDB over HTTP has no interactive transactions: connect with \
                 `surrealdb://` (WebSocket) to use `atomic` or a transaction"
                    .into(),
            ));
        }
        let tx = self.db.clone().begin().await.map_err(db_err)?;
        Ok(SurrealTransaction {
            tx,
            slow_query: self.slow_query,
        })
    }
}

impl Deref for SurrealPool {
    type Target = Surreal<Any>;
    fn deref(&self) -> &Surreal<Any> {
        &self.db
    }
}

impl std::fmt::Debug for SurrealPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurrealPool").finish_non_exhaustive()
    }
}

/// An open transaction. Pass `&*tx` (or `&mut *tx`) to any Rango op inside
/// [`atomic`](crate::atomic). Dropping it without a commit rolls it back.
pub struct SurrealTransaction {
    tx: Transaction<Any>,
    slow_query: Duration,
}

impl SurrealTransaction {
    /// The SDK transaction, for anything Rango does not wrap. Queries on it run inside
    /// this transaction.
    pub fn sdk(&self) -> &Transaction<Any> {
        &self.tx
    }

    pub async fn commit(self) -> Result<()> {
        self.tx.commit().await.map(drop).map_err(db_err)
    }

    pub async fn cancel(self) -> Result<()> {
        self.tx.cancel().await.map(drop).map_err(db_err)
    }
}

/// Where an operation runs: the client, or an open transaction.
///
/// Built with `.into()` from `&SurrealPool` or `&SurrealTransaction` / `&mut SurrealTransaction`:
/// `User::filter(&db)` outside a transaction, `User::filter(&*tx)` inside `rango::atomic`.
#[derive(Clone, Copy, Default)]
pub enum Conn<'a> {
    Pool(&'a SurrealPool),
    Tx(&'a SurrealTransaction),
    /// Sub-builders (`group`, `where_exists`, ...): they only carry filters, never run.
    #[default]
    Detached,
}

impl<'a> From<&'a SurrealPool> for Conn<'a> {
    fn from(p: &'a SurrealPool) -> Self {
        Conn::Pool(p)
    }
}

impl<'a> From<&'a SurrealTransaction> for Conn<'a> {
    fn from(t: &'a SurrealTransaction) -> Self {
        Conn::Tx(t)
    }
}

impl<'a> From<&'a mut SurrealTransaction> for Conn<'a> {
    fn from(t: &'a mut SurrealTransaction) -> Self {
        Conn::Tx(t)
    }
}

/// Query parameters `$p1..$pn` for positional binds, plus named ones.
#[derive(Default)]
pub(crate) struct Vars(Object);

impl Vars {
    pub(crate) fn positional(binds: Vec<SqlValue>) -> Self {
        let mut v = Self::default();
        for (i, b) in binds.into_iter().enumerate() {
            v.0.insert(format!("p{}", i + 1), crate::value::to_value(b));
        }
        v
    }

    pub(crate) fn set(mut self, name: &str, value: Value) -> Self {
        self.0.insert(name, value);
        self
    }
}

impl<'a> Conn<'a> {
    /// Run `sql` and return every statement result, failing on the first statement error.
    pub(crate) async fn run(self, sql: &str, vars: Vars) -> Result<Vec<Value>> {
        let (query, slow) = match self {
            Conn::Pool(p) => (p.db.query(sql), p.slow_query),
            Conn::Tx(t) => (t.tx.query(sql), t.slow_query),
            Conn::Detached => {
                return Err(Error::Invalid(
                    "a sub-query builder has no connection and cannot be executed".into(),
                ));
            }
        };
        let start = Instant::now();
        let mut res = query.bind(vars.0).await.map_err(db_err)?;
        let elapsed = start.elapsed();
        if elapsed >= slow {
            // SurrealQL text only: values are bound, never logged.
            tracing::warn!(
                db.statement = sql,
                elapsed_ms = elapsed.as_millis() as u64,
                "slow query"
            );
        }
        let n = res.num_statements();
        let mut errors: Vec<(usize, surrealdb::Error)> = res.take_errors().into_iter().collect();
        if !errors.is_empty() {
            errors.sort_by_key(|(i, _)| *i);
            // Inside a transaction the other statements only report "not executed".
            let pick = errors
                .iter()
                .position(|(_, e)| e.query_details() != Some(&QueryError::NotExecuted))
                .unwrap_or(0);
            return Err(db_err(errors.swap_remove(pick).1));
        }
        (0..n)
            .map(|i| res.take::<Value>(i).map_err(db_err))
            .collect()
    }

    /// The result of the last statement.
    pub(crate) async fn value(self, sql: &str, vars: Vars) -> Result<Value> {
        Ok(self.run(sql, vars).await?.pop().unwrap_or(Value::None))
    }

    /// `sql` as one atomic request: wrapped in `BEGIN ... COMMIT` on a pool (works over
    /// HTTP), run as is inside the caller's transaction. Returns the last result of `sql`.
    pub(crate) async fn atomic_value(self, sql: &str, vars: Vars) -> Result<Value> {
        match self {
            Conn::Pool(_) => {
                let wrapped = format!("BEGIN TRANSACTION;\n{sql}\nCOMMIT TRANSACTION;");
                let mut results = self.run(&wrapped, vars).await?;
                results.pop(); // COMMIT
                Ok(results.pop().unwrap_or(Value::None))
            }
            _ => self.value(sql, vars).await,
        }
    }

    /// The rows (array items) returned by the last statement.
    pub(crate) async fn rows(self, sql: &str, vars: Vars) -> Result<Vec<Value>> {
        Ok(match self.value(sql, vars).await? {
            Value::Array(a) => a.into_vec(),
            Value::None | Value::Null => Vec::new(),
            other => vec![other],
        })
    }

    /// A transaction only when the work spans several requests (`chunks > 1`): a single
    /// request is made atomic with [`atomic_value`](Self::atomic_value), on any transport.
    pub(crate) async fn scope_for(self, chunks: usize) -> Result<Scope<'a>> {
        if chunks <= 1 {
            Ok(Scope {
                outer: self,
                owned: None,
            })
        } else {
            self.scope().await
        }
    }

    /// [`scope`](Self::scope), or the plain connection when the pool cannot open a
    /// transaction (HTTP): for operations that stay correct, but not atomic, without one.
    pub(crate) async fn scope_if_possible(self) -> Result<Scope<'a>> {
        match self {
            Conn::Pool(p) if !p.interactive => Ok(Scope {
                outer: self,
                owned: None,
            }),
            _ => self.scope().await,
        }
    }

    /// Same connection if it is a transaction; a new transaction on a pool.
    pub(crate) async fn scope(self) -> Result<Scope<'a>> {
        Ok(match self {
            Conn::Pool(p) => Scope {
                outer: self,
                owned: Some(p.begin().await?),
            },
            _ => Scope {
                outer: self,
                owned: None,
            },
        })
    }
}

/// Several statements as one unit: a transaction opened here, or the caller's.
pub(crate) struct Scope<'a> {
    outer: Conn<'a>,
    owned: Option<SurrealTransaction>,
}

impl Scope<'_> {
    pub(crate) fn conn(&self) -> Conn<'_> {
        match &self.owned {
            Some(tx) => Conn::Tx(tx),
            None => self.outer,
        }
    }

    /// Commit (or cancel on `Err`) the transaction this scope opened.
    pub(crate) async fn finish<T>(self, result: Result<T>) -> Result<T> {
        let Some(tx) = self.owned else {
            return result;
        };
        match result {
            Ok(v) => tx.commit().await.map(|_| v),
            Err(e) => {
                tx.cancel().await.ok();
                Err(e)
            }
        }
    }
}

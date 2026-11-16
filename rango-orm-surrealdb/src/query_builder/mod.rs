use crate::conn::{Conn, Vars};
use rango_orm_core::filter::{Dialect, Filter};
use rango_orm_core::{FromRow, Model, ModelValues, SqlValue};

mod exec;
pub use exec::SelectAs;

pub use rango_orm_core::DEFAULT_QUERY_LIMIT;

/// Chainable, lazy query builder. SurrealQL is built and executed only at `.all()` / `.one()`.
///
/// The filters and the SurrealQL they render to live in the DB-free `Filter`; this type adds
/// the connection and the execution.
pub struct QueryBuilder<'a, M> {
    conn: Conn<'a>,
    f: Filter<M>,
}

impl<'a, M> QueryBuilder<'a, M>
where
    M: Model + ModelValues + FromRow,
{
    pub fn new(conn: impl Into<Conn<'a>>) -> Self {
        Self {
            conn: conn.into(),
            f: Filter::new(Dialect::Surreal),
        }
    }

    /// A sub-builder (`group`, `where_exists`, ...): filters only, never runs.
    fn detached(f: Filter<M>) -> Self {
        Self {
            conn: Conn::Detached,
            f,
        }
    }

    fn map(mut self, g: impl FnOnce(Filter<M>) -> Filter<M>) -> Self {
        self.f = g(self.f);
        self
    }

    /// Add a raw condition (`$__` placeholders), for backend-specific extensions only.
    pub(crate) fn raw_where(self, sql: String, binds: Vec<SqlValue>) -> Self {
        self.map(|f| f.raw_where(sql, binds))
    }

    /// Remember the first unknown column of model `T`; reported when the query runs.
    pub(crate) fn check_col<T: Model>(&mut self, name: &str) {
        self.f.check_col::<T>(name);
    }

    /// Warn when the implicit row cap may have hidden rows.
    fn warn_if_capped(&self, rows: usize) {
        if self.f.is_capped(rows) {
            tracing::warn!(
                table = M::table_name(),
                cap = DEFAULT_QUERY_LIMIT,
                "query returned exactly the default row cap, results may be truncated; \
                 call .limit(n) to choose a cap or .unlimited() to fetch every row"
            );
        }
    }

    /// Returns the SurrealQL that `.all()` runs, for debugging.
    pub fn explain(&self) -> String {
        self.f.select_sql().0
    }
}

rango_orm_core::impl_filter_methods!();

fn vars(binds: Vec<SqlValue>) -> Vars {
    Vars::positional(binds)
}

// ─── RangoFilterExt trait ─────────────────────────────────────────────────────
pub trait RangoFilterExt: Model + ModelValues + FromRow + Sized {
    /// `&db`, or `&*tx` inside `atomic`.
    fn filter<'a>(conn: impl Into<Conn<'a>>) -> QueryBuilder<'a, Self> {
        QueryBuilder::new(conn)
    }
}

impl<M: Model + ModelValues + FromRow> RangoFilterExt for M {}

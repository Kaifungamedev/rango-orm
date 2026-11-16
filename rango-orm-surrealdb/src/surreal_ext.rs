//! SurrealDB-specific query extensions.
//!
//! Import this trait to unlock SurrealDB-only filter methods on any `QueryBuilder`.
//! They render SurrealQL that the other backends do not understand.
//!
//! ```rust,ignore
//! use rango::surrealdb::SurrealQueryExt;
//!
//! let posts = Post::filter(&db)
//!     .json_path_eq(Post::METADATA, &["status"], "published")
//!     .all()
//!     .await?;
//! ```
use crate::query_builder::QueryBuilder;
use rango_orm_core::{ColumnRef, FromRow, Model, ModelValues, SqlValue, quote_ident_surreal as q};

pub trait SurrealQueryExt<M>
where
    M: Model + ModelValues + FromRow,
{
    /// A nested field of a JSON (object) column equals `value`: `col.a.b = $p`.
    /// Every path segment is quoted as an identifier.
    fn json_path_eq(
        self,
        col: impl ColumnRef<M>,
        path: &[&str],
        value: impl Into<SqlValue>,
    ) -> Self;

    /// An array (JSON) column contains `value`: `col CONTAINS $p`.
    fn contains(self, col: impl ColumnRef<M>, value: impl Into<SqlValue>) -> Self;
}

impl<M> SurrealQueryExt<M> for QueryBuilder<'_, M>
where
    M: Model + ModelValues + FromRow,
{
    fn json_path_eq(
        mut self,
        col: impl ColumnRef<M>,
        path: &[&str],
        value: impl Into<SqlValue>,
    ) -> Self {
        self.check_col::<M>(col.column_name());
        let mut field = q(col.column_name());
        for seg in path {
            field.push('.');
            field.push_str(&q(seg));
        }
        self.raw_where(format!("{field} = $__"), vec![value.into()])
    }

    fn contains(mut self, col: impl ColumnRef<M>, value: impl Into<SqlValue>) -> Self {
        self.check_col::<M>(col.column_name());
        self.raw_where(
            format!("{} CONTAINS $__", q(col.column_name())),
            vec![value.into()],
        )
    }
}

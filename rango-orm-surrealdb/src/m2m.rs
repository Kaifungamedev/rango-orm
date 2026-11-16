//! Many-to-many operations on the pivot table `{from}_{to}`, whose two fields are record
//! links (`record<from>`, `record<to>`) under a unique index.

use crate::conn::{Conn, Vars};
use crate::row::object;
use crate::value::to_value;
use rango_orm_core::related::M2mSql;
use rango_orm_core::{Dialect, FromRow, Model, ModelValues, Result};
use surrealdb::types::{Array, Value};

/// Many-to-many operations.
/// `From` = the model that declares the M2M field
/// `To`   = the related model
pub struct M2M<From, To> {
    _from: std::marker::PhantomData<From>,
    _to: std::marker::PhantomData<To>,
}

impl<From, To> M2M<From, To>
where
    From: Model + ModelValues,
    To: Model + ModelValues + FromRow,
{
    fn sql() -> M2mSql {
        M2mSql::new(Dialect::Surreal, From::table_name(), To::table_name())
    }

    fn pivot_table() -> String {
        Self::sql().pivot_table().to_owned()
    }

    fn pair(from: &From, to: &To) -> Vars {
        Vars::positional(vec![from.pk_value(), to.pk_value()])
    }

    /// Add a relation. Idempotent: an existing relation is left as is.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "INSERT", db.sql.table = %Self::pivot_table(), otel.kind = "client"))]
    pub async fn add<'a>(conn: impl Into<Conn<'a>>, from: &From, to: &To) -> Result<()> {
        conn.into()
            .run(&Self::sql().insert(), Self::pair(from, to))
            .await?;
        Ok(())
    }

    /// Remove a relation.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "DELETE", db.sql.table = %Self::pivot_table(), otel.kind = "client"))]
    pub async fn remove<'a>(conn: impl Into<Conn<'a>>, from: &From, to: &To) -> Result<()> {
        conn.into()
            .run(&Self::sql().remove(), Self::pair(from, to))
            .await?;
        Ok(())
    }

    /// Clear all relations of a `from` instance.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "DELETE", db.sql.table = %Self::pivot_table(), otel.kind = "client"))]
    pub async fn clear<'a>(conn: impl Into<Conn<'a>>, from: &From) -> Result<()> {
        let vars = Vars::positional(vec![from.pk_value()]);
        conn.into().run(&Self::sql().clear(), vars).await?;
        Ok(())
    }

    /// All `To` records related to `from`.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = %Self::pivot_table(), otel.kind = "client"))]
    pub async fn all<'a>(conn: impl Into<Conn<'a>>, from: &From) -> Result<Vec<To>> {
        let vars = Vars::positional(vec![from.pk_value()]);
        let rows = conn
            .into()
            .rows(&Self::sql().all(To::pk_column()), vars)
            .await?;
        decode_linked::<To>(&rows)
    }

    /// All `From` records related to `to` (reverse).
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = %Self::pivot_table(), otel.kind = "client"))]
    pub async fn reverse<'a>(conn: impl Into<Conn<'a>>, to: &To) -> Result<Vec<From>>
    where
        From: FromRow,
    {
        let vars = Vars::positional(vec![to.pk_value()]);
        let rows = conn
            .into()
            .rows(&Self::sql().reverse(From::pk_column()), vars)
            .await?;
        decode_linked::<From>(&rows)
    }

    /// Replace every relation of `from` with `to_list`, atomically (one request).
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "UPDATE", db.sql.table = %Self::pivot_table(), otel.kind = "client"))]
    pub async fn set<'a>(conn: impl Into<Conn<'a>>, from: &From, to_list: &[To]) -> Result<()> {
        let s = Self::sql();
        // `$p2` is the loop variable: the insert binds the to-key under that name.
        let sql = format!("{};\nFOR $p2 IN $tos {{ {}; }};", s.clear(), s.insert());
        let tos: Vec<Value> = to_list.iter().map(|t| to_value(t.pk_value())).collect();
        let vars =
            Vars::positional(vec![from.pk_value()]).set("tos", Value::Array(Array::from(tos)));
        conn.into().atomic_value(&sql, vars).await?;
        Ok(())
    }

    /// Does the relation exist.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = %Self::pivot_table(), otel.kind = "client"))]
    pub async fn exists<'a>(conn: impl Into<Conn<'a>>, from: &From, to: &To) -> Result<bool> {
        let rows = conn
            .into()
            .rows(&Self::sql().exists(), Self::pair(from, to))
            .await?;
        Ok(!rows.is_empty())
    }
}

/// Linked records, skipping dangling links (`NONE`).
fn decode_linked<M: Model + ModelValues + FromRow>(
    rows: &[surrealdb::types::Value],
) -> Result<Vec<M>> {
    rows.iter()
        .filter(|r| object(r).is_some())
        .map(crate::ops::decode_model::<M>)
        .collect()
}

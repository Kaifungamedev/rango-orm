//! Raw SurrealQL escape hatch. The query text is trusted: never build it from user input.
//!
//! `params` are bound as `$p1`, `$p2`, ... Results come from the last statement of the query.

use super::*;
use crate::value::FromSurreal;

/// Raw SurrealQL returning records, decoded as `M`.
///
/// A primary key column other than `id` is read from the record `id`
/// (`SELECT *, id AS code FROM ...` also works).
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.rows = tracing::field::Empty, otel.kind = "client"))]
pub async fn raw<'a, M>(
    conn: impl Into<Conn<'a>>,
    sql: &str,
    params: Vec<SqlValue>,
) -> Result<Vec<M>>
where
    M: FromRow,
{
    let rows = conn.into().rows(sql, Vars::positional(params)).await?;
    tracing::Span::current().record("db.rows", rows.len());
    decode_all::<M>(&rows, None)
}

/// Raw SurrealQL returning one value: the first item of an array result, the single field
/// of an object (`SELECT count() FROM ... GROUP ALL`), or the value itself (`RETURN ...`).
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", otel.kind = "client"))]
pub async fn raw_scalar<'a, T>(
    conn: impl Into<Conn<'a>>,
    sql: &str,
    params: Vec<SqlValue>,
) -> Result<T>
where
    T: FromSurreal,
{
    let mut v = conn.into().value(sql, Vars::positional(params)).await?;
    if let Value::Array(a) = v {
        v = a.into_vec().into_iter().next().ok_or(Error::NotFound)?;
    }
    if let Value::Object(o) = &v
        && o.len() == 1
    {
        v = o
            .iter()
            .next()
            .map(|(_, v)| v.clone())
            .unwrap_or(Value::None);
    }
    T::from_surreal(&v).map_err(Error::from)
}

/// Raw SurrealQL statement(s). Returns the number of records the last statement returned:
/// add `RETURN BEFORE` to a `DELETE` to count what it deleted (it returns nothing by default).
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "EXECUTE", db.rows = tracing::field::Empty, otel.kind = "client"))]
pub async fn raw_execute<'a>(
    conn: impl Into<Conn<'a>>,
    sql: &str,
    params: Vec<SqlValue>,
) -> Result<u64> {
    let rows = conn.into().rows(sql, Vars::positional(params)).await?;
    Ok(rows.len() as u64)
}

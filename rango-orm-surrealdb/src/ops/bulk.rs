//! Multi-record operations: bulk insert, update and upsert.
//!
//! Records travel as one bound array per statement (no parameter limit); large inputs are
//! split in chunks of [`CHUNK`] records. A single chunk is one statement, atomic by
//! itself; several chunks share one transaction (the caller's inside `atomic`).

use super::*;
use surrealdb::types::{Array, Number};

const CHUNK: usize = 5_000;

/// Run `$body` (an expression of `$c: Conn` and `$chunk: &[M]` giving `Result<usize>`)
/// per chunk: directly for one chunk, in one transaction for several.
macro_rules! chunked {
    ($conn:expr, $models:expr, |$c:ident, $chunk:ident| $body:expr) => {{
        let conn: Conn<'_> = $conn;
        let models = $models;
        if models.is_empty() {
            Ok(0)
        } else if models.len() <= CHUNK {
            let ($c, $chunk) = (conn, models);
            $body
        } else {
            let scope = conn.scope().await?;
            let result = async {
                let mut total = 0usize;
                for $chunk in models.chunks(CHUNK) {
                    let $c = scope.conn();
                    total += $body?;
                }
                Ok::<usize, Error>(total)
            }
            .await;
            scope.finish(result).await
        }
    }};
}

#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "INSERT", db.sql.table = M::table_name(), db.rows = tracing::field::Empty, otel.kind = "client"))]
pub async fn bulk_create<'a, M>(conn: impl Into<Conn<'a>>, models: &[M]) -> Result<usize>
where
    M: Model + ModelValues,
{
    let sql = format!("INSERT INTO {} $rows RETURN NONE", q(M::table_name()));
    chunked!(conn.into(), models, |c, chunk| async {
        let rows = records(c, chunk).await?;
        c.run(&sql, Vars::default().set("rows", rows)).await?;
        Ok::<usize, Error>(chunk.len())
    }
    .await)
}

/// UPDATE `fields` of each record by primary key. Returns the number of records matched
/// (missing records are skipped, never created). Unknown columns fail with
/// `Error::Invalid` before anything runs.
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "UPDATE", db.sql.table = M::table_name(), db.rows = tracing::field::Empty, otel.kind = "client"))]
pub async fn bulk_update<'a, M>(
    conn: impl Into<Conn<'a>>,
    models: &[M],
    fields: &[&str],
) -> Result<usize>
where
    M: Model + ModelValues,
{
    for f in fields {
        if let Some(msg) = rango_orm_core::unknown_column::<M>(f) {
            return Err(Error::Invalid(msg));
        }
        if *f == M::pk_column() {
            return Err(Error::Invalid(format!(
                "`{f}` is the primary key and cannot be updated"
            )));
        }
    }
    if models.is_empty() || fields.is_empty() {
        return Ok(0);
    }
    let cols = Columns::of::<M>();
    let table = M::table_name();
    // Counting and writing are several statements: one atomic request per chunk, and a
    // transaction around the chunks when there are several.
    let scope = conn.into().scope_for(models.len().div_ceil(CHUNK)).await?;
    let result = async {
        let mut matched = 0usize;
        for chunk in models.chunks(CHUNK) {
            let rows: Vec<Value> = chunk
                .iter()
                .map(|m| {
                    let values = m
                        .field_values()
                        .into_iter()
                        .filter(|(c, _)| fields.contains(c));
                    let data = cols.object(table, values.collect());
                    let mut row = Object::new();
                    row.insert("id", record(table, m.pk_value()));
                    row.insert("data", Value::Object(data));
                    Value::Object(row)
                })
                .collect();
            let n = scope
                .conn()
                .atomic_value(
                    "LET $n = count(SELECT 1 FROM $rows.id); \
                     FOR $r IN $rows { UPDATE $r.id MERGE $r.data RETURN NONE; }; \
                     RETURN $n;",
                    Vars::default().set("rows", Value::Array(Array::from(rows))),
                )
                .await?;
            matched += crate::value::as_i64(&n).map_err(Error::from)? as usize;
        }
        Ok(matched)
    }
    .await;
    let matched = scope.finish(result).await?;
    tracing::Span::current().record("db.rows", matched);
    Ok(matched)
}

/// INSERT records, updating the existing one on a conflict.
///
/// `conflict_on` names the unique columns that detect a conflict (the PK when empty); every
/// other column is overwritten, or the record is left as is when there is no other column.
/// A conflict on another unique index is an error (`UniqueViolation`), as on Postgres.
/// Returns the number of records inserted or updated.
///
/// On the PK this is `INSERT .. ON DUPLICATE KEY UPDATE`. On other columns each record is
/// looked up by them (through their unique index) and merged or created, in one transaction:
/// SurrealDB raises `ON DUPLICATE KEY` for an index only on records without an id.
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "INSERT", db.sql.table = M::table_name(), db.rows = tracing::field::Empty, otel.kind = "client"))]
pub async fn bulk_upsert<'a, M>(
    conn: impl Into<Conn<'a>>,
    models: &[M],
    conflict_on: &[&str],
) -> Result<usize>
where
    M: Model + ModelValues,
{
    for c in conflict_on {
        if let Some(msg) = rango_orm_core::unknown_column::<M>(c) {
            return Err(Error::Invalid(msg));
        }
    }
    let pk = M::pk_column();
    let Some(first) = models.first() else {
        return Ok(0);
    };
    let updates: Vec<&str> = first
        .field_values()
        .into_iter()
        .map(|(c, _)| c)
        .filter(|c| *c != pk && !conflict_on.contains(c))
        .collect();
    let table = q(M::table_name());
    let n = if conflict_on.iter().all(|c| *c == pk) {
        let sql = if updates.is_empty() {
            format!("INSERT IGNORE INTO {table} $rows RETURN id")
        } else {
            let set: Vec<String> = updates
                .iter()
                .map(|c| format!("{0} = $input.{0}", q(c)))
                .collect();
            format!(
                "INSERT INTO {table} $rows ON DUPLICATE KEY UPDATE {} RETURN id",
                set.join(", ")
            )
        };
        chunked!(conn.into(), models, |c, chunk| async {
            let rows = records(c, chunk).await?;
            Ok::<usize, Error>(c.rows(&sql, Vars::default().set("rows", rows)).await?.len())
        }
        .await)?
    } else {
        let cond: Vec<String> = conflict_on
            .iter()
            .map(|c| {
                if *c == pk {
                    "id = $r.id".to_string()
                } else {
                    format!("{0} = $r.data.{0}", q(c))
                }
            })
            .collect();
        let write = if updates.is_empty() {
            "IF $found = NONE { CREATE $r.id CONTENT $r.data RETURN NONE; };"
        } else {
            "IF $found = NONE { CREATE $r.id CONTENT $r.data RETURN NONE; } \
             ELSE { UPDATE $found MERGE $r.data RETURN NONE; };"
        };
        // Created = ids that exist afterwards and did not before (a matched record keeps its id).
        let sql = format!(
            "LET $pre = count(SELECT 1 FROM $rows.id); \
             FOR $r IN $rows {{ LET $found = (SELECT VALUE id FROM {table} WHERE {} LIMIT 1)[0]; {write} }}; \
             RETURN count(SELECT 1 FROM $rows.id) - $pre;",
            cond.join(" AND ")
        );
        let scope = conn.into().scope_for(models.len().div_ceil(CHUNK)).await?;
        let result = async {
            let mut total = 0usize;
            for chunk in models.chunks(CHUNK) {
                let rows = split_records(records(scope.conn(), chunk).await?);
                let created = scope
                    .conn()
                    .atomic_value(&sql, Vars::default().set("rows", rows))
                    .await?;
                total += if updates.is_empty() {
                    crate::value::as_i64(&created).map_err(Error::from)? as usize
                } else {
                    chunk.len()
                };
            }
            Ok(total)
        }
        .await;
        scope.finish(result).await?
    };
    tracing::Span::current().record("db.rows", n);
    Ok(n)
}

/// `[{id, ..fields}]` -> `[{id, data: {..fields}}]`.
fn split_records(rows: Value) -> Value {
    let Value::Array(rows) = rows else {
        return rows;
    };
    let rows: Vec<Value> = rows
        .into_vec()
        .into_iter()
        .map(|row| {
            let Value::Object(mut data) = row else {
                return row;
            };
            let mut out = Object::new();
            out.insert("id", data.remove("id").unwrap_or(Value::None));
            out.insert("data", Value::Object(data));
            Value::Object(out)
        })
        .collect();
    Value::Array(Array::from(rows))
}

/// The records of `models` as an array of objects with their `id`. Auto-increment models
/// without a PK get the next sequence values, in one round trip per chunk (an upsert that
/// then matches an existing record by a unique index leaves those values unused, like a
/// Postgres sequence).
async fn records<M: Model + ModelValues>(conn: Conn<'_>, models: &[M]) -> Result<Value> {
    let table = M::table_name();
    let cols = Columns::of::<M>();
    let missing = models.iter().filter(|m| m.pk_value().is_null()).count();
    let mut ids = Vec::new();
    if missing > 0 {
        if !auto_pk::<M>() {
            return Err(Error::Invalid(format!(
                "`{table}`: the primary key `{}` is NULL",
                M::pk_column()
            )));
        }
        ids = conn
            .rows(
                "SELECT VALUE sequence::nextval($seq) FROM array::range(0, $n)",
                Vars::default()
                    .set("seq", sequence::<M>())
                    .set("n", Value::Number(Number::Int(missing as i64))),
            )
            .await?;
    }
    let mut ids = ids.iter();
    let mut rows = Vec::with_capacity(models.len());
    for m in models {
        let mut obj = cols.object(table, m.field_values());
        let pk = match m.pk_value() {
            pk if pk.is_null() => {
                let id = ids
                    .next()
                    .ok_or_else(|| Error::Decode("sequence returned too few values".into()))?;
                SqlValue::BigInt(crate::value::as_i64(id).map_err(Error::from)?)
            }
            pk => pk,
        };
        obj.insert("id", record(table, pk));
        rows.push(Value::Object(obj));
    }
    Ok(Value::Array(Array::from(rows)))
}

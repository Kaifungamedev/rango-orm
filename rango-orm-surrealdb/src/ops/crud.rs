//! Single-record CRUD operations.

use super::*;
use rango_orm_core::{
    after_delete, after_insert, after_update, before_delete, before_insert, before_update,
};

/// CREATE one record (`RETURN AFTER`: database defaults come back in the model).
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "CREATE", db.sql.table = M::table_name(), otel.kind = "client"))]
pub async fn insert<'a, M>(conn: impl Into<Conn<'a>>, mut model: M) -> Result<M>
where
    M: Model + ModelValues + FromRow,
{
    before_insert(&mut model).await?;
    let saved = create::<M>(conn.into(), &model).await?;
    after_insert(&saved).await?;
    Ok(saved)
}

/// UPDATE every non-PK field of the record with the model's PK. `Error::NotFound` when
/// there is no such record (UPDATE never creates one).
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "UPDATE", db.sql.table = M::table_name(), otel.kind = "client"))]
pub async fn update<'a, M>(conn: impl Into<Conn<'a>>, mut model: M) -> Result<M>
where
    M: Model + ModelValues + FromRow,
{
    before_update(&mut model).await?;
    let pk = model.pk_value();
    let saved = merge::<M>(conn.into(), &model, pk).await?;
    after_update(&saved).await?;
    Ok(saved)
}

/// DELETE the record with the model's PK. Deleting a missing record is not an error.
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "DELETE", db.sql.table = M::table_name(), otel.kind = "client"))]
pub async fn delete<'a, M>(conn: impl Into<Conn<'a>>, model: &M) -> Result<()>
where
    M: Model + ModelValues,
{
    before_delete(model).await?;
    let vars = Vars::default().set("id", record(M::table_name(), model.pk_value()));
    conn.into().run("DELETE $id", vars).await?;
    after_delete(model).await
}

/// The record with primary key `pk`, if any.
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), otel.kind = "client"))]
pub async fn get<'a, M>(conn: impl Into<Conn<'a>>, pk: &SqlValue) -> Result<Option<M>>
where
    M: Model + ModelValues + FromRow,
{
    let vars = Vars::default().set("id", record(M::table_name(), pk.clone()));
    let rows = conn.into().rows("SELECT * FROM $id", vars).await?;
    rows.first().map(decode_model::<M>).transpose()
}

/// Every record of the table. Uncapped: unlike `QueryBuilder::all` (1000-row default cap).
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), db.rows = tracing::field::Empty, otel.kind = "client"))]
pub async fn all<'a, M>(conn: impl Into<Conn<'a>>) -> Result<Vec<M>>
where
    M: Model + ModelValues + FromRow,
{
    let sql = format!("SELECT * FROM {}", q(M::table_name()));
    let rows = conn.into().rows(&sql, Vars::default()).await?;
    tracing::Span::current().record("db.rows", rows.len());
    decode_all::<M>(&rows, Some(M::pk_column()))
}

/// The record matching `lookup`, or `defaults` inserted. Returns `(model, created)`.
///
/// Lookup and insert are one atomic request (inside the caller's transaction in `atomic`).
/// When a concurrent call creates the record first, the unique index (or the transaction
/// conflict) makes this insert fail, and the record it created is returned instead.
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "CREATE", db.sql.table = M::table_name(), otel.kind = "client"))]
pub async fn get_or_create<'a, M>(
    conn: impl Into<Conn<'a>>,
    lookup: Vec<(&'static str, SqlValue)>,
    mut defaults: M,
) -> Result<(M, bool)>
where
    M: Model + ModelValues + FromRow,
{
    rango_orm_core::check_columns::<M>(lookup.iter().map(|(c, _)| *c))?;
    let conn = conn.into();
    before_insert(&mut defaults).await?;
    // Lookup and create in one atomic request (one round trip, any transport).
    let (find_sql, find_vars) = find_stmt::<M>(&lookup);
    let (create_sql, vars) = create_stmt::<M>(&defaults, find_vars)?;
    let sql = format!(
        "LET $found = ({find_sql});\n\
         LET $created = IF array::len($found) = 0 {{ ({create_sql}) }} ELSE {{ [] }};\n\
         RETURN [$found, $created];"
    );
    let result = match conn.atomic_value(&sql, vars).await {
        Ok(Value::Array(pair)) => {
            let mut pair = pair.into_vec().into_iter().map(|v| match v {
                Value::Array(a) => a.into_vec(),
                _ => Vec::new(),
            });
            let (found, created) = (
                pair.next().unwrap_or_default(),
                pair.next().unwrap_or_default(),
            );
            match (found.first(), created.first()) {
                (Some(f), _) => Ok((decode_model::<M>(f)?, false)),
                (None, Some(c)) => Ok((decode_model::<M>(c)?, true)),
                (None, None) => Err(Error::NotFound),
            }
        }
        Ok(_) => Err(Error::Decode("get_or_create: unexpected result".into())),
        // A concurrent call created it first: the unique index (or a conflict) says so.
        Err(Error::UniqueViolation { .. } | Error::Conflict) if matches!(conn, Conn::Pool(_)) => {
            match find::<M>(conn, &lookup).await? {
                Some(found) => Ok((found, false)),
                None => Err(Error::Conflict),
            }
        }
        Err(e) => Err(e),
    };
    let (model, created) = result?;
    if created {
        after_insert(&model).await?;
    }
    Ok((model, created))
}

/// Update the record matching `lookup` (update hooks fire), or insert `values` (create
/// hooks fire). Lookup and write run in one transaction; over HTTP, which has none, they
/// are two requests (as on SQLite outside `atomic`).
#[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "UPDATE", db.sql.table = M::table_name(), otel.kind = "client"))]
pub async fn update_or_create<'a, M>(
    conn: impl Into<Conn<'a>>,
    lookup: Vec<(&'static str, SqlValue)>,
    mut values: M,
) -> Result<M>
where
    M: Model + ModelValues + FromRow,
{
    rango_orm_core::check_columns::<M>(lookup.iter().map(|(c, _)| *c))?;
    let scope = conn.into().scope_if_possible().await?;
    let result = async {
        if let Some(existing) = find::<M>(scope.conn(), &lookup).await? {
            // Update the record that exists (its PK), not the PK carried by `values`.
            before_update(&mut values).await?;
            let saved = merge::<M>(scope.conn(), &values, existing.pk_value()).await?;
            after_update(&saved).await?;
            Ok(saved)
        } else {
            before_insert(&mut values).await?;
            let saved = create::<M>(scope.conn(), &values).await?;
            after_insert(&saved).await?;
            Ok(saved)
        }
    }
    .await;
    scope.finish(result).await
}

// ─── Statements ───────────────────────────────────────────────────────────────

pub(crate) fn decode_model<M: Model + ModelValues + FromRow>(v: &Value) -> Result<M> {
    decode::<M>(v, Some(M::pk_column()))
}

/// True when the PK is generated by the database (`FieldAutoInt` / `FieldAutoBigInt`).
pub(crate) fn auto_pk<M: Model + ModelValues>() -> bool {
    M::schema().columns.iter().any(|c| {
        c.name == M::pk_column()
            && matches!(c.col_type, ColumnType::AutoInt | ColumnType::AutoBigInt)
    })
}

pub(crate) fn sequence<M: Model + ModelValues>() -> Value {
    Value::String(surreal_sequence_name(M::table_name(), M::pk_column()))
}

async fn create<M>(conn: Conn<'_>, model: &M) -> Result<M>
where
    M: Model + ModelValues + FromRow,
{
    let (sql, vars) = create_stmt::<M>(model, Vars::default())?;
    let rows = conn.rows(sql, vars).await?;
    rows.first()
        .map(decode_model::<M>)
        .transpose()?
        .ok_or(Error::NotFound)
}

/// `CREATE ... CONTENT $data RETURN AFTER` for `model`, its parameters added to `vars`.
fn create_stmt<M>(model: &M, vars: Vars) -> Result<(&'static str, Vars)>
where
    M: Model + ModelValues,
{
    let table = M::table_name();
    let data = Columns::of::<M>().object(table, model.field_values());
    let pk = model.pk_value();
    let (sql, vars) = if pk.is_null() {
        if !auto_pk::<M>() {
            return Err(Error::Invalid(format!(
                "`{table}`: the primary key `{}` is NULL",
                M::pk_column()
            )));
        }
        (
            "CREATE type::record($tb, sequence::nextval($seq)) CONTENT $data RETURN AFTER",
            vars.set("tb", Value::String(table.to_string()))
                .set("seq", sequence::<M>()),
        )
    } else {
        (
            "CREATE $id CONTENT $data RETURN AFTER",
            vars.set("id", record(table, pk)),
        )
    };
    Ok((sql, vars.set("data", Value::Object(data))))
}

/// MERGE the fields of `model` into the record `pk` (NULL fields are unset).
async fn merge<M>(conn: Conn<'_>, model: &M, pk: SqlValue) -> Result<M>
where
    M: Model + ModelValues + FromRow,
{
    let table = M::table_name();
    let data = Columns::of::<M>().object(table, model.field_values());
    let vars = Vars::default()
        .set("id", record(table, pk))
        .set("data", Value::Object(data));
    let rows = conn
        .rows("UPDATE $id MERGE $data RETURN AFTER", vars)
        .await?;
    rows.first()
        .map(decode_model::<M>)
        .transpose()?
        .ok_or(Error::NotFound)
}

/// The first record whose columns equal `lookup`.
pub(crate) async fn find<M>(
    conn: Conn<'_>,
    lookup: &[(&'static str, SqlValue)],
) -> Result<Option<M>>
where
    M: Model + ModelValues + FromRow,
{
    let (sql, vars) = find_stmt::<M>(lookup);
    let rows = conn.rows(&sql, vars).await?;
    rows.first().map(decode_model::<M>).transpose()
}

/// `SELECT * FROM t WHERE <lookup> LIMIT 1`, its values bound as `$l0..`.
fn find_stmt<M: Model + ModelValues>(lookup: &[(&'static str, SqlValue)]) -> (String, Vars) {
    let cols = Columns::of::<M>();
    let mut vars = Vars::default();
    let mut conds = Vec::with_capacity(lookup.len());
    for (i, (col, v)) in lookup.iter().enumerate() {
        let field = if *col == M::pk_column() {
            "id".to_string()
        } else {
            q(col)
        };
        if v.is_null() {
            conds.push(format!("{field} = NONE"));
        } else {
            let name = format!("l{i}");
            conds.push(format!("{field} = ${name}"));
            vars = vars.set(&name, cols.value(M::table_name(), col, v.clone()));
        }
    }
    let mut sql = format!("SELECT * FROM {}", q(M::table_name()));
    if !conds.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conds.join(" AND "));
    }
    sql.push_str(" LIMIT 1");
    (sql, vars)
}

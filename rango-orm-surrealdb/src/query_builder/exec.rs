use super::*;
use crate::row::{SurrealRow, decode, decode_all, object};
use crate::value::{FromSurreal, as_f64, as_i64, record, to_sql_value};
use rango_orm_core::related::{row_key, sql_value_key};
use rango_orm_core::{
    ColumnRef, Error, Partial, Result, WithRelated, check_columns, unknown_column,
};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use surrealdb::types::{Array, Value};

impl<'a, M> QueryBuilder<'a, M>
where
    M: Model + ModelValues + FromRow,
{
    /// Fetch all matching records, populating relation caches when specs are present.
    ///
    /// Returns `Vec<WithRelated<M>>`: each item derefs to `M`, and related records are
    /// reached with `.related::<R>()` / `.prefetched::<R>()`.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), db.rows = tracing::field::Empty, otel.kind = "client"))]
    pub async fn all(self) -> Result<Vec<WithRelated<M>>> {
        self.f.ensure_valid()?;
        let (sql, binds) = self.f.select_sql();
        let rows = self.conn.rows(&sql, vars(binds)).await?;
        self.warn_if_capped(rows.len());
        tracing::Span::current().record("db.rows", rows.len());
        let pk = Some(M::pk_column());

        let mut results: Vec<WithRelated<M>> = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut wr = WithRelated::new(decode::<M>(row, pk)?);
            // select_related: R came back nested in `__r{i}` (NONE for a NULL / dangling FK).
            for (i, spec) in self.f.select_related_specs().iter().enumerate() {
                let Some(r) = object(row)
                    .and_then(|o| o.get(&format!("__r{i}")))
                    .and_then(object)
                else {
                    continue;
                };
                let boxed =
                    (spec.from_row)(&SurrealRow::new(r, Some(spec.pk_col))).map_err(|e| {
                        Error::Decode(format!("select_related({}): {}", spec.fk_col, e))
                    })?;
                wr.insert_raw(spec.type_id, boxed);
            }
            results.push(wr);
        }
        if results.is_empty() {
            return Ok(results);
        }

        // prefetch_related: one query per spec, keys bound as one array of M's records.
        for spec in self.f.prefetch_specs() {
            let pks: Vec<SqlValue> = results.iter().map(|wr| wr.inner.pk_value()).collect();
            let mut seen = HashSet::new();
            let keys: Vec<Value> = pks
                .iter()
                .filter(|v| sql_value_key(v).is_some_and(|k| seen.insert(k)))
                .map(|v| record(M::table_name(), v.clone()))
                .collect();
            if keys.is_empty() {
                continue;
            }
            let like = pks[0].clone();
            let sql = format!("{} IN $keys", spec.select);
            let fetched = self
                .conn
                .rows(
                    &sql,
                    Vars::default().set("keys", Value::Array(Array::from(keys))),
                )
                .await?;

            let mut grouped: HashMap<String, Vec<Box<dyn Any + Send + Sync>>> = HashMap::new();
            for row in &fetched {
                let Some(obj) = object(row) else { continue };
                let top = SurrealRow::new(obj, None);
                let Some(key) = row_key(&top, &spec.key_col, &like) else {
                    continue;
                };
                let r_obj = match spec.nested {
                    // M2M: R nested in `__t`, NONE for a dangling pivot link.
                    Some(field) => match obj.get(field).and_then(object) {
                        Some(o) => o,
                        None => continue,
                    },
                    None => obj,
                };
                let boxed =
                    (spec.from_row)(&SurrealRow::new(r_obj, Some(spec.pk_col))).map_err(|e| {
                        Error::Decode(format!("prefetch_related({}): {}", spec.key_col, e))
                    })?;
                grouped.entry(key).or_default().push(boxed);
            }

            // Always set the cache (an empty Vec when nothing matched).
            for (wr, pk) in results.iter_mut().zip(&pks) {
                let items = sql_value_key(pk)
                    .and_then(|k| grouped.remove(&k))
                    .unwrap_or_default();
                wr.insert_raw(spec.vec_type_id, (spec.collect_vec)(items));
            }
        }
        Ok(results)
    }

    /// Fetch the first matching record (no relation population).
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), otel.kind = "client"))]
    pub async fn one(mut self) -> Result<Option<M>> {
        self.f.ensure_valid()?;
        self = self.limit(1);
        let (sql, binds) = self.f.select_sql();
        let rows = self.conn.rows(&sql, vars(binds)).await?;
        rows.first()
            .map(|r| decode::<M>(r, Some(M::pk_column())))
            .transpose()
    }

    /// Count matching records.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), otel.kind = "client"))]
    pub async fn count(self) -> Result<i64> {
        self.f.ensure_valid()?;
        let (sql, binds) = self.f.count_sql();
        let rows = self.conn.rows(&sql, vars(binds)).await?;
        match rows.first().and_then(object).and_then(|o| o.get("n")) {
            Some(n) => as_i64(n).map_err(Error::from),
            None => Ok(0),
        }
    }

    /// Check if any matching record exists (stops at the first one).
    pub async fn exists(self) -> Result<bool> {
        self.f.ensure_valid()?;
        let (sql, binds) = self.f.exists_sql();
        let v = self.conn.value(&sql, vars(binds)).await?;
        bool::from_surreal(&v).map_err(Error::from)
    }

    /// SELECT only the given columns: `Vec<HashMap<String, SqlValue>>`. Record links come
    /// back as their key; objects and arrays as `SqlValue::Json`.
    ///
    /// ```rust,ignore
    /// let rows = User::filter(&db).eq(User::ACTIVE, true).values(&["id", "email"]).await?;
    /// ```
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), db.rows = tracing::field::Empty, otel.kind = "client"))]
    pub async fn values(self, cols: &[&str]) -> Result<Vec<HashMap<String, SqlValue>>> {
        self.f.ensure_valid()?;
        check_columns::<M>(cols.iter().copied())?;
        let (sql, binds) = self.f.values_sql(cols);
        let rows = self.conn.rows(&sql, vars(binds)).await?;
        self.warn_if_capped(rows.len());
        tracing::Span::current().record("db.rows", rows.len());

        let schema = M::schema();
        let type_of = |col: &str| {
            schema
                .columns
                .iter()
                .find(|c| c.name == col)
                .map(|c| c.col_type.clone())
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let obj = object(row).ok_or_else(|| Error::Decode("expected an object".into()))?;
            let mut map = HashMap::with_capacity(cols.len());
            for &col in cols {
                use rango_orm_core::ColumnType as T;
                let v = obj.get(col).map_or(SqlValue::NullText, to_sql_value);
                // Read by column type: dates are stored as datetimes, NULLs carry no type.
                let v = match (v, type_of(col)) {
                    (SqlValue::Decimal(mut d), Some(T::Decimal { scale, .. })) => {
                        d.rescale(u32::from(scale));
                        SqlValue::Decimal(d)
                    }
                    (SqlValue::DateTime(dt), Some(T::Date)) => SqlValue::Date(dt.date_naive()),
                    (SqlValue::NullText, Some(T::Decimal { .. })) => SqlValue::NullDecimal,
                    (SqlValue::NullText, Some(T::Date)) => SqlValue::NullDate,
                    (SqlValue::NullText, Some(T::Time)) => SqlValue::NullTime,
                    (SqlValue::NullText, Some(T::DateTime)) => SqlValue::NullDateTime,
                    (v, _) => v,
                };
                map.insert(col.to_string(), v);
            }
            out.push(map);
        }
        Ok(out)
    }

    /// Sum of `col` over the matching records. `None` when no record has a value.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), otel.kind = "client"))]
    pub async fn sum(self, col: impl ColumnRef<M>) -> Result<Option<f64>> {
        self.numeric("SUM", col.column_name()).await
    }

    /// Mean of `col` over the matching records. `None` when no record has a value.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), otel.kind = "client"))]
    pub async fn avg(self, col: impl ColumnRef<M>) -> Result<Option<f64>> {
        self.numeric("AVG", col.column_name()).await
    }

    async fn numeric(self, func: &str, col: &str) -> Result<Option<f64>> {
        self.f.ensure_valid()?;
        if let Some(msg) = unknown_column::<M>(col) {
            return Err(Error::Invalid(msg));
        }
        let (sql, binds) = self.f.aggregate_sql(func, col);
        let rows = self.conn.rows(&sql, vars(binds)).await?;
        let Some(obj) = rows.first().and_then(object) else {
            return Ok(None);
        };
        let n = obj.get("n").map(as_i64).transpose()?.unwrap_or(0);
        if n == 0 {
            return Ok(None);
        }
        obj.get("v").map(as_f64).transpose().map_err(Error::from)
    }

    /// Smallest value of `col` (numbers, text, dates, ...). `None` when no record has one.
    ///
    /// ```rust,ignore
    /// let oldest: Option<NaiveDate> = Event::filter(&db).min::<NaiveDate>(Event::DATE).await?;
    /// ```
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), otel.kind = "client"))]
    pub async fn min<T: FromSurreal>(self, col: impl ColumnRef<M>) -> Result<Option<T>> {
        self.extreme("MIN", col.column_name()).await
    }

    /// Largest value of `col` (numbers, text, dates, ...). `None` when no record has one.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), otel.kind = "client"))]
    pub async fn max<T: FromSurreal>(self, col: impl ColumnRef<M>) -> Result<Option<T>> {
        self.extreme("MAX", col.column_name()).await
    }

    async fn extreme<T: FromSurreal>(self, func: &str, col: &str) -> Result<Option<T>> {
        self.f.ensure_valid()?;
        if let Some(msg) = unknown_column::<M>(col) {
            return Err(Error::Invalid(msg));
        }
        let (sql, binds) = self.f.aggregate_sql(func, col);
        let rows = self.conn.rows(&sql, vars(binds)).await?;
        rows.first()
            .map(T::from_surreal)
            .transpose()
            .map_err(Error::from)
    }

    /// Select only the columns of the `#[derive(Partial)]` struct `P`.
    ///
    /// Same filters, ordering and limits as `.all()`; relations are not supported.
    pub fn select_as<P: Partial<Model = M>>(self) -> SelectAs<'a, M, P> {
        SelectAs {
            qb: self,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Runs the `.all()` query with `EXPLAIN FULL`: the plan (indexes used) and the
    /// measured rows and time of each operator, as pretty-printed JSON.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "EXPLAIN", db.sql.table = M::table_name(), otel.kind = "client"))]
    pub async fn explain_analyze(&mut self) -> Result<String> {
        self.f.ensure_valid()?;
        let (sql, binds) = self.f.select_sql();
        let plan = self
            .conn
            .value(&format!("{sql} EXPLAIN FULL"), vars(binds))
            .await?;
        serde_json::to_string_pretty(&plan.into_json_value())
            .map_err(|e| Error::Decode(e.to_string()))
    }
}

/// A query selecting a partial struct, built by `QueryBuilder::select_as`.
pub struct SelectAs<'a, M, P> {
    qb: QueryBuilder<'a, M>,
    _phantom: std::marker::PhantomData<P>,
}

impl<M, P> SelectAs<'_, M, P>
where
    M: Model + ModelValues + FromRow,
    P: Partial<Model = M>,
{
    /// Fetch all matching records as `P`.
    #[tracing::instrument(name = "db.query", skip_all, fields(db.system = "surrealdb", db.operation = "SELECT", db.sql.table = M::table_name(), db.rows = tracing::field::Empty, otel.kind = "client"))]
    pub async fn all(self) -> Result<Vec<P>> {
        self.qb.f.ensure_valid()?;
        if !self.qb.f.select_related_specs().is_empty() || !self.qb.f.prefetch_specs().is_empty() {
            return Err(Error::Invalid(
                "select_related / prefetch_related are not supported with select_as".into(),
            ));
        }
        let (sql, binds) = self.qb.f.values_sql(P::columns());
        let rows = self.qb.conn.rows(&sql, vars(binds)).await?;
        self.qb.warn_if_capped(rows.len());
        tracing::Span::current().record("db.rows", rows.len());
        decode_all::<P>(&rows, Some(M::pk_column()))
    }

    /// Fetch the first matching record as `P`.
    pub async fn one(self) -> Result<Option<P>> {
        let rows = self.qb.limit(1).select_as::<P>().all().await?;
        Ok(rows.into_iter().next())
    }
}

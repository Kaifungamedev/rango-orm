//! A returned record (an object) as a [`RangoRow`].

use crate::value::*;
use rango_orm_core::{Error, FromRow, RangoRow, Result, RowError};
use surrealdb::types::{Object, Value};

/// One record. The primary key column is read from the record `id` (its key) when the
/// object has no field of that name; record links read as their key.
pub(crate) struct SurrealRow<'a> {
    obj: &'a Object,
    pk: Option<&'a str>,
}

impl<'a> SurrealRow<'a> {
    pub(crate) fn new(obj: &'a Object, pk: Option<&'a str>) -> Self {
        Self { obj, pk }
    }

    fn field(&self, col: &str) -> Option<&Value> {
        match self.obj.get(col) {
            Some(v) => Some(v),
            None if self.pk == Some(col) => self.obj.get("id"),
            None => None,
        }
    }

    fn get<T>(
        &self,
        col: &str,
        read: impl FnOnce(&Value) -> std::result::Result<T, RowError>,
    ) -> std::result::Result<T, RowError> {
        match self.field(col) {
            Some(v) if !is_null(Some(v)) => {
                read(v).map_err(|e| RowError::new(format!("{col}: {e}")))
            }
            _ => Err(RowError::new(format!("{col}: unexpected NONE"))),
        }
    }
}

impl RangoRow for SurrealRow<'_> {
    fn get_bool(&self, col: &str) -> std::result::Result<bool, RowError> {
        self.get(col, as_bool)
    }
    fn get_i16(&self, col: &str) -> std::result::Result<i16, RowError> {
        self.get(col, |v| {
            as_i64(v).and_then(|i| i16::try_from(i).map_err(|e| RowError::new(e.to_string())))
        })
    }
    fn get_i32(&self, col: &str) -> std::result::Result<i32, RowError> {
        self.get(col, |v| {
            as_i64(v).and_then(|i| i32::try_from(i).map_err(|e| RowError::new(e.to_string())))
        })
    }
    fn get_i64(&self, col: &str) -> std::result::Result<i64, RowError> {
        self.get(col, as_i64)
    }
    fn get_f32(&self, col: &str) -> std::result::Result<f32, RowError> {
        self.get(col, |v| as_f64(v).map(|f| f as f32))
    }
    fn get_f64(&self, col: &str) -> std::result::Result<f64, RowError> {
        self.get(col, as_f64)
    }
    fn get_decimal(&self, col: &str) -> std::result::Result<rango_orm_core::Decimal, RowError> {
        self.get(col, as_decimal)
    }
    fn get_string(&self, col: &str) -> std::result::Result<String, RowError> {
        self.get(col, as_string)
    }
    fn get_bytes(&self, col: &str) -> std::result::Result<Vec<u8>, RowError> {
        self.get(col, as_bytes)
    }
    fn get_uuid(&self, col: &str) -> std::result::Result<uuid::Uuid, RowError> {
        self.get(col, as_uuid)
    }
    fn get_datetime(
        &self,
        col: &str,
    ) -> std::result::Result<chrono::DateTime<chrono::Utc>, RowError> {
        self.get(col, as_datetime)
    }
    fn get_date(&self, col: &str) -> std::result::Result<chrono::NaiveDate, RowError> {
        self.get(col, as_date)
    }
    fn get_time(&self, col: &str) -> std::result::Result<chrono::NaiveTime, RowError> {
        self.get(col, as_time)
    }
    fn get_json(&self, col: &str) -> std::result::Result<serde_json::Value, RowError> {
        // A JSON column holding JSON `null` is a value, not a missing field.
        match self.field(col) {
            Some(Value::None) | None => Err(RowError::new(format!("{col}: unexpected NONE"))),
            Some(v) => Ok(as_json(v)),
        }
    }
    fn is_null(&self, col: &str) -> bool {
        is_null(self.field(col))
    }
}

/// The object of a returned value (`NONE` for a dangling link or a missing nested record).
pub(crate) fn object(v: &Value) -> Option<&Object> {
    match v {
        Value::Object(o) => Some(o),
        _ => None,
    }
}

/// Decode one record of a model whose PK column is `pk`.
pub(crate) fn decode<M: FromRow>(v: &Value, pk: Option<&str>) -> Result<M> {
    let obj = object(v).ok_or_else(|| Error::Decode("expected a record object".into()))?;
    M::from_row(&SurrealRow::new(obj, pk)).map_err(Error::from)
}

pub(crate) fn decode_all<M: FromRow>(rows: &[Value], pk: Option<&str>) -> Result<Vec<M>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(decode(row, pk)?);
    }
    Ok(out)
}

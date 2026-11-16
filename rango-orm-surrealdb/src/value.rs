//! `SqlValue` <-> SurrealDB `Value`, and the record conventions of the storage model:
//! a primary key is the record key (`user:⟨uuid⟩`), a foreign key is a `record<target>`.
//!
//! A date is a `datetime` at midnight UTC and a time of day a `duration` since midnight:
//! the SDK writes requests to a server in plain `cbor`, which turns SurrealDB 4's own
//! `date` / `time` values into exactly those, so storing them that way keeps the embedded
//! and the remote engines identical.

use rango_orm_core::{Decimal, Model, ModelValues, RowError, SqlValue};
use std::borrow::Cow;
use std::collections::HashMap;
use surrealdb::types::{
    Bytes, Datetime, Duration, Number, Object, RecordId, RecordIdKey, SurrealValue, Uuid, Value,
};

const DAY_SECS: u64 = 86_400;

/// A date as stored: the `datetime` of its midnight, UTC.
pub fn date_value(d: chrono::NaiveDate) -> Value {
    Value::Datetime(Datetime::from(d.and_time(chrono::NaiveTime::MIN).and_utc()))
}

/// A time of day as stored: the `duration` since midnight.
pub fn time_value(t: chrono::NaiveTime) -> Value {
    use chrono::Timelike;
    // Leap-second nanos (>= 1e9) fold into the last second.
    let nanos = t.nanosecond().min(999_999_999);
    Value::Duration(Duration::new(
        u64::from(t.num_seconds_from_midnight()),
        nanos,
    ))
}

/// A bound value. Every SQL NULL is `NONE` (an absent field).
pub(crate) fn to_value(v: SqlValue) -> Value {
    match v {
        v if v.is_null() => Value::None,
        SqlValue::Bool(b) => Value::Bool(b),
        SqlValue::SmallInt(i) => Value::Number(Number::Int(i.into())),
        SqlValue::Int(i) => Value::Number(Number::Int(i.into())),
        SqlValue::BigInt(i) => Value::Number(Number::Int(i)),
        SqlValue::Float(f) => Value::Number(Number::Float(f.into())),
        SqlValue::Double(f) => Value::Number(Number::Float(f)),
        SqlValue::Decimal(d) => Value::Number(Number::Decimal(d)),
        SqlValue::Text(s) => Value::String(s),
        SqlValue::Bytes(b) => Value::Bytes(Bytes::from(b)),
        SqlValue::Uuid(u) => Value::Uuid(Uuid::from(u)),
        SqlValue::DateTime(d) => Value::Datetime(Datetime::from(d)),
        SqlValue::Date(d) => date_value(d),
        SqlValue::Time(t) => time_value(t),
        SqlValue::Json(j) => j.into_value(),
        _ => Value::None,
    }
}

/// The record key of a primary key value (UUID, integer or text).
fn record_key(v: &SqlValue) -> Option<RecordIdKey> {
    Some(match v {
        SqlValue::Uuid(u) => RecordIdKey::Uuid(Uuid::from(*u)),
        SqlValue::SmallInt(i) => RecordIdKey::Number((*i).into()),
        SqlValue::Int(i) => RecordIdKey::Number((*i).into()),
        SqlValue::BigInt(i) => RecordIdKey::Number(*i),
        SqlValue::Text(s) => RecordIdKey::String(s.clone()),
        _ => return None,
    })
}

/// `table:key` for a key value; `NONE` for a NULL.
pub(crate) fn record(table: &str, v: SqlValue) -> Value {
    match record_key(&v) {
        Some(key) => Value::RecordId(RecordId::new(table, key)),
        None => to_value(v),
    }
}

/// How the columns of `M` are stored: the record table of each PK / FK column.
pub(crate) struct Columns {
    pk: &'static str,
    links: HashMap<String, String>,
}

impl Columns {
    pub(crate) fn of<M: Model + ModelValues>() -> Self {
        let links = M::schema()
            .columns
            .into_iter()
            .filter_map(|c| c.references.map(|fk| (c.name, fk.table)))
            .collect();
        Self {
            pk: M::pk_column(),
            links,
        }
    }

    /// A column value as stored: FKs as records of their target table.
    pub(crate) fn value(&self, table: &str, col: &str, v: SqlValue) -> Value {
        if col == self.pk {
            return record(table, v);
        }
        match self.links.get(col) {
            Some(target) => record(target, v),
            None => to_value(v),
        }
    }

    /// The fields of a record: every non-PK column (`NONE` for NULLs, so they are unset).
    pub(crate) fn object(&self, table: &str, fields: Vec<(&'static str, SqlValue)>) -> Object {
        let mut obj = Object::new();
        for (col, v) in fields {
            if col != self.pk {
                obj.insert(col, self.value(table, col, v));
            }
        }
        obj
    }
}

/// The record key of a `RecordId` as a plain value; anything else as is.
pub(crate) fn plain(v: &Value) -> Cow<'_, Value> {
    match v {
        Value::RecordId(r) => Cow::Owned(match &r.key {
            RecordIdKey::Number(i) => Value::Number(Number::Int(*i)),
            RecordIdKey::String(s) => Value::String(s.clone()),
            RecordIdKey::Uuid(u) => Value::Uuid(*u),
            _ => v.clone(),
        }),
        _ => Cow::Borrowed(v),
    }
}

fn wrong(what: &str, v: &Value) -> RowError {
    RowError::new(format!("expected {what}, found {}", kind(v)))
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::None => "NONE",
        Value::Null => "NULL",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Bytes(_) => "bytes",
        Value::Datetime(_) => "datetime",
        Value::Duration(_) => "duration",
        Value::Uuid(_) => "uuid",
        Value::RecordId(_) => "record",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
        Value::Date(_) => "date",
        Value::Time(_) => "time",
        _ => "value",
    }
}

pub(crate) fn as_bool(v: &Value) -> Result<bool, RowError> {
    match v {
        Value::Bool(b) => Ok(*b),
        v => Err(wrong("bool", v)),
    }
}

pub(crate) fn as_i64(v: &Value) -> Result<i64, RowError> {
    match &*plain(v) {
        Value::Number(Number::Decimal(d)) if d.fract().is_zero() => {
            i64::try_from(*d).map_err(|e| RowError::new(e.to_string()))
        }
        Value::Number(Number::Float(f)) if f.fract() == 0.0 => Ok(*f as i64),
        Value::Number(n) => n.to_int().ok_or_else(|| wrong("integer", v)),
        v => Err(wrong("integer", v)),
    }
}

pub(crate) fn as_f64(v: &Value) -> Result<f64, RowError> {
    match v {
        Value::Number(Number::Decimal(d)) => {
            f64::try_from(*d).map_err(|e| RowError::new(e.to_string()))
        }
        Value::Number(n) => n.to_f64().ok_or_else(|| wrong("number", v)),
        v => Err(wrong("number", v)),
    }
}

pub(crate) fn as_decimal(v: &Value) -> Result<Decimal, RowError> {
    let err = |e: &dyn std::fmt::Display| RowError::new(e.to_string());
    match v {
        Value::Number(Number::Decimal(d)) => Ok(*d),
        Value::Number(Number::BigDecimal(d)) => d.to_string().parse().map_err(|e| err(&e)),
        Value::Number(Number::Float(f)) => Decimal::try_from(*f).map_err(|e| err(&e)),
        Value::Number(n) => n
            .to_int()
            .map(Decimal::from)
            .ok_or_else(|| wrong("decimal", v)),
        Value::String(s) => s.parse().map_err(|e| err(&e)),
        v => Err(wrong("decimal", v)),
    }
}

pub(crate) fn as_string(v: &Value) -> Result<String, RowError> {
    match &*plain(v) {
        Value::String(s) => Ok(s.clone()),
        v => Err(wrong("string", v)),
    }
}

pub(crate) fn as_bytes(v: &Value) -> Result<Vec<u8>, RowError> {
    match v {
        Value::Bytes(b) => Ok(b.clone().into_inner().to_vec()),
        v => Err(wrong("bytes", v)),
    }
}

pub(crate) fn as_uuid(v: &Value) -> Result<uuid::Uuid, RowError> {
    match &*plain(v) {
        Value::Uuid(u) => Ok((*u).into_inner()),
        Value::String(s) => s
            .parse()
            .map_err(|e: uuid::Error| RowError::new(e.to_string())),
        v => Err(wrong("uuid", v)),
    }
}

pub(crate) fn as_datetime(v: &Value) -> Result<chrono::DateTime<chrono::Utc>, RowError> {
    match v {
        Value::Datetime(d) => Ok((*d).into_inner()),
        Value::String(s) => s
            .parse()
            .map_err(|e: chrono::ParseError| RowError::new(e.to_string())),
        v => Err(wrong("datetime", v)),
    }
}

pub(crate) fn as_date(v: &Value) -> Result<chrono::NaiveDate, RowError> {
    match v {
        Value::Date(d) => Ok((*d).into_inner()),
        Value::Datetime(d) => Ok((*d).into_inner().date_naive()),
        v => Err(wrong("date", v)),
    }
}

pub(crate) fn as_time(v: &Value) -> Result<chrono::NaiveTime, RowError> {
    match v {
        Value::Time(t) => (*t)
            .into_inner()
            .ok_or_else(|| RowError::new("time out of range")),
        Value::Duration(d) if d.secs() < DAY_SECS => {
            chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                d.secs() as u32,
                d.into_inner().subsec_nanos(),
            )
            .ok_or_else(|| RowError::new("time out of range"))
        }
        v => Err(wrong("time", v)),
    }
}

pub(crate) fn as_json(v: &Value) -> serde_json::Value {
    plain(v).into_owned().into_json_value()
}

pub(crate) fn is_null(v: Option<&Value>) -> bool {
    matches!(v, None | Some(Value::None | Value::Null))
}

/// A returned value as a `SqlValue` (for `.values()`): keys of records, JSON for objects.
pub(crate) fn to_sql_value(v: &Value) -> SqlValue {
    match &*plain(v) {
        Value::None | Value::Null => SqlValue::NullText,
        Value::Bool(b) => SqlValue::Bool(*b),
        Value::Number(Number::Decimal(d)) => SqlValue::Decimal(*d),
        Value::Number(Number::Float(f)) => SqlValue::Double(*f),
        Value::Number(n) => match n.to_int() {
            Some(i) => SqlValue::BigInt(i),
            None => n.to_f64().map_or(SqlValue::NullDouble, SqlValue::Double),
        },
        Value::String(s) => SqlValue::Text(s.clone()),
        Value::Uuid(u) => SqlValue::Uuid((*u).into_inner()),
        Value::Datetime(d) => SqlValue::DateTime((*d).into_inner()),
        Value::Date(d) => SqlValue::Date((*d).into_inner()),
        Value::Time(t) => (*t).into_inner().map_or(SqlValue::NullTime, SqlValue::Time),
        v @ Value::Duration(d) if d.secs() < DAY_SECS => {
            as_time(v).map_or(SqlValue::NullTime, SqlValue::Time)
        }
        Value::Bytes(b) => SqlValue::Bytes(b.clone().into_inner().to_vec()),
        other => SqlValue::Json(other.clone().into_json_value()),
    }
}

/// A Rust type read from a single SurrealDB value: the result type of `min` / `max` and
/// `raw_scalar`. Record links decode as their key.
pub trait FromSurreal: Sized {
    #[doc(hidden)]
    fn from_surreal(v: &Value) -> Result<Self, RowError>;
}

macro_rules! from_surreal {
    ($($t:ty => $f:expr),* $(,)?) => {
        $(impl FromSurreal for $t {
            fn from_surreal(v: &Value) -> Result<Self, RowError> {
                $f(v)
            }
        })*
    };
}

from_surreal! {
    bool => as_bool,
    i64 => as_i64,
    i32 => |v| as_i64(v).and_then(|i| i32::try_from(i).map_err(|e| RowError::new(e.to_string()))),
    i16 => |v| as_i64(v).and_then(|i| i16::try_from(i).map_err(|e| RowError::new(e.to_string()))),
    f64 => as_f64,
    f32 => |v| as_f64(v).map(|f| f as f32),
    Decimal => as_decimal,
    String => as_string,
    Vec<u8> => as_bytes,
    uuid::Uuid => as_uuid,
    chrono::DateTime<chrono::Utc> => as_datetime,
    chrono::NaiveDate => as_date,
    chrono::NaiveTime => as_time,
    serde_json::Value => |v| Ok(as_json(v)),
}

impl<T: FromSurreal> FromSurreal for Option<T> {
    fn from_surreal(v: &Value) -> Result<Self, RowError> {
        if is_null(Some(v)) {
            Ok(None)
        } else {
            T::from_surreal(v).map(Some)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_records() {
        let id = uuid::Uuid::new_v4();
        let rec = record("user", SqlValue::Uuid(id));
        assert_eq!(as_uuid(&rec).unwrap(), id);
        assert_eq!(record("user", SqlValue::NullUuid), Value::None);
        assert_eq!(as_i64(&record("t", SqlValue::Int(7))).unwrap(), 7);
        assert_eq!(
            to_sql_value(&record("t", SqlValue::BigInt(7))),
            SqlValue::BigInt(7)
        );
        let d: Decimal = "12.340".parse().unwrap();
        assert_eq!(as_decimal(&to_value(SqlValue::Decimal(d))).unwrap(), d);
        assert_eq!(
            as_json(&to_value(SqlValue::Json(
                serde_json::json!({"a": [1, null]})
            ))),
            serde_json::json!({"a": [1, null]})
        );
        assert!(i16::from_surreal(&to_value(SqlValue::BigInt(1 << 20))).is_err());
    }
}

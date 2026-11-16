//! Record operations: CRUD, bulk and raw SurrealQL.

use crate::conn::{Conn, Vars};
use crate::row::{decode, decode_all};
use crate::value::{Columns, record};
use rango_orm_core::{
    ColumnType, Error, FromRow, Model, ModelValues, Result, SqlValue, quote_ident_surreal as q,
    surreal_sequence_name,
};
use surrealdb::types::{Object, Value};

mod bulk;
mod crud;
mod raw;

pub use bulk::*;
pub(crate) use crud::decode_model;
pub use crud::*;
pub use raw::*;

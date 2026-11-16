//! With only the SurrealDB backend, the prelude carries its ops; with every backend,
//! SurrealDB stays reachable by path.
#![cfg(feature = "surrealdb")]

use rango::prelude::*;

#[derive(Model)]
pub struct Item {
    pub id: FieldUuid,
    pub name: FieldVarchar<1, 100>,
}

#[cfg(not(any(feature = "postgres", feature = "sqlite", feature = "mysql")))]
fn _prelude(db: &SurrealPool) -> QueryBuilder<'_, Item> {
    Item::filter(db).eq(Item::NAME, "x")
}

fn _by_path(db: &SurrealPool) -> rango::surrealdb::QueryBuilder<'_, Item> {
    <Item as rango::surrealdb::RangoFilterExt>::filter(db).eq(Item::NAME, "x")
}

#[test]
fn prelude_compiles_with_surrealdb() {}

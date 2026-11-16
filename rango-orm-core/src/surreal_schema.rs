//! SurrealQL schema statements (`DEFINE TABLE / FIELD / INDEX / EVENT / SEQUENCE`) for the
//! storage model of `rango-orm-surrealdb`. DB-free, shared by the backend, its tests and
//! `rango makemigrations`.
//!
//! - a table is `SCHEMAFULL`, commented `rango:pk=<column>:<uuid|int|bigint|text>` (or
//!   `rango:pk=none` for a pivot) so `rango dump` knows where the key lives;
//! - the primary key is the record key, never a field; an auto-increment key is numbered
//!   by the sequence [`crate::surreal_sequence_name`];
//! - a foreign key is a `record<target>` field with `REFERENCE ON DELETE ...`, plus an
//!   `ASSERT` that the record exists, which throws `rango:fk:<table>_<col>_fkey`;
//! - a date is a `datetime` at midnight UTC and a time of day a `duration` since midnight
//!   (commented `rango:date` / `rango:time`): the SDK sends both that way to a server;
//! - a CHECK constraint is an event throwing `rango:check:<name>` on create / update;
//! - a partial unique constraint (`UNIQUE ... WHERE`) has no SurrealDB form: skipped with
//!   a comment, as on MySQL.

use crate::constraints::{Constraint, constraint_name};
use crate::{
    ColumnDef, ColumnType, DefaultValue, ReferentialAction, TableSchema, quote_ident_surreal as q,
    quote_str_surreal, surreal_sequence_name,
};

/// The PK column of `schema`, if it has one (pivot tables do not).
pub fn primary_key(schema: &TableSchema) -> Option<&ColumnDef> {
    schema.columns.iter().find(|c| c.primary_key)
}

/// The record key kind of a PK column, as written in the table comment.
pub fn key_kind(col_type: &ColumnType) -> &'static str {
    match col_type {
        ColumnType::Uuid => "uuid",
        ColumnType::SmallInt | ColumnType::Int | ColumnType::AutoInt => "int",
        ColumnType::BigInt | ColumnType::AutoBigInt => "bigint",
        _ => "text",
    }
}

/// `rango:pk=id:uuid`, `rango:pk=none`.
pub fn table_comment(schema: &TableSchema) -> String {
    match primary_key(schema) {
        Some(pk) => format!("rango:pk={}:{}", pk.name, key_kind(&pk.col_type)),
        None => "rango:pk=none".to_string(),
    }
}

/// Parse a table comment written by [`table_comment`]: `Some((pk column, key kind))`,
/// `Some(None)` for a table without a PK, `None` for a table Rango did not define.
pub fn parse_table_comment(comment: &str) -> Option<Option<(String, String)>> {
    let rest = comment.strip_prefix("rango:pk=")?;
    if rest == "none" {
        return Some(None);
    }
    let (col, kind) = rest.rsplit_once(':')?;
    Some(Some((col.to_string(), kind.to_string())))
}

/// True for a M2M pivot: two NOT NULL key columns and no PK.
pub fn is_pivot(schema: &TableSchema) -> bool {
    schema.columns.len() == 2
        && schema.columns.iter().all(|c| {
            !c.primary_key
                && !c.nullable
                && matches!(
                    c.col_type,
                    ColumnType::Uuid | ColumnType::Int | ColumnType::BigInt
                )
        })
}

fn base_type(col: &ColumnDef) -> String {
    if let Some(fk) = &col.references {
        return format!("record<{}>", q(&fk.table));
    }
    match &col.col_type {
        ColumnType::Bool => "bool",
        ColumnType::SmallInt
        | ColumnType::Int
        | ColumnType::BigInt
        | ColumnType::AutoInt
        | ColumnType::AutoBigInt => "int",
        ColumnType::Float | ColumnType::Double => "float",
        ColumnType::Decimal { .. } => "decimal",
        ColumnType::Text | ColumnType::Varchar(_) => "string",
        ColumnType::Bytea => "bytes",
        ColumnType::Uuid => "uuid",
        ColumnType::Date => "datetime",
        ColumnType::Time => "duration",
        ColumnType::DateTime => "datetime",
        ColumnType::Json | ColumnType::Jsonb => "object | array | string | number | bool | null",
    }
    .to_string()
}

/// The `TYPE` of a field (`option<...>` when nullable, `FLEXIBLE` for JSON).
pub fn field_type(col: &ColumnDef) -> String {
    let ty = base_type(col);
    let ty = if col.nullable {
        format!("option<{ty}>")
    } else {
        ty
    };
    match col.col_type {
        ColumnType::Json | ColumnType::Jsonb if col.references.is_none() => {
            format!("{ty} FLEXIBLE")
        }
        _ => ty,
    }
}

fn on_delete(action: &ReferentialAction) -> &'static str {
    match action {
        ReferentialAction::Cascade => "CASCADE",
        // SurrealDB has no "set default": the field is unset like SET NULL.
        ReferentialAction::SetNull | ReferentialAction::SetDefault => "UNSET",
        ReferentialAction::Restrict | ReferentialAction::NoAction => "REJECT",
    }
}

/// Name of the FK assertion a violation reports (`rango:fk:<name>`).
pub fn fk_name(table: &str, col: &str) -> String {
    format!("{table}_{col}_fkey")
}

/// `DEFINE FIELD` of a non-PK column. `mode` is `IF NOT EXISTS` or `OVERWRITE`.
pub fn define_field(table: &str, col: &ColumnDef, mode: &str) -> String {
    let mut def = format!(
        "DEFINE FIELD {mode} {} ON {} TYPE {}",
        q(&col.name),
        q(table),
        field_type(col)
    );
    let mut asserts: Vec<String> = Vec::new();
    if let Some(fk) = &col.references {
        def.push_str(&format!(
            " REFERENCE ON DELETE {}",
            on_delete(&fk.on_delete)
        ));
        asserts.push(format!(
            "record::exists($value) OR THROW {}",
            quote_str_surreal(&format!("rango:fk:{}", fk_name(table, &col.name)))
        ));
    } else {
        match col.col_type {
            ColumnType::Varchar(n) => asserts.push(format!("string::len($value) <= {n}")),
            ColumnType::SmallInt => asserts.push("$value >= -32768 AND $value <= 32767".into()),
            ColumnType::Int => {
                asserts.push("$value >= -2147483648 AND $value <= 2147483647".into())
            }
            ColumnType::Time => asserts.push("$value < 1d".into()),
            _ => {}
        }
    }
    match &col.default {
        Some(DefaultValue::CurrentTimestamp) => def.push_str(" DEFAULT time::now()"),
        Some(DefaultValue::GeneratedUuid) => def.push_str(" DEFAULT rand::uuid::v4()"),
        // Developer SQL, trusted like on the SQL backends.
        Some(DefaultValue::Literal(v)) => def.push_str(&format!(" DEFAULT {v}")),
        None => {}
    }
    if !asserts.is_empty() {
        let cond = asserts
            .iter()
            .map(|a| format!("({a})"))
            .collect::<Vec<_>>()
            .join(" AND ");
        if col.nullable || col.references.is_some() {
            def.push_str(&format!(" ASSERT $value = NONE OR {cond}"));
        } else {
            def.push_str(&format!(" ASSERT {cond}"));
        }
    }
    match col.col_type {
        ColumnType::Date => def.push_str(" COMMENT 'rango:date'"),
        ColumnType::Time => def.push_str(" COMMENT 'rango:time'"),
        _ => {}
    }
    def.push_str(";\n");
    def
}

/// Indexes of one column: `{t}_{c}_unique` (UNIQUE) or `{t}_{c}_idx`.
pub fn column_indexes(table: &str, col: &ColumnDef) -> String {
    if col.primary_key {
        return String::new();
    }
    if col.unique {
        define_index(
            table,
            &format!("{table}_{}_unique", col.name),
            &[&col.name],
            true,
        )
    } else if col.index {
        define_index(
            table,
            &format!("{table}_{}_idx", col.name),
            &[&col.name],
            false,
        )
    } else {
        String::new()
    }
}

pub fn define_index(table: &str, name: &str, fields: &[&str], unique: bool) -> String {
    let fields = fields.iter().map(|f| q(f)).collect::<Vec<_>>().join(", ");
    format!(
        "DEFINE INDEX IF NOT EXISTS {} ON {} FIELDS {fields}{};\n",
        q(name),
        q(table),
        if unique { " UNIQUE" } else { "" }
    )
}

/// `DEFINE INDEX` / `DEFINE EVENT` of a table-level constraint (or a skip comment).
pub fn define_constraint(table: &str, constraint: &Constraint, name: &str) -> String {
    match constraint {
        Constraint::Unique(u) if u.condition.is_none() => {
            let fields: Vec<&str> = u.fields.iter().map(String::as_str).collect();
            define_index(table, name, &fields, true)
        }
        Constraint::Unique(_) => format!(
            "-- SurrealDB has no partial unique index: constraint \"{name}\" on \"{table}\" was \
             skipped (enforce it in application code).\n"
        ),
        // Developer SQL, trusted: `age >= 18` reads the written record's fields.
        Constraint::Check(c) => format!(
            "DEFINE EVENT IF NOT EXISTS {} ON {} WHEN $event != 'DELETE' AND !({}) THEN {{ THROW {} }};\n",
            q(name),
            q(table),
            c.sql,
            quote_str_surreal(&format!("rango:check:{name}"))
        ),
    }
}

/// Everything a table needs: the table, its fields, indexes, constraints and sequence.
pub fn define_table(schema: &TableSchema) -> String {
    let t = &schema.table_name;
    let mut out = format!(
        "DEFINE TABLE IF NOT EXISTS {} TYPE NORMAL SCHEMAFULL COMMENT {};\n",
        q(t),
        quote_str_surreal(&table_comment(schema))
    );
    for col in schema.columns.iter().filter(|c| !c.primary_key) {
        out.push_str(&define_field(t, col, "IF NOT EXISTS"));
    }
    if let Some(pk) = primary_key(schema)
        && matches!(pk.col_type, ColumnType::AutoInt | ColumnType::AutoBigInt)
    {
        out.push_str(&format!(
            "DEFINE SEQUENCE IF NOT EXISTS {} START 1;\n",
            q(&surreal_sequence_name(t, &pk.name))
        ));
    }
    if is_pivot(schema) {
        let fields: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        out.push_str(&define_index(t, &format!("{t}_pkey"), &fields, true));
    }
    for col in &schema.columns {
        out.push_str(&column_indexes(t, col));
    }
    for (i, c) in schema.constraints.iter().enumerate() {
        out.push_str(&define_constraint(t, c, &constraint_name(t, c, i)));
    }
    out
}

/// The migration ledger, a Rango-owned table like `_rango_migrations` on SQL backends.
pub const TRACKING_TABLE_DDL: &str = "DEFINE TABLE IF NOT EXISTS _rango_migrations TYPE NORMAL SCHEMAFULL COMMENT 'rango:pk=none';\n\
DEFINE FIELD IF NOT EXISTS name ON _rango_migrations TYPE string;\n\
DEFINE FIELD IF NOT EXISTS applied_at ON _rango_migrations TYPE datetime DEFAULT time::now();\n\
DEFINE INDEX IF NOT EXISTS _rango_migrations_name_unique ON _rango_migrations FIELDS name UNIQUE;\n";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CheckConstraint, FkReference, UniqueConstraint};

    fn col(name: &str, col_type: ColumnType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            col_type,
            nullable: false,
            primary_key: false,
            unique: false,
            default: None,
            references: None,
            index: false,
        }
    }

    #[test]
    fn defines_a_model_table() {
        let mut id = col("id", ColumnType::AutoBigInt);
        id.primary_key = true;
        let mut email = col("email", ColumnType::Varchar(100));
        email.unique = true;
        let mut author = col("author_id", ColumnType::Uuid);
        author.nullable = true;
        author.index = true;
        author.references = Some(FkReference {
            table: "user".into(),
            column: "id".into(),
            on_delete: ReferentialAction::SetNull,
            on_update: ReferentialAction::NoAction,
        });
        let schema = TableSchema {
            table_name: "post".into(),
            columns: vec![id, email, author, col("meta", ColumnType::Json)],
            constraints: vec![
                CheckConstraint::new("length > 0").name("chk_len").into(),
                UniqueConstraint::on(&["email", "author_id"]).into(),
            ],
            ..TableSchema::default()
        };
        let sql = define_table(&schema);
        assert_eq!(
            sql,
            "DEFINE TABLE IF NOT EXISTS `post` TYPE NORMAL SCHEMAFULL COMMENT 'rango:pk=id:bigint';\n\
             DEFINE FIELD IF NOT EXISTS `email` ON `post` TYPE string ASSERT (string::len($value) <= 100);\n\
             DEFINE FIELD IF NOT EXISTS `author_id` ON `post` TYPE option<record<`user`>> REFERENCE ON DELETE UNSET \
             ASSERT $value = NONE OR (record::exists($value) OR THROW 'rango:fk:post_author_id_fkey');\n\
             DEFINE FIELD IF NOT EXISTS `meta` ON `post` TYPE object | array | string | number | bool | null FLEXIBLE;\n\
             DEFINE SEQUENCE IF NOT EXISTS `post_id_seq` START 1;\n\
             DEFINE INDEX IF NOT EXISTS `post_email_unique` ON `post` FIELDS `email` UNIQUE;\n\
             DEFINE INDEX IF NOT EXISTS `post_author_id_idx` ON `post` FIELDS `author_id`;\n\
             DEFINE EVENT IF NOT EXISTS `chk_len` ON `post` WHEN $event != 'DELETE' AND !(length > 0) \
             THEN { THROW 'rango:check:chk_len' };\n\
             DEFINE INDEX IF NOT EXISTS `post_email_author_id_unique` ON `post` FIELDS `email`, `author_id` UNIQUE;\n"
        );
        assert_eq!(
            parse_table_comment("rango:pk=id:bigint"),
            Some(Some(("id".into(), "bigint".into())))
        );
        assert_eq!(parse_table_comment("rango:pk=none"), Some(None));
        assert_eq!(parse_table_comment("other"), None);
    }
}

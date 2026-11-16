//! SurrealQL migrations: schema diffs as `DEFINE` / `REMOVE` statements.
//!
//! SurrealDB redefines in place (`DEFINE FIELD OVERWRITE`), so there is no table rebuild.
//! Existing values are not converted when a field type changes: the new type applies to
//! the next writes (the migration says so in a comment).

use super::*;
use rango_orm_core::quote_ident_surreal as q;
use rango_orm_core::surreal_schema::{
    column_indexes, define_constraint, define_field, define_index, define_table,
};

/// SurrealQL for `diffs`. `schemas` are the new model schemas: renames, reference changes
/// and new constraints read their full definitions from them.
pub(crate) fn generate_surql_from_diff(diffs: &[SchemaDiff], schemas: &[TableSchema]) -> String {
    let (mut creates, mut others, mut drops) = (Vec::new(), Vec::new(), Vec::new());
    for d in diffs {
        match d {
            SchemaDiff::CreateTable(s) => creates.push(s.clone()),
            SchemaDiff::DropTable(_) => drops.push(d),
            _ => others.push(d),
        }
    }
    let mut out = String::new();
    // A record<target> field may name a table defined later in the same file: SurrealDB
    // accepts it, but defining parents first keeps the file readable.
    for i in sort_create_tables_topologically(&creates) {
        out.push_str(&define_table(&creates[i]));
        out.push('\n');
    }
    for d in others.into_iter().chain(drops) {
        out.push_str(&diff_to_surql(d, schemas));
    }
    out
}

fn column_of<'a>(schemas: &'a [TableSchema], table: &str, column: &str) -> Option<&'a ColumnDef> {
    schemas
        .iter()
        .find(|s| s.table_name == table)?
        .columns
        .iter()
        .find(|c| c.name == column)
}

fn remove_column_indexes(table: &str, column: &str) -> String {
    format!(
        "REMOVE INDEX IF EXISTS {} ON {t};\nREMOVE INDEX IF EXISTS {} ON {t};\n",
        q(&format!("{table}_{column}_unique")),
        q(&format!("{table}_{column}_idx")),
        t = q(table)
    )
}

fn diff_to_surql(d: &SchemaDiff, schemas: &[TableSchema]) -> String {
    let missing = |table: &str, column: &str| {
        format!("-- column \"{table}\".\"{column}\" is not in the models: nothing generated.\n\n")
    };
    match d {
        SchemaDiff::CreateTable(schema) => format!("{}\n", define_table(schema)),
        SchemaDiff::DropTable(name) => format!("REMOVE TABLE IF EXISTS {};\n\n", q(name)),
        SchemaDiff::AddColumn { table, column } => format!(
            "{}{}\n",
            define_field(table, column, "IF NOT EXISTS"),
            column_indexes(table, column)
        ),
        SchemaDiff::DropColumn { table, column } => format!(
            "{}REMOVE FIELD IF EXISTS {c} ON {t};\nUPDATE {t} UNSET {c} RETURN NONE;\n\n",
            remove_column_indexes(table, column),
            c = q(column),
            t = q(table)
        ),
        SchemaDiff::AlterColumnType { table, column } => format!(
            "-- Existing values of \"{table}\".\"{}\" keep their old type until rewritten.\n{}\n",
            column.name,
            define_field(table, column, "OVERWRITE")
        ),
        SchemaDiff::AlterColumnNullable { table, column } => {
            format!("{}\n", define_field(table, column, "OVERWRITE"))
        }
        SchemaDiff::AlterColumnUnique {
            table,
            column,
            unique,
        } => {
            let name = format!("{table}_{column}_unique");
            if *unique {
                format!("{}\n", define_index(table, &name, &[column], true))
            } else {
                format!("REMOVE INDEX IF EXISTS {} ON {};\n\n", q(&name), q(table))
            }
        }
        SchemaDiff::AlterColumnIndex {
            table,
            column,
            index,
            ..
        } => {
            let name = format!("{table}_{column}_idx");
            if *index {
                format!("{}\n", define_index(table, &name, &[column], false))
            } else {
                format!("REMOVE INDEX IF EXISTS {} ON {};\n\n", q(&name), q(table))
            }
        }
        SchemaDiff::AlterColumnReference { table, column, .. } => {
            match column_of(schemas, table, column) {
                Some(col) => format!("{}\n", define_field(table, col, "OVERWRITE")),
                None => missing(table, column),
            }
        }
        SchemaDiff::AddConstraint { table, constraint } => {
            let typed = schemas
                .iter()
                .find(|s| &s.table_name == table)
                .and_then(|s| {
                    s.constraints
                        .iter()
                        .enumerate()
                        .find(|(i, c)| constraint_name(table, c, *i) == constraint.name)
                        .map(|(_, c)| c)
                });
            match typed {
                Some(c) => format!("{}\n", define_constraint(table, c, &constraint.name)),
                None => format!(
                    "-- constraint \"{}\" on \"{table}\" is not in the models: nothing generated.\n\n",
                    constraint.name
                ),
            }
        }
        SchemaDiff::DropConstraint { table, name, .. } => format!(
            "REMOVE INDEX IF EXISTS {n} ON {t};\nREMOVE EVENT IF EXISTS {n} ON {t};\n\n",
            n = q(name),
            t = q(table)
        ),
        SchemaDiff::RenameColumn { table, from, to } => match column_of(schemas, table, to) {
            Some(col) => format!(
                "{}UPDATE {t} SET {new} = {old}, {old} = NONE RETURN NONE;\n{}REMOVE FIELD IF EXISTS {old} ON {t};\n{}\n",
                define_field(table, col, "IF NOT EXISTS"),
                remove_column_indexes(table, from),
                column_indexes(table, col),
                t = q(table),
                new = q(to),
                old = q(from),
            ),
            None => missing(table, to),
        },
        SchemaDiff::RebuildTable { schema, .. } => format!("{}\n", define_table(schema)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, col_type: ColumnType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            col_type,
            nullable: false,
            primary_key: name == "id",
            unique: false,
            default: None,
            references: None,
            index: false,
        }
    }

    #[test]
    fn alters_rename_and_drops() {
        let schema = TableSchema {
            table_name: "user".into(),
            columns: vec![
                col("id", ColumnType::Uuid),
                col("full_name", ColumnType::Text),
            ],
            ..TableSchema::default()
        };
        let mut bigger = col("age", ColumnType::BigInt);
        bigger.nullable = true;
        let sql = generate_surql_from_diff(
            &[
                SchemaDiff::DropTable("old".into()),
                SchemaDiff::RenameColumn {
                    table: "user".into(),
                    from: "name".into(),
                    to: "full_name".into(),
                },
                SchemaDiff::AlterColumnType {
                    table: "user".into(),
                    column: bigger,
                },
                SchemaDiff::DropColumn {
                    table: "user".into(),
                    column: "nick".into(),
                },
            ],
            &[schema],
        );
        assert_eq!(
            sql,
            "DEFINE FIELD IF NOT EXISTS `full_name` ON `user` TYPE string;\n\
             UPDATE `user` SET `full_name` = `name`, `name` = NONE RETURN NONE;\n\
             REMOVE INDEX IF EXISTS `user_name_unique` ON `user`;\n\
             REMOVE INDEX IF EXISTS `user_name_idx` ON `user`;\n\
             REMOVE FIELD IF EXISTS `name` ON `user`;\n\n\
             -- Existing values of \"user\".\"age\" keep their old type until rewritten.\n\
             DEFINE FIELD OVERWRITE `age` ON `user` TYPE option<int>;\n\n\
             REMOVE INDEX IF EXISTS `user_nick_unique` ON `user`;\n\
             REMOVE INDEX IF EXISTS `user_nick_idx` ON `user`;\n\
             REMOVE FIELD IF EXISTS `nick` ON `user`;\n\
             UPDATE `user` UNSET `nick` RETURN NONE;\n\n\
             REMOVE TABLE IF EXISTS `old`;\n\n"
        );
    }
}

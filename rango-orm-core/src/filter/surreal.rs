//! SurrealQL rendering of a [`Filter`] (SurrealDB 4).
//!
//! The storage model, shared with `rango-orm-surrealdb` and `rango makemigrations`:
//! - a model is a `SCHEMAFULL` table; its primary key is the record key, so the PK column
//!   is the record `id` (`user:⟨uuid⟩`, `user:42`), compared as `id = type::record('user', $p1)`;
//! - a `ForeignKey<R>` column is a `record<r>` link, compared the same way;
//! - SQL NULL is `NONE` (an absent field).
//!
//! SQL semantics are kept where SurrealQL differs: a comparison never matches a `NONE`
//! field (SurrealQL orders `NONE` below every value, so `age < 5` would), `LIKE` patterns
//! become anchored regular expressions, and `xor()` is a boolean `!=`.

use super::{Condition, ConditionGroup, Connector, Filter, Op};
use crate::{Model, ModelValues, SqlValue, quote_ident_surreal as q, quote_str_surreal};

/// How a column is stored in SurrealDB, resolved once from the model schema.
#[derive(Debug, Clone, Default)]
pub(super) struct SurCol {
    /// The primary key: the record `id`.
    pk: bool,
    /// The table of the record this column holds (its own table for the PK, the target
    /// table for a foreign key). `None` for a plain value.
    record_table: Option<String>,
    nullable: bool,
}

impl SurCol {
    pub(super) fn of<T: Model + ModelValues>(name: &str) -> Self {
        let schema = T::schema();
        let Some(col) = schema.columns.iter().find(|c| c.name == name) else {
            // Unknown column: `check_col` already recorded the error.
            return Self::default();
        };
        if col.primary_key || name == T::pk_column() {
            return Self {
                pk: true,
                record_table: Some(T::table_name().to_string()),
                nullable: false,
            };
        }
        Self {
            pk: false,
            record_table: col.references.as_ref().map(|fk| fk.table.clone()),
            nullable: col.nullable,
        }
    }

    fn expr(&self, column: &str) -> String {
        if self.pk { "id".to_string() } else { q(column) }
    }

    /// A bound key as the record it names, or the bound value itself.
    fn value(&self, placeholder: String) -> String {
        match &self.record_table {
            Some(t) => format!("type::record({}, {placeholder})", quote_str_surreal(t)),
            None => placeholder,
        }
    }
}

/// `col IN (SELECT VALUE sub_col FROM r`: both sides compared as records, or both as keys.
pub(super) fn in_subquery_head<M, R>(col: &str, sub_col: &str) -> String
where
    M: Model + ModelValues,
    R: Model + ModelValues,
{
    let (outer, inner) = (SurCol::of::<M>(col), SurCol::of::<R>(sub_col));
    let inner_expr = inner.expr(sub_col);
    let projected = match (&outer.record_table, &inner.record_table) {
        (None, Some(_)) => format!("record::id({inner_expr})"),
        (Some(t), None) => format!("type::record({}, {inner_expr})", quote_str_surreal(t)),
        _ => inner_expr,
    };
    format!(
        "{} IN (SELECT VALUE {projected} FROM {}",
        outer.expr(col),
        q(R::table_name())
    )
}

/// Projection of plain column names; the PK is read from the record `id`.
pub(super) fn projection<M: Model + ModelValues>(cols: &[&str]) -> String {
    cols.iter()
        .map(|c| match *c {
            c if c == M::pk_column() && c == "id" => "id".to_string(),
            c if c == M::pk_column() => format!("id AS {}", q(c)),
            c => q(c),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// SQL `LIKE` pattern (`%`, `_`, `\` escape) as an anchored regular expression.
pub(super) fn like_to_regex(pattern: &str, case_insensitive: bool) -> String {
    let mut out = String::from(if case_insensitive { "(?is)^" } else { "(?s)^" });
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            '\\' => push_literal(&mut out, chars.next().unwrap_or('\\')),
            c => push_literal(&mut out, c),
        }
    }
    out.push('$');
    out
}

fn push_literal(out: &mut String, c: char) {
    if "\\.+*?()|[]{}^$#&-~".contains(c) {
        out.push('\\');
    }
    out.push(c);
}

impl<M: Model + ModelValues> Filter<M> {
    /// Conditions folded left to right. `AND` binds tighter than `OR` as in SQL; `xor()`
    /// compares everything before it with the next condition.
    pub(super) fn surreal_conditions(
        &self,
        groups: &[ConditionGroup],
        binds: &mut Vec<SqlValue>,
        idx: &mut usize,
    ) -> String {
        let mut acc = String::new();
        for (i, cg) in groups.iter().enumerate() {
            let expr = match &cg.node {
                super::ConditionNode::Single(c) => self.surreal_condition(c, binds, idx),
                super::ConditionNode::Group(inner) => {
                    format!("({})", self.surreal_conditions(inner, binds, idx))
                }
                super::ConditionNode::Subquery(sub) => self.subquery_sql(sub, binds, idx),
            };
            acc = if i == 0 {
                match cg.connector {
                    Connector::AndNot | Connector::OrNot => format!("NOT ({expr})"),
                    _ => expr,
                }
            } else {
                match cg.connector {
                    Connector::And => format!("{acc} AND {expr}"),
                    Connector::Or => format!("{acc} OR {expr}"),
                    Connector::AndNot => format!("{acc} AND NOT ({expr})"),
                    Connector::OrNot => format!("{acc} OR NOT ({expr})"),
                    Connector::Xor => format!("(({acc}) != ({expr}))"),
                }
            };
        }
        acc
    }

    fn surreal_condition(
        &self,
        c: &Condition,
        binds: &mut Vec<SqlValue>,
        idx: &mut usize,
    ) -> String {
        let col = c.meta.expr(&c.column);
        // SQL: a comparison with NULL is never true. SurrealQL would match NONE fields.
        let guard = |expr: String| {
            if c.meta.nullable {
                format!("({col} != NONE AND {expr})")
            } else {
                expr
            }
        };
        match &c.op {
            Op::IsNull => format!("{col} = NONE"),
            Op::IsNotNull => format!("{col} != NONE"),
            Op::In => {
                let vals: Vec<&SqlValue> =
                    c.values.iter().flatten().filter(|v| !v.is_null()).collect();
                if vals.is_empty() {
                    return "false".to_string();
                }
                let items: Vec<String> = vals
                    .into_iter()
                    .map(|v| {
                        binds.push(v.clone());
                        c.meta.value(self.dialect.placeholder(idx))
                    })
                    .collect();
                format!("{col} IN [{}]", items.join(", "))
            }
            Op::Like | Op::ILike => {
                let pattern = match &c.value {
                    Some(SqlValue::Text(p)) => p.as_str(),
                    _ => "",
                };
                binds.push(SqlValue::Text(like_to_regex(
                    pattern,
                    matches!(c.op, Op::ILike),
                )));
                let p = self.dialect.placeholder(idx);
                guard(format!("string::matches({col}, {p})"))
            }
            op => {
                let value = c.value.clone().unwrap_or(SqlValue::NullText);
                if value.is_null() {
                    return "false".to_string();
                }
                binds.push(value);
                let v = c.meta.value(self.dialect.placeholder(idx));
                let op_str = match op {
                    Op::Eq => "=",
                    Op::Ne => "!=",
                    Op::Gt => ">",
                    Op::Gte => ">=",
                    Op::Lt => "<",
                    Op::Lte => "<=",
                    _ => unreachable!(),
                };
                let expr = format!("{col} {op_str} {v}");
                if matches!(op, Op::Eq) {
                    expr // a bound value never equals NONE
                } else {
                    guard(expr)
                }
            }
        }
    }

    /// `SELECT <cols> FROM t WHERE .. ORDER BY .. LIMIT .. START ..`, plus the ORDER BY clause.
    pub(super) fn surreal_select(&self, cols: &str) -> (String, String, Vec<SqlValue>) {
        let (where_clause, binds) = self.build_where();
        let mut sql = format!("SELECT {cols} FROM {}", q(M::table_name()));
        if !where_clause.is_empty() {
            sql.push(' ');
            sql.push_str(&where_clause);
        }
        let clauses = self.order_clauses();
        let order_by = if clauses.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", clauses.join(", "))
        };
        sql.push_str(&order_by);
        if !self.explicit_limit {
            sql.push_str(&format!(" LIMIT {}", super::DEFAULT_QUERY_LIMIT));
        } else if let Some(l) = self.limit {
            sql.push_str(&format!(" LIMIT {l}"));
        }
        if let Some(o) = self.offset {
            sql.push_str(&format!(" START {o}"));
        }
        (sql, order_by, binds)
    }

    /// No JOIN: each `select_related` target is fetched through its record link into a
    /// nested `__r{i}` object of the same row.
    pub(super) fn surreal_select_related(&self, cols: &str) -> (String, Vec<SqlValue>) {
        let mut projection = cols.to_string();
        for (i, spec) in self.select_related.iter().enumerate() {
            projection.push_str(&format!(", {}.* AS __r{i}", q(&spec.fk_col)));
        }
        let (sql, _, binds) = self.surreal_select(&projection);
        (sql, binds)
    }

    /// The filters as a condition, `true` when there are none.
    fn surreal_where_body(&self, binds: &mut Vec<SqlValue>) -> String {
        self.where_body(&self.conditions, &self.raw_conditions, binds, &mut 1)
    }

    /// `SELECT count() AS n FROM t WHERE .. GROUP ALL` (a single `{n}` row, or none).
    pub(super) fn surreal_count(&self) -> (String, Vec<SqlValue>) {
        let (where_clause, binds) = self.build_where();
        let mut sql = format!("SELECT count() AS n FROM {}", q(M::table_name()));
        if !where_clause.is_empty() {
            sql.push(' ');
            sql.push_str(&where_clause);
        }
        sql.push_str(" GROUP ALL");
        (sql, binds)
    }

    /// `RETURN count(SELECT 1 FROM t WHERE .. LIMIT 1) > 0`: stops at the first match.
    pub(super) fn surreal_exists(&self) -> (String, Vec<SqlValue>) {
        let (where_clause, binds) = self.build_where();
        let mut inner = format!("SELECT 1 FROM {}", q(M::table_name()));
        if !where_clause.is_empty() {
            inner.push(' ');
            inner.push_str(&where_clause);
        }
        (format!("RETURN count({inner} LIMIT 1) > 0"), binds)
    }

    pub(super) fn surreal_aggregate(&self, func: &str, col: &str) -> (String, Vec<SqlValue>) {
        let c = SurCol::of::<M>(col).expr(col);
        let mut binds = Vec::new();
        let body = self.surreal_where_body(&mut binds);
        let cond = if body.is_empty() {
            format!("{c} != NONE")
        } else {
            format!("({body}) AND {c} != NONE")
        };
        let table = q(M::table_name());
        let sql = match func {
            "SUM" | "AVG" => {
                let f = if func == "SUM" {
                    "math::sum"
                } else {
                    "math::mean"
                };
                format!("SELECT count() AS n, {f}({c}) AS v FROM {table} WHERE {cond} GROUP ALL")
            }
            _ => {
                let dir = if func == "MAX" { "DESC" } else { "ASC" };
                format!("SELECT VALUE {c} FROM {table} WHERE {cond} ORDER BY {c} {dir} LIMIT 1")
            }
        };
        (sql, binds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Dialect;
    use crate::{
        ColumnDef, ColumnType, FkReference, FromRow, RangoRow, ReferentialAction, RowError,
        TableSchema,
    };

    struct Post;
    impl crate::ModelHooks for Post {}
    impl Model for Post {
        type Pk = uuid::Uuid;
        fn table_name() -> &'static str {
            "post"
        }
        fn schema() -> TableSchema {
            let col = |name: &str, nullable, references: Option<&str>| ColumnDef {
                name: name.to_string(),
                col_type: ColumnType::Text,
                nullable,
                primary_key: name == "id",
                unique: false,
                default: None,
                references: references.map(|t| FkReference {
                    table: t.to_string(),
                    column: "id".to_string(),
                    on_delete: ReferentialAction::Cascade,
                    on_update: ReferentialAction::NoAction,
                }),
                index: false,
            };
            TableSchema {
                table_name: "post".into(),
                columns: vec![
                    col("id", false, None),
                    col("author_id", false, Some("user")),
                    col("title", false, None),
                    col("score", true, None),
                ],
                ..TableSchema::default()
            }
        }
    }
    impl ModelValues for Post {
        fn field_values(&self) -> Vec<(&'static str, SqlValue)> {
            Vec::new()
        }
        fn pk_value(&self) -> SqlValue {
            SqlValue::NullUuid
        }
        fn pk_column() -> &'static str {
            "id"
        }
    }
    impl FromRow for Post {
        fn from_row(_: &dyn RangoRow) -> Result<Self, RowError> {
            Ok(Post)
        }
    }

    fn f() -> Filter<Post> {
        Filter::new(Dialect::Surreal)
    }

    #[test]
    fn keys_compare_as_records_and_nullable_columns_skip_none() {
        let id = uuid::Uuid::nil();
        let (sql, binds) = f()
            .eq("id", id)
            .eq("author_id", id)
            .lt("score", 5i32)
            .order_by("-id")
            .limit(10)
            .offset(20)
            .select_sql();
        assert_eq!(
            sql,
            "SELECT * FROM `post` WHERE id = type::record('post', $p1) \
             AND `author_id` = type::record('user', $p2) \
             AND (`score` != NONE AND `score` < $p3) ORDER BY id DESC LIMIT 10 START 20"
        );
        assert_eq!(binds.len(), 3);
    }

    #[test]
    fn connectors_subqueries_and_like() {
        let (sql, binds) = f()
            .eq("title", "a")
            .xor()
            .like("title", "50\\%_x%".to_string())
            .not()
            .where_exists::<Post>("author_id", |q| q.eq("title", "b"))
            .in_subquery::<Post>("author_id", "id", |q| q)
            .in_values("title", Vec::new())
            .build_where();
        assert_eq!(
            sql,
            "WHERE ((`title` = $p1) != (string::matches(`title`, $p2))) \
             AND NOT (count(SELECT 1 FROM `post` WHERE `author_id` = $parent.id AND (`title` = $p3) LIMIT 1) > 0) \
             AND `author_id` IN (SELECT VALUE id FROM `post`) AND false"
        );
        assert_eq!(binds[1], SqlValue::Text("(?s)^50%.x.*$".to_string()));
    }

    #[test]
    fn like_patterns_escape_regex_syntax() {
        assert_eq!(like_to_regex("a.b(c)%", true), "(?is)^a\\.b\\(c\\).*$");
        assert_eq!(like_to_regex("\\_x", false), "(?s)^_x$");
    }

    #[test]
    fn aggregates_count_values_and_ignore_none() {
        let (sql, _) = f().gt("title", "a").aggregate_sql("SUM", "score");
        assert_eq!(
            sql,
            "SELECT count() AS n, math::sum(`score`) AS v FROM `post` \
             WHERE (`title` > $p1) AND `score` != NONE GROUP ALL"
        );
        let (sql, _) = f().aggregate_sql("MAX", "title");
        assert_eq!(
            sql,
            "SELECT VALUE `title` FROM `post` WHERE `title` != NONE ORDER BY `title` DESC LIMIT 1"
        );
        assert_eq!(
            f().count_sql().0,
            "SELECT count() AS n FROM `post` GROUP ALL"
        );
        assert_eq!(
            f().eq("title", "x").exists_sql().0,
            "RETURN count(SELECT 1 FROM `post` WHERE `title` = $p1 LIMIT 1) > 0"
        );
        assert_eq!(
            f().values_sql(&["id", "title"]).0,
            "SELECT id, `title` FROM `post` LIMIT 1000"
        );
    }

    #[test]
    fn select_related_nests_the_target() {
        let (sql, _) = f().select_related::<Post>("author_id").select_sql();
        assert_eq!(
            sql,
            "SELECT *, `author_id`.* AS __r0 FROM `post` LIMIT 1000"
        );
    }
}

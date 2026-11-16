//! SurrealDB 4: `rango migrate`, `export` / `dump` and `import` / `restore`.
//!
//! Dumps are Rango JSON like on the SQL backends: the primary key comes out as a column
//! (the record key), record links as the key they point at, bytes in base64, decimals and
//! dates as text. The catalog (`INFO FOR DB` / `INFO FOR TABLE`) and the table comments
//! written by `rango makemigrations` (`rango:pk=id:uuid`) say how to read them back.
//! Files are untrusted on import: names are quoted, values bound, unknown tables and
//! columns refused (SurrealDB would create the table, or drop the field, silently).

use crate::commands::export::Rows;
use crate::commands::import::{
    OnConflict, conflict_for, decode_binary, encode_binary, group_by_table, insert_columns,
    order_by_dependencies,
};
use crate::commands::migrate::pending_migrations;
use anyhow::{Context, Result, bail};
use rango_orm_core::quote_ident_surreal as q;
use rango_orm_core::surreal_schema::{TRACKING_TABLE_DDL, parse_table_comment};
use rango_orm_surrealdb::surrealdb::types::{
    Array, Bytes, Datetime, Duration, Number, Object, RecordId, RecordIdKey, SurrealValue, Uuid,
    Value,
};
use rango_orm_surrealdb::{SurrealPool, SurrealTransaction};
use std::collections::{BTreeMap, HashSet};

const LOCK_DDL: &str =
    "DEFINE TABLE IF NOT EXISTS _rango_lock TYPE NORMAL SCHEMALESS COMMENT 'rango:internal';";

// ─── migrate ──────────────────────────────────────────────────────────────────

/// Apply every pending `.surql` migration, each in its own transaction with its ledger row.
/// A lock record keeps two `rango migrate` from running at once.
pub(crate) async fn apply_migrations(pool: &SurrealPool, migrations_dir: &str) -> Result<()> {
    pool.query(TRACKING_TABLE_DDL)
        .query(LOCK_DDL)
        .await
        .and_then(|r| r.check())
        .context("Failed to create _rango_migrations")?;
    pool.query("CREATE _rango_lock:migrate CONTENT { at: time::now() } RETURN NONE")
        .await
        .and_then(|r| r.check())
        .context(
            "Failed to acquire the migration lock: another `rango migrate` is running \
             (if one crashed, remove the lock with `DELETE _rango_lock:migrate`)",
        )?;
    let result = apply_pending(pool, migrations_dir).await;
    let unlock = pool
        .query("DELETE _rango_lock:migrate")
        .await
        .and_then(|r| r.check());
    match (result, unlock) {
        (Err(e), _) => Err(e),
        (Ok(()), Err(e)) => Err(e).context("Failed to release the migration lock"),
        (Ok(()), Ok(_)) => Ok(()),
    }
}

async fn apply_pending(pool: &SurrealPool, migrations_dir: &str) -> Result<()> {
    let applied: Vec<String> = pool
        .query("SELECT VALUE name FROM _rango_migrations")
        .await
        .and_then(|mut r| r.take(0))
        .context("Failed to fetch applied migrations")?;
    let applied: HashSet<String> = applied.into_iter().collect();
    let pending = pending_migrations(migrations_dir, &applied, "surql")?;
    if pending.is_empty() {
        println!("✅ Nothing to apply — database is up to date.");
        return Ok(());
    }
    let count = pending.len();
    for (name, sql) in pending {
        println!("  ▶  Applying {}...", name);
        let tx = pool.begin().await?;
        let result = async {
            tx.sdk().query(&sql).await?.check()?;
            tx.sdk()
                .query("CREATE _rango_migrations CONTENT { name: $name } RETURN NONE")
                .bind(("name", name.clone()))
                .await?
                .check()?;
            Ok::<_, rango_orm_surrealdb::surrealdb::Error>(())
        }
        .await;
        match result {
            Ok(()) => tx
                .commit()
                .await
                .with_context(|| format!("Failed to commit migration {name}"))?,
            Err(e) => {
                tx.cancel().await.ok();
                return Err(e).with_context(|| format!("Failed to apply {name}"));
            }
        }
        println!("  ✅ {}", name);
    }
    println!("✅ Applied {} migration(s).", count);
    Ok(())
}

// ─── Catalog ──────────────────────────────────────────────────────────────────

/// How a field is stored, from its catalog `TYPE`.
#[derive(Debug, Clone, PartialEq)]
enum Kind {
    Record(String),
    Uuid,
    Datetime,
    Date,
    Time,
    Decimal,
    Bytes,
    Int,
    Float,
    Bool,
    Text,
    Json,
}

struct TableMeta {
    /// `(pk column, key kind)`; `None` for a table without a PK (pivot, ledger).
    pk: Option<(String, String)>,
    /// Field name -> kind, sorted.
    fields: BTreeMap<String, Kind>,
}

struct Catalog {
    tables: BTreeMap<String, TableMeta>,
    sequences: HashSet<String>,
}

impl Catalog {
    async fn load(tx: &SurrealTransaction) -> Result<Self> {
        let info: Value = tx
            .sdk()
            .query("INFO FOR DB")
            .await
            .and_then(|mut r| r.take(0))
            .context("Failed to read the catalog")?;
        let sequences = defs(&info, "sequences").into_keys().collect();
        let mut tables = BTreeMap::new();
        for (name, def) in defs(&info, "tables") {
            if name == "_rango_lock" {
                continue;
            }
            let pk = match between(&def, "COMMENT '", "'").and_then(parse_table_comment) {
                Some(pk) => pk,
                // Not defined by Rango: the record key is `id`.
                None => Some(("id".to_string(), "text".to_string())),
            };
            let info: Value = tx
                .sdk()
                .query(format!("INFO FOR TABLE {}", q(&name)))
                .await
                .and_then(|mut r| r.take(0))
                .with_context(|| format!("Failed to read the fields of {name}"))?;
            let fields = defs(&info, "fields")
                .into_iter()
                .filter(|(f, _)| !f.contains('.') && !f.contains('['))
                .map(|(f, d)| (f, kind_of(&d)))
                .collect();
            tables.insert(name, TableMeta { pk, fields });
        }
        Ok(Self { tables, sequences })
    }

    /// `(child, parent)` pairs of record links between tables.
    fn edges(&self) -> Vec<(String, String)> {
        let mut edges = Vec::new();
        for (t, meta) in &self.tables {
            for kind in meta.fields.values() {
                if let Kind::Record(target) = kind {
                    edges.push((t.clone(), target.clone()));
                }
            }
        }
        edges
    }

    fn key_kind(&self, table: &str) -> &str {
        self.tables
            .get(table)
            .and_then(|m| m.pk.as_ref())
            .map_or("text", |(_, k)| k.as_str())
    }
}

/// The `name -> definition` map at `info[section]`.
fn defs(info: &Value, section: &str) -> BTreeMap<String, String> {
    let Value::Object(info) = info else {
        return BTreeMap::new();
    };
    match info.get(section) {
        Some(Value::Object(o)) => o
            .iter()
            .filter_map(|(k, v)| match v {
                Value::String(s) => Some((k.clone(), s.clone())),
                _ => None,
            })
            .collect(),
        _ => BTreeMap::new(),
    }
}

fn between<'a>(s: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let from = s.find(start)? + start.len();
    Some(&s[from..from + s[from..].find(end)?])
}

/// The kind of a `DEFINE FIELD ... TYPE <type> ...` definition.
fn kind_of(def: &str) -> Kind {
    const CLAUSES: [&str; 9] = [
        " FLEXIBLE",
        " ASSERT ",
        " REFERENCE",
        " DEFAULT ",
        " PERMISSIONS",
        " COMMENT ",
        " READONLY",
        " VALUE ",
        " COMPUTED ",
    ];
    // Dates and times are stored as datetime / duration, marked by a comment.
    if def.contains("COMMENT 'rango:date'") {
        return Kind::Date;
    }
    if def.contains("COMMENT 'rango:time'") {
        return Kind::Time;
    }
    let Some(start) = def.find(" TYPE ").map(|i| i + 6) else {
        return Kind::Json;
    };
    let rest = &def[start..];
    let end = CLAUSES
        .iter()
        .filter_map(|c| rest.find(c))
        .min()
        .unwrap_or(rest.len());
    let ty = rest[..end].trim();
    let ty = ty
        .strip_prefix("option<")
        .and_then(|t| t.strip_suffix('>'))
        .unwrap_or(ty);
    let parts: Vec<&str> = ty
        .split('|')
        .map(str::trim)
        .filter(|p| *p != "none" && *p != "null")
        .collect();
    if parts.iter().any(|p| *p == "object" || *p == "array") || parts.len() != 1 {
        return Kind::Json;
    }
    let p = parts[0];
    if let Some(target) = p.strip_prefix("record<").and_then(|t| t.strip_suffix('>')) {
        return Kind::Record(unquote(target));
    }
    match p {
        "uuid" => Kind::Uuid,
        "datetime" => Kind::Datetime,
        "date" => Kind::Date,
        "time" => Kind::Time,
        "decimal" => Kind::Decimal,
        "bytes" => Kind::Bytes,
        "int" => Kind::Int,
        "float" | "number" => Kind::Float,
        "bool" => Kind::Bool,
        "string" => Kind::Text,
        _ => Kind::Json,
    }
}

/// `` `a\`b` `` -> ``a`b``.
fn unquote(ident: &str) -> String {
    match ident.strip_prefix('`').and_then(|i| i.strip_suffix('`')) {
        Some(inner) => {
            let mut out = String::new();
            let mut chars = inner.chars();
            while let Some(c) = chars.next() {
                out.push(if c == '\\' {
                    chars.next().unwrap_or('\\')
                } else {
                    c
                });
            }
            out
        }
        None => ident.to_string(),
    }
}

// ─── export ───────────────────────────────────────────────────────────────────

/// Rows of `requested` tables (all when empty), parents first, from one snapshot.
pub(crate) async fn export_tables(
    pool: &SurrealPool,
    requested: &[String],
) -> Result<Vec<(String, Rows)>> {
    let tx = pool.begin().await?;
    let result = export_in(&tx, requested).await;
    tx.cancel().await.ok();
    result
}

async fn export_in(tx: &SurrealTransaction, requested: &[String]) -> Result<Vec<(String, Rows)>> {
    let catalog = Catalog::load(tx).await?;
    let names: Vec<String> = catalog.tables.keys().cloned().collect();
    let tables = crate::commands::export::select_tables(names, requested, &catalog.edges())?;
    let mut out = Vec::new();
    for t in tables {
        let meta = &catalog.tables[&t];
        let records: Vec<Value> = tx
            .sdk()
            .query(format!("SELECT * FROM {}", q(&t)))
            .await
            .and_then(|mut r| r.take(0))
            .with_context(|| format!("Failed to export table {t}"))?;
        let mut rows = Vec::with_capacity(records.len());
        for record in records {
            let Value::Object(obj) = record else {
                bail!("Unexpected row shape in table {t}");
            };
            let mut map = serde_json::Map::new();
            if let Some((pk, _)) = &meta.pk {
                map.insert(
                    pk.clone(),
                    to_json(obj.get("id").unwrap_or(&Value::None), None),
                );
            }
            for (field, kind) in &meta.fields {
                let v = obj.get(field).unwrap_or(&Value::None);
                map.insert(field.clone(), to_json(v, Some(kind)));
            }
            rows.push(map);
        }
        out.push((t, rows));
    }
    Ok(out)
}

fn to_json(v: &Value, kind: Option<&Kind>) -> serde_json::Value {
    use serde_json::Value as J;
    match (v, kind) {
        (Value::Datetime(d), Some(Kind::Date)) => {
            return J::String((*d).into_inner().date_naive().to_string());
        }
        (Value::Duration(d), Some(Kind::Time)) => {
            let d = (*d).into_inner();
            return chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                d.as_secs() as u32,
                d.subsec_nanos(),
            )
            .map_or(J::Null, |t| J::String(t.to_string()));
        }
        _ => {}
    }
    match v {
        Value::None | Value::Null => J::Null,
        Value::Bool(b) => J::Bool(*b),
        // Decimals as text, so the JSON keeps every digit.
        Value::Number(Number::Decimal(d)) => J::String(d.to_string()),
        Value::Number(Number::BigDecimal(d)) => J::String(d.to_string()),
        Value::Number(n) => match n.to_int() {
            Some(i) => J::from(i),
            None => n.to_f64().map_or(J::Null, J::from),
        },
        Value::String(s) => J::String(s.clone()),
        Value::Uuid(u) => J::String((*u).into_inner().to_string()),
        Value::Datetime(d) => J::String(
            (*d).into_inner()
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        ),
        Value::Date(d) => J::String((*d).into_inner().to_string()),
        Value::Time(t) => (*t)
            .into_inner()
            .map_or(J::Null, |t| J::String(t.to_string())),
        Value::Bytes(b) => J::String(encode_binary(&b.clone().into_inner())),
        Value::RecordId(r) => match &r.key {
            RecordIdKey::Number(i) => J::from(*i),
            RecordIdKey::String(s) => J::String(s.clone()),
            RecordIdKey::Uuid(u) => J::String((*u).into_inner().to_string()),
            other => serde_json::to_value(other).unwrap_or(J::Null),
        },
        other => other.clone().into_json_value(),
    }
}

// ─── import ───────────────────────────────────────────────────────────────────

/// Insert `rows` in one transaction, after `setup_sql` (the schema, for `restore`).
pub(crate) async fn import_rows(
    pool: &SurrealPool,
    rows: &[serde_json::Map<String, serde_json::Value>],
    table: Option<&str>,
    on_conflict: OnConflict,
    setup_sql: Option<&str>,
) -> Result<usize> {
    let tx = pool.begin().await?;
    match import_in(&tx, rows, table, on_conflict, setup_sql).await {
        Ok(n) => {
            tx.commit().await.context("Failed to commit import")?;
            Ok(n)
        }
        Err(e) => {
            tx.cancel().await.ok();
            Err(e)
        }
    }
}

async fn import_in(
    tx: &SurrealTransaction,
    rows: &[serde_json::Map<String, serde_json::Value>],
    table: Option<&str>,
    on_conflict: OnConflict,
    setup_sql: Option<&str>,
) -> Result<usize> {
    if let Some(sql) = setup_sql {
        tx.sdk()
            .query(sql)
            .await
            .and_then(|r| r.check())
            .context("Failed to create the schema")?;
    }
    let catalog = Catalog::load(tx).await?;
    let groups = group_by_table(rows, table);
    for t in groups.keys() {
        if !catalog.tables.contains_key(t) {
            bail!("Unknown table '{t}': run `rango migrate` first");
        }
    }
    // Record links are checked on write: parents first.
    let order = order_by_dependencies(groups.keys().cloned().collect(), &catalog.edges());
    let mut total = 0;
    for t in order {
        let table_rows = &groups[&t];
        total += insert_table(tx, &catalog, &t, table_rows, conflict_for(&t, on_conflict)).await?;
        resync_sequence(tx, &catalog, &t).await?;
    }
    Ok(total)
}

async fn insert_table(
    tx: &SurrealTransaction,
    catalog: &Catalog,
    table: &str,
    rows: &[&serde_json::Map<String, serde_json::Value>],
    on_conflict: OnConflict,
) -> Result<usize> {
    let Some(first) = rows.first() else {
        return Ok(0);
    };
    let meta = &catalog.tables[table];
    let pk = meta.pk.as_ref().map(|(c, _)| c.as_str());
    for col in insert_columns(first) {
        if Some(col) != pk && !meta.fields.contains_key(col) {
            bail!("Column '{col}' does not exist in table '{table}'");
        }
    }
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let mut obj = Object::new();
        for (col, j) in row.iter().filter(|(c, _)| *c != "__table") {
            let value = if Some(col.as_str()) == pk {
                record(table, catalog.key_kind(table), j)
                    .with_context(|| format!("{table}.{col}: invalid key"))?
            } else {
                to_value(catalog, col, &meta.fields[col], j)
                    .with_context(|| format!("{table}.{col}: invalid value"))?
            };
            if !matches!(value, Value::None) {
                obj.insert(
                    if Some(col.as_str()) == pk {
                        "id"
                    } else {
                        col.as_str()
                    },
                    value,
                );
            }
        }
        records.push(Value::Object(obj));
    }
    let t = q(table);
    let sql = match on_conflict {
        OnConflict::Fail => format!("INSERT INTO {t} $rows RETURN id"),
        OnConflict::Skip => format!("INSERT IGNORE INTO {t} $rows RETURN id"),
        OnConflict::Replace if pk.is_some() => {
            "FOR $r IN $rows { UPSERT $r.id CONTENT $r RETURN NONE; }; RETURN $rows;".to_string()
        }
        OnConflict::Replace => {
            let set: Vec<String> = meta
                .fields
                .keys()
                .map(|f| format!("{0} = $input.{0}", q(f)))
                .collect();
            format!(
                "INSERT INTO {t} $rows ON DUPLICATE KEY UPDATE {} RETURN id",
                set.join(", ")
            )
        }
    };
    let mut res = tx
        .sdk()
        .query(sql)
        .bind(("rows", Value::Array(Array::from(records))))
        .await
        .and_then(|r| r.check())
        .with_context(|| format!("Failed to insert into {table}"))?;
    let n = res.num_statements();
    let returned: Vec<Value> = res
        .take(n - 1)
        .with_context(|| format!("Failed to insert into {table}"))?;
    Ok(returned.len())
}

/// After explicit ids were imported, the next auto-increment id follows the largest one.
async fn resync_sequence(tx: &SurrealTransaction, catalog: &Catalog, table: &str) -> Result<()> {
    let Some((pk, kind)) = catalog.tables[table].pk.as_ref() else {
        return Ok(());
    };
    let seq = rango_orm_core::surreal_sequence_name(table, pk);
    if !matches!(kind.as_str(), "int" | "bigint") || !catalog.sequences.contains(&seq) {
        return Ok(());
    }
    let max: Option<i64> = tx
        .sdk()
        .query(format!(
            "SELECT VALUE record::id(id) FROM {} ORDER BY id DESC LIMIT 1",
            q(table)
        ))
        .await
        .and_then(|mut r| r.take::<Vec<i64>>(0))
        .with_context(|| format!("Failed to read the largest id of {table}"))?
        .into_iter()
        .next();
    if let Some(max) = max {
        tx.sdk()
            .query(format!(
                "DEFINE SEQUENCE OVERWRITE {} START {}",
                q(&seq),
                max + 1
            ))
            .await
            .and_then(|r| r.check())
            .with_context(|| format!("Failed to move sequence {seq}"))?;
    }
    Ok(())
}

fn record(table: &str, key_kind: &str, j: &serde_json::Value) -> Result<Value> {
    use serde_json::Value as J;
    let key = match (key_kind, j) {
        (_, J::Null) => return Ok(Value::None),
        ("uuid", J::String(s)) => RecordIdKey::Uuid(Uuid::from(s.parse::<uuid::Uuid>()?)),
        ("int" | "bigint", J::Number(n)) => {
            RecordIdKey::Number(n.as_i64().context("not an integer")?)
        }
        ("int" | "bigint", J::String(s)) => RecordIdKey::Number(s.parse()?),
        (_, J::String(s)) => RecordIdKey::String(s.clone()),
        (_, J::Number(n)) => RecordIdKey::Number(n.as_i64().context("not an integer")?),
        _ => bail!("expected a key"),
    };
    Ok(Value::RecordId(RecordId::new(table, key)))
}

fn to_value(catalog: &Catalog, col: &str, kind: &Kind, j: &serde_json::Value) -> Result<Value> {
    use serde_json::Value as J;
    if j.is_null() {
        return Ok(Value::None);
    }
    let text = || j.as_str().context("expected a string");
    Ok(match kind {
        Kind::Record(target) => return record(target, catalog.key_kind(target), j),
        Kind::Uuid => Value::Uuid(Uuid::from(text()?.parse::<uuid::Uuid>()?)),
        Kind::Datetime => {
            let s = text()?;
            let dt = chrono::DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&chrono::Utc))
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                        .map(|n| n.and_utc())
                })
                .or_else(|_| {
                    s.parse::<chrono::NaiveDate>()
                        .map(|d| d.and_time(chrono::NaiveTime::MIN).and_utc())
                })?;
            Value::Datetime(Datetime::from(dt))
        }
        // A date is the datetime of its midnight (UTC), a time the duration since midnight.
        Kind::Date => {
            let s = text()?;
            let day = match s.parse::<chrono::NaiveDate>() {
                Ok(d) => d,
                Err(_) => chrono::DateTime::parse_from_rfc3339(s)?.date_naive(),
            };
            Value::Datetime(Datetime::from(
                day.and_time(chrono::NaiveTime::MIN).and_utc(),
            ))
        }
        Kind::Time => {
            use chrono::Timelike;
            let t = text()?.parse::<chrono::NaiveTime>()?;
            Value::Duration(Duration::new(
                u64::from(t.num_seconds_from_midnight()),
                t.nanosecond().min(999_999_999),
            ))
        }
        Kind::Decimal => {
            let s = match j {
                J::Number(n) => n.to_string(),
                _ => text()?.to_string(),
            };
            Value::Number(Number::Decimal(s.parse()?))
        }
        Kind::Bytes => Value::Bytes(Bytes::from(decode_binary(col, text()?)?)),
        Kind::Int => Value::Number(Number::Int(match j {
            J::Number(n) => n.as_i64().context("not an integer")?,
            J::Bool(b) => i64::from(*b),
            _ => text()?.parse()?,
        })),
        Kind::Float => Value::Number(Number::Float(match j {
            J::Number(n) => n.as_f64().context("not a number")?,
            _ => text()?.parse()?,
        })),
        Kind::Bool => Value::Bool(match j {
            J::Bool(b) => *b,
            J::Number(n) => n.as_i64() != Some(0),
            _ => text()?.parse()?,
        }),
        Kind::Text => Value::String(match j {
            J::String(s) => s.clone(),
            other => other.to_string(),
        }),
        Kind::Json => j.clone().into_value(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_catalog_types() {
        let k = |d: &str| kind_of(d);
        assert_eq!(
            k(
                "DEFINE FIELD p ON `o\\`dd` TYPE none | record<s_user> ASSERT $value = NONE REFERENCE ON DELETE UNSET PERMISSIONS FULL"
            ),
            Kind::Record("s_user".into())
        );
        assert_eq!(
            k("DEFINE FIELD p ON t TYPE record<`we\\`ird`> PERMISSIONS FULL"),
            Kind::Record("we`ird".into())
        );
        assert_eq!(
            k("DEFINE FIELD d ON t TYPE none | decimal DEFAULT 0dec PERMISSIONS FULL"),
            Kind::Decimal
        );
        assert_eq!(
            k(
                "DEFINE FIELD m ON t TYPE object | array | string | number | bool | null FLEXIBLE PERMISSIONS FULL"
            ),
            Kind::Json
        );
        assert_eq!(
            k("DEFINE FIELD e ON t TYPE string ASSERT string::len($value) <= 254 PERMISSIONS FULL"),
            Kind::Text
        );
    }
}

/// makemigrations -> migrate -> write -> export -> restore into a fresh engine, in process
/// (an embedded `mem://` datastore lives as long as its client).
#[cfg(all(test, feature = "surrealdb-mem"))]
mod e2e {
    use super::*;
    use crate::migration::{
        diff::{SchemaDiff, diff},
        snapshot::Snapshot,
        sql::{generate_sql_from_diff, generate_surql_from_diff},
    };
    use rango::prelude::{
        FieldAutoInt, FieldText, FieldUuid, ForeignKey, ManyToMany, Model, SqlValue, ToSqlValue,
    };
    use rango_orm_surrealdb as sdb;

    #[derive(Model, Debug, Clone, PartialEq)]
    #[model(table = "e_author")]
    struct Author {
        id: FieldUuid,
        #[field(unique)]
        name: FieldText,
        born: Option<rango::prelude::FieldDate>,
        wakes: rango::prelude::FieldTime,
    }

    #[derive(Model, Debug, Clone)]
    #[model(table = "e_book")]
    struct Book {
        id: FieldAutoInt,
        author_id: ForeignKey<Author>,
        tags: ManyToMany<Tag>,
    }

    #[derive(Model, Debug, Clone, PartialEq)]
    #[model(table = "e_tag")]
    struct Tag {
        id: FieldUuid,
        label: FieldText,
    }

    fn big_stack(f: fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>) {
        std::thread::Builder::new()
            .stack_size(64 << 20)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(f())
            })
            .unwrap()
            .join()
            .unwrap_or_else(|e| std::panic::resume_unwind(e));
    }

    fn schemas() -> Vec<TableSchema> {
        let pivot = crate::migration::scanner::generate_pivot_table(
            &crate::migration::scanner::M2MRelation {
                from_table: "e_book".into(),
                to_table: "e_tag".into(),
            },
            &[Author::schema(), Book::schema(), Tag::schema()],
        );
        vec![Author::schema(), Book::schema(), Tag::schema(), pivot]
    }

    use rango_orm_core::TableSchema;

    #[test]
    fn migrate_export_restore() {
        big_stack(|| Box::pin(run()));
    }

    async fn run() {
        let dir = std::env::temp_dir().join(format!("rango-surreal-e2e-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_str().unwrap();
        let schemas = schemas();
        let diffs = diff(&Snapshot::default(), &schemas);
        std::fs::write(
            dir.join("0001_initial.surql"),
            generate_surql_from_diff(&diffs, &schemas),
        )
        .unwrap();
        // Not a SurrealDB migration: ignored.
        std::fs::write(dir.join("0002_other.sql"), "CREATE TABLE x (id INT);").unwrap();

        let db = sdb::connect_url("surrealdb+mem://").await.unwrap();
        apply_migrations(&db, dir_s).await.unwrap();
        apply_migrations(&db, dir_s).await.unwrap(); // nothing to apply, lock released

        let ann = sdb::insert(
            &db,
            Author {
                id: FieldUuid::new(),
                name: "ann".into(),
                born: Some(chrono::NaiveDate::from_ymd_opt(1990, 5, 17).unwrap().into()),
                wakes: chrono::NaiveTime::from_hms_micro_opt(6, 30, 0, 250)
                    .unwrap()
                    .into(),
            },
        )
        .await
        .unwrap();
        let tag = sdb::insert(
            &db,
            Tag {
                id: FieldUuid::new(),
                label: "sf".into(),
            },
        )
        .await
        .unwrap();
        let book = |a: &Author| Book {
            id: FieldAutoInt(None),
            author_id: ForeignKey::new(a.id.0),
            tags: ManyToMany::empty(),
        };
        let b1 = sdb::insert(&db, book(&ann)).await.unwrap();
        sdb::insert(&db, book(&ann)).await.unwrap();
        sdb::M2M::<Book, Tag>::add(&db, &b1, &tag).await.unwrap();

        let exported = export_tables(&db, &[]).await.unwrap();
        let names: Vec<&str> = exported.iter().map(|(t, _)| t.as_str()).collect();
        assert!(
            names.iter().position(|t| *t == "e_author") < names.iter().position(|t| *t == "e_book"),
            "{names:?}"
        );
        let books = &exported.iter().find(|(t, _)| t == "e_book").unwrap().1;
        assert_eq!(books[0]["id"], serde_json::json!(1));
        assert_eq!(
            books[0]["author_id"],
            serde_json::json!(ann.id.0.to_string())
        );
        let authors = &exported.iter().find(|(t, _)| t == "e_author").unwrap().1;
        assert_eq!(authors[0]["born"], serde_json::json!("1990-05-17"));
        assert_eq!(authors[0]["wakes"], serde_json::json!("06:30:00.000250"));
        let ledger = &exported
            .iter()
            .find(|(t, _)| t == "_rango_migrations")
            .unwrap()
            .1;
        assert_eq!(ledger[0]["name"], serde_json::json!("0001_initial.surql"));
        let pivot = &exported
            .iter()
            .find(|(t, _)| t == "e_book_e_tag")
            .unwrap()
            .1;
        assert!(!pivot[0].contains_key("id"), "a pivot has no PK column");

        // restore: schema from the snapshot, then the rows (ledger included), in one transaction.
        let creates: Vec<SchemaDiff> = schemas
            .iter()
            .cloned()
            .map(SchemaDiff::CreateTable)
            .collect();
        let mut setup = generate_sql_from_diff(&creates, rango_orm_core::BackendKind::Surreal);
        setup.push_str(TRACKING_TABLE_DDL);
        let mut rows = Vec::new();
        for (t, table_rows) in &exported {
            for r in table_rows {
                let mut r = r.clone();
                r.insert("__table".into(), t.clone().into());
                rows.push(r);
            }
        }
        let fresh = sdb::connect_url("surrealdb+mem://").await.unwrap();
        let n = import_rows(&fresh, &rows, None, OnConflict::Fail, Some(&setup))
            .await
            .unwrap();
        assert_eq!(n, rows.len());
        assert_eq!(sdb::all::<Book>(&fresh).await.unwrap().len(), 2);
        assert_eq!(
            sdb::M2M::<Book, Tag>::all(&fresh, &b1).await.unwrap()[0].id,
            tag.id
        );
        let next = sdb::insert(&fresh, book(&ann)).await.unwrap();
        assert_eq!(next.id.0, Some(3), "the sequence follows the restored ids");
        let got = sdb::get::<Author>(&fresh, &ann.id.to_sql_value())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, ann);

        // import: skip existing, replace, refuse what the schema does not know.
        assert_eq!(
            import_rows(&fresh, &rows, None, OnConflict::Skip, None)
                .await
                .unwrap(),
            0
        );
        let mut renamed = exported[0].1[0].clone();
        if exported[0].0 == "e_author" {
            renamed.insert("name".into(), "bob".into());
            assert_eq!(
                import_rows(
                    &fresh,
                    &[renamed],
                    Some("e_author"),
                    OnConflict::Replace,
                    None
                )
                .await
                .unwrap(),
                1
            );
            let got = sdb::get::<Author>(&fresh, &SqlValue::Uuid(ann.id.0))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(got.name.0, "bob");
        }
        let mut bad = serde_json::Map::new();
        bad.insert("nope".into(), 1.into());
        let err = import_rows(
            &fresh,
            &[bad.clone()],
            Some("e_author"),
            OnConflict::Fail,
            None,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err:#}");
        let err = import_rows(&fresh, &[bad], Some("ghost"), OnConflict::Fail, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Unknown table"), "{err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

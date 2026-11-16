# SurrealDB

Rango supports **SurrealDB 4** through the official Rust SDK, over WebSocket, HTTP, or the
embedded in-memory engine. Models, the typed query builder, relations, bulk operations,
`rango makemigrations` / `migrate` and `dump` / `restore` work as on the SQL backends;
migrations are SurrealQL `DEFINE` statements instead of SQL.

> **License.** Rango is MIT OR Apache-2.0, but the SurrealDB 4 SDK it links is
> **BUSL-1.1**: its additional use grant lets you use it, except to offer it as a
> database service. That is why the backend is opt-in: no default build of `rango-orm`
> or of the `rango` CLI contains it. Read the SurrealDB license before you ship a binary
> built with it.

## Setup

```toml
# Cargo.toml
[dependencies]
# Remote servers (ws / wss / http / https):
rango-orm = { version = "0.2", default-features = false, features = ["surrealdb"] }
# ...plus the embedded in-memory engine (tests, local tools):
rango-orm = { version = "0.2", default-features = false, features = ["surrealdb-mem"] }
```

```toml
# rango.toml
[database]
backend = "surrealdb"
```

```bash
# The CLI links the SDK only when asked:
cargo install rango-orm-cli --features surrealdb
```

## Connecting

```text
surrealdb://user:pass@host:8000/namespace/database   wss for a remote host, ws on loopback
surrealdb+ws://…  surrealdb+wss://…  surrealdb+http://…  surrealdb+https://…
surrealdb+mem://namespace/database                    embedded (feature surrealdb-mem)
```

Namespace and database default to `rango`. Credentials sign in as a **root** user; add
`?auth=namespace` or `?auth=database` to sign in as a namespace or database user, which is
what an application should use (see [Credentials](../security/credentials.md)).

```rust
use rango::prelude::*;

let db = connect_surrealdb("surrealdb://app:secret@db.internal:8000/shop/main?auth=database").await?;

let adults = User::filter(&db).gte(User::AGE, 18).all().await?;
```

`SurrealPool` is a client, not a pool: SurrealDB multiplexes every request over one
connection, so clone it freely. It derefs to the SDK's `Surreal<Any>` for anything Rango
does not wrap. When several backends are enabled, the ops live under `rango::surrealdb::*`.

## Storage model

| Rango | SurrealDB |
|---|---|
| model | `SCHEMAFULL` table, commented `rango:pk=<column>:<kind>` |
| primary key | the record key: `user:⟨uuid⟩`, `book:42` (not a field) |
| `FieldAutoInt` / `FieldAutoBigInt` | key from the sequence `<table>_<pk>_seq` |
| `ForeignKey<User>` | `record<user>` field, `REFERENCE ON DELETE CASCADE / UNSET / REJECT`, plus an `ASSERT` that the record exists |
| `ManyToMany<Tag>` | pivot table `<from>_<to>` of two record links, unique on the pair |
| `Option<T>` / SQL NULL | `option<T>`, NULL is `NONE` (the field is absent) |
| `FieldVarchar<_, N>` | `string` with `ASSERT string::len($value) <= N` |
| `FieldDecimal` | `decimal` (exact) |
| `FieldJson` | `object \| array \| string \| number \| bool \| null`, `FLEXIBLE` |
| `FieldDate` | `datetime` at midnight UTC (comment `rango:date`) |
| `FieldTime` | `duration` since midnight (comment `rango:time`) |
| `#[field(unique)]`, FK index | `DEFINE INDEX ... UNIQUE` / `DEFINE INDEX` |
| `CheckConstraint` | event throwing `rango:check:<name>` on create / update |

Dates and times are stored as `datetime` / `duration` because the SDK writes requests to a
server in plain `cbor`, which carries SurrealDB 4's own `date` / `time` values as exactly
those: the embedded and the remote engines then hold the same data.

Deletes cascade, unset or are rejected **in the database** (`REFERENCE ON DELETE`), and a
write pointing at a missing record fails with `Error::ForeignKeyViolation`, as on Postgres.

## Queries

The query builder renders SurrealQL; values are always bound (`$p1`, `$p2`, …) and
identifiers backtick-quoted. SQL semantics are kept where SurrealQL differs:

- a comparison never matches a `NONE` field (`age < 5` skips users without an age, as SQL
  NULL would), `IS NULL` is `= NONE`;
- `like` / `ilike` patterns (`%`, `_`, `\` escape) become anchored regular expressions;
- `xor()` is a boolean `!=` of everything before it and the next condition;
- `select_related` follows the record link (`author_id.*`), no JOIN; `prefetch_related`
  binds the keys as one array;
- `where_exists` uses `$parent`, `in_subquery` compares records with records.

`.explain()` returns the SurrealQL, `.explain_analyze()` runs it with `EXPLAIN FULL`.
`.not()` differs slightly: `NOT (age > 5)` matches a `NONE` age, where SQL's three-valued
logic would not.

### SurrealDB-specific filters

```rust
use rango::prelude::*; // SurrealQueryExt is included

Post::filter(&db).json_path_eq(Post::METADATA, &["status"], "published").all().await?;
Post::filter(&db).contains(Post::KEYWORDS, "rust").all().await?; // a JSON array column
```

### Raw SurrealQL

`raw`, `raw_scalar` and `raw_execute` take SurrealQL with `$p1`, `$p2`, … parameters and
read the result of the last statement. Raw queries keep SurrealQL semantics (`NONE` sorts
below every number), and `raw_execute` counts the records the last statement returned:
add `RETURN BEFORE` to a `DELETE` to count what it deleted.

## Transactions

`rango::surrealdb::atomic(&db, async |tx| …)` opens an interactive transaction; pass `&*tx`
to any op. `atomic_retry` retries on `Error::Conflict` (SurrealDB transaction conflicts).

**HTTP has no interactive transactions**: each request stands alone, so `atomic` fails with
`Error::Invalid` there. Single operations stay atomic on every transport (one
`BEGIN … COMMIT` request): `get_or_create`, `bulk_update`, `bulk_upsert`, `M2M::set`.
`update_or_create` is a lookup then a write over HTTP (as on SQLite outside `atomic`), and
bulk operations over more than 5 000 records need WebSocket. Use `surrealdb://` unless you
have a reason not to.

## Migrations

```bash
rango makemigrations                 # writes migrations/0001_<label>.surql
rango migrate                        # applies pending .surql files
```

Each migration and its `_rango_migrations` row run in one transaction (SurrealDB `DEFINE`
statements are transactional). A lock record (`_rango_lock:migrate`) keeps two `rango
migrate` from running at once; if one crashed, remove it with `DELETE _rango_lock:migrate`.

Changes are redefinitions (`DEFINE FIELD OVERWRITE`), never a table rebuild. A type change
applies to new writes: existing values keep their type until rewritten (the migration says
so). A rename copies the values (`UPDATE … SET new = old, old = NONE`).

`rango dump` / `restore` / `export` / `import` use the same Rango JSON as the other
backends, so a dump moves between SurrealDB and Postgres, SQLite or MySQL.

## Limitations

- Requires a SurrealDB **4** server (the SDK accepts 3.x and 4.x; Rango's schema uses 4.x).
- Partial unique constraints (`UNIQUE … WHERE`) are skipped with a comment, as on MySQL.
- A `CheckConstraint` runs its SQL as SurrealQL against the written record; a condition on a
  nullable column must allow `NONE` itself (`age = NONE OR age >= 18`).
- Writing to a table that was never migrated creates it (schemaless) instead of failing;
  reading it fails with `Error::TableMissing`.
- In debug builds the embedded engine needs a large stack: run it on a thread with
  8 MiB or more (`RUST_MIN_STACK`, or `std::thread::Builder::stack_size`).

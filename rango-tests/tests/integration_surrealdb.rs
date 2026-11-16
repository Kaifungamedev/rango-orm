//! SurrealDB 4 integration tests: embedded in-memory engine, no server needed.
//!
//! Run with:
//!   cargo test -p rango-tests --test integration_surrealdb --features integration-surrealdb
//!
//! Each test opens its own `surrealdb+mem://` datastore and defines the schema with the
//! same generator as `rango makemigrations`. With `SURREALDB_URL` set (a server, e.g.
//! `surrealdb+ws://root:rango@127.0.0.1:8011`), each test gets its own database there.
#![cfg(feature = "integration-surrealdb")]

use rango_orm_core::surreal_schema::define_table;
use rango_orm_core::*;
use rango_orm_derive::{Model, Partial};
use rango_orm_surrealdb::*;
use uuid::Uuid;

// ── Models ────────────────────────────────────────────────────────────────────

#[derive(Model, Debug, Clone, PartialEq)]
#[model(table = "s_user")]
struct SUser {
    id: FieldUuid,
    #[field(unique)]
    email: FieldEmail,
    name: FieldVarchar<1, 10>,
    active: FieldBool,
    score: Option<FieldInt>,
}

#[derive(Model, Debug, Clone)]
#[model(table = "s_post")]
struct SPost {
    id: FieldUuid,
    author_id: ForeignKey<SUser>,
    title: FieldVarchar<1, 100>,
}

#[derive(Model, Debug, Clone)]
#[model(table = "s_note")]
struct SNote {
    id: FieldUuid,
    #[field(on_delete = "set_null")]
    author_id: Option<ForeignKey<SUser>>,
    #[field(on_delete = "restrict")]
    post_id: Option<ForeignKey<SPost>>,
}

#[derive(Model, Debug, Clone)]
#[model(table = "s_tag")]
struct STag {
    id: FieldUuid,
    name: FieldVarchar<1, 50>,
}

#[derive(Model, Debug, Clone)]
#[model(table = "s_blog")]
struct SBlog {
    id: FieldUuid,
    title: FieldVarchar<1, 50>,
    tags: ManyToMany<STag>,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[model(table = "s_counter", constraints = [CheckConstraint::new("hits >= 0").name("chk_hits")])]
struct SCounter {
    id: FieldAutoBigInt,
    label: FieldText,
    hits: FieldBigInt,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[model(table = "s_kind")]
struct SKind {
    id: FieldUuid,
    small: FieldSmallInt,
    big: FieldBigInt,
    ratio: FieldFloat,
    precise: FieldDouble,
    price: FieldDecimal<10, 2>,
    blob: FieldBytes,
    day: FieldDate,
    at_time: FieldTime,
    at: FieldDateTime,
    meta: FieldJson,
    maybe: Option<FieldText>,
}

#[derive(Partial, Debug)]
#[partial(model = SUser)]
#[allow(dead_code)]
struct SUserLite {
    id: FieldUuid,
    email: FieldEmail,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// The embedded engine recurses deeply in debug builds: the default 2 MiB test-thread
/// stack overflows. Run each test on its own runtime, on a thread with a larger stack.
fn big_stack<F: std::future::Future<Output = ()> + 'static>(test: fn() -> F) {
    std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(test())
        })
        .unwrap()
        .join()
        .unwrap_or_else(|e| std::panic::resume_unwind(e));
}

fn pivot(from: &str, to: &str) -> TableSchema {
    let (name, from_col, to_col) = rango_orm_core::related::m2m_pivot(from, to);
    let col = |name: String, table: &str, index: bool| ColumnDef {
        name,
        col_type: ColumnType::Uuid,
        nullable: false,
        primary_key: false,
        unique: false,
        default: None,
        references: Some(FkReference {
            table: table.to_string(),
            column: "id".into(),
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::NoAction,
        }),
        index,
    };
    TableSchema {
        table_name: name,
        columns: vec![col(from_col, from, false), col(to_col, to, true)],
        ..TableSchema::default()
    }
}

/// A fresh, empty database: embedded, or on the `SURREALDB_URL` server.
fn fresh_url() -> String {
    match std::env::var("SURREALDB_URL") {
        Ok(base) => format!(
            "{}/rango_test/t{}",
            base.trim_end_matches('/'),
            Uuid::new_v4().simple()
        ),
        Err(_) => "surrealdb+mem://test/test".to_string(),
    }
}

async fn db() -> SurrealPool {
    let db = connect_url(&fresh_url()).await.expect("connect");
    let mut ddl = String::new();
    for schema in [
        SUser::schema(),
        SPost::schema(),
        SNote::schema(),
        STag::schema(),
        SBlog::schema(),
        pivot("s_blog", "s_tag"),
        SCounter::schema(),
        SKind::schema(),
    ] {
        ddl.push_str(&define_table(&schema));
    }
    raw_execute(&db, &ddl, vec![]).await.expect("schema");
    db
}

fn user(email: &str, name: &str, score: Option<i32>) -> SUser {
    SUser {
        id: FieldUuid(Uuid::new_v4()),
        email: FieldEmail::new(email).unwrap(),
        name: FieldVarchar::new(name).unwrap(),
        active: FieldBool(true),
        score: score.map(FieldInt),
    }
}

fn post(author: &SUser, title: &str) -> SPost {
    SPost {
        id: FieldUuid(Uuid::new_v4()),
        author_id: ForeignKey::new(author.id.0),
        title: FieldVarchar::new(title).unwrap(),
    }
}

// ── CRUD ──────────────────────────────────────────────────────────────────────

#[test]
fn crud_round_trip() {
    big_stack(crud_round_trip_body);
}

async fn crud_round_trip_body() {
    let db = db().await;
    let u = insert(&db, user("a@b.c", "ann", Some(3))).await.unwrap();
    assert_eq!(
        get::<SUser>(&db, &u.id.to_sql_value()).await.unwrap(),
        Some(u.clone())
    );

    let mut changed = u.clone();
    changed.score = None;
    changed.name = FieldVarchar::new("bob").unwrap();
    let saved = update(&db, changed.clone()).await.unwrap();
    assert_eq!(saved, changed, "NULL fields are unset by update");
    assert_eq!(all::<SUser>(&db).await.unwrap(), vec![changed.clone()]);

    delete(&db, &changed).await.unwrap();
    delete(&db, &changed).await.unwrap(); // idempotent
    assert!(
        get::<SUser>(&db, &u.id.to_sql_value())
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(update(&db, changed).await, Err(Error::NotFound)));
}

#[test]
fn typed_errors() {
    big_stack(typed_errors_body);
}

async fn typed_errors_body() {
    let db = db().await;
    let u = insert(&db, user("a@b.c", "ann", None)).await.unwrap();
    let dup = insert(&db, user("a@b.c", "other", None)).await;
    assert!(
        matches!(&dup, Err(Error::UniqueViolation { constraint: Some(c) }) if c == "s_user_email_unique"),
        "{dup:?}"
    );
    let same_pk = insert(
        &db,
        SUser {
            email: FieldEmail::new("x@y.z").unwrap(),
            ..u.clone()
        },
    )
    .await;
    assert!(
        matches!(same_pk, Err(Error::UniqueViolation { .. })),
        "{same_pk:?}"
    );

    let ghost = user("g@h.i", "ghost", None);
    let dangling = insert(&db, post(&ghost, "t")).await;
    assert!(
        matches!(&dangling, Err(Error::ForeignKeyViolation { constraint: Some(c) }) if c == "s_post_author_id_fkey"),
        "{dangling:?}"
    );

    // FieldVarchar<1, 10> bypassed through raw SurrealQL: the ASSERT still holds.
    let long = raw_execute(
        &db,
        "CREATE s_user CONTENT { email: 'l@m.n', name: 'much too long', active: true }",
        vec![],
    )
    .await;
    assert!(
        matches!(long, Err(Error::CheckViolation { .. })),
        "{long:?}"
    );
    let missing = raw_execute(
        &db,
        "CREATE s_user CONTENT { email: 'l@m.n', active: true }",
        vec![],
    )
    .await;
    assert!(
        matches!(missing, Err(Error::NotNullViolation { .. })),
        "{missing:?}"
    );

    let neg = insert(
        &db,
        SCounter {
            id: FieldAutoBigInt(None),
            label: "x".into(),
            hits: FieldBigInt(-1),
        },
    )
    .await;
    assert!(
        matches!(&neg, Err(Error::CheckViolation { constraint: Some(c) }) if c == "chk_hits"),
        "{neg:?}"
    );

    let fresh = connect_url(&fresh_url()).await.unwrap();
    let r = SUser::filter(&fresh).count().await;
    assert!(
        matches!(&r, Err(Error::TableMissing { table }) if table == "s_user"),
        "{r:?}"
    );
}

#[test]
fn auto_increment_keys_and_bulk_create() {
    big_stack(auto_increment_keys_and_bulk_create_body);
}

async fn auto_increment_keys_and_bulk_create_body() {
    let db = db().await;
    let c = |label: &str| SCounter {
        id: FieldAutoBigInt(None),
        label: label.into(),
        hits: FieldBigInt(0),
    };
    let a = insert(&db, c("a")).await.unwrap();
    let b = insert(&db, c("b")).await.unwrap();
    assert_eq!((a.id.0, b.id.0), (Some(1), Some(2)));
    assert_eq!(bulk_create(&db, &[c("c"), c("d")]).await.unwrap(), 2);
    let ids: Vec<i64> = SCounter::filter(&db)
        .order_by(SCounter::ID)
        .all()
        .await
        .unwrap()
        .iter()
        .map(|r| r.id.0.unwrap())
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);
    let got = get::<SCounter>(&db, &SqlValue::BigInt(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.label.0, "c");
}

#[test]
fn every_field_type_round_trips() {
    big_stack(every_field_type_round_trips_body);
}

async fn every_field_type_round_trips_body() {
    let db = db().await;
    let k = SKind {
        id: FieldUuid(Uuid::new_v4()),
        small: FieldSmallInt(-7),
        big: FieldBigInt(i64::MAX),
        ratio: FieldFloat(0.25),
        precise: FieldDouble(1.0 / 3.0),
        price: FieldDecimal("12345678.90".parse().unwrap()),
        blob: FieldBytes(vec![0, 1, 255]),
        day: FieldDate(chrono::NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()),
        at_time: FieldTime(chrono::NaiveTime::from_hms_micro_opt(23, 59, 1, 123456).unwrap()),
        at: FieldDateTime(chrono::DateTime::from_timestamp(1_800_000_000, 123_456_000).unwrap()),
        meta: FieldJson(serde_json::json!({"a": [1, null, {"b": "c"}], "n": null})),
        maybe: None,
    };
    let saved = insert(&db, k.clone()).await.unwrap();
    assert_eq!(saved, k);
    let values = SKind::filter(&db)
        .values(&["id", "price", "maybe", "meta"])
        .await
        .unwrap();
    assert_eq!(values[0]["id"], SqlValue::Uuid(k.id.0));
    assert_eq!(
        values[0]["price"],
        SqlValue::Decimal("12345678.90".parse().unwrap())
    );
    assert!(values[0]["maybe"].is_null());
    let days = SKind::filter(&db)
        .values(&["day", "at_time"])
        .await
        .unwrap();
    assert_eq!(days[0]["day"], SqlValue::Date(k.day.0));
    assert_eq!(days[0]["at_time"], SqlValue::Time(k.at_time.0));
    // Dates and times compare in order (stored as datetime / duration).
    let later = SKind::filter(&db)
        .gt(
            SKind::DAY,
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
        )
        .lt(
            SKind::AT_TIME,
            chrono::NaiveTime::from_hms_opt(23, 59, 2).unwrap(),
        )
        .count()
        .await
        .unwrap();
    assert_eq!(later, 1);
    let first_day = SKind::filter(&db)
        .min::<chrono::NaiveDate>(SKind::DAY)
        .await
        .unwrap();
    assert_eq!(first_day, Some(k.day.0));
}

// ── Query builder ─────────────────────────────────────────────────────────────

async fn seeded() -> SurrealPool {
    let db = db().await;
    for (email, name, score) in [
        ("a@x.io", "ann", Some(1)),
        ("b@x.io", "bob", Some(5)),
        ("c@x.io", "cid", Some(9)),
        ("d@y.io", "Dan", None),
    ] {
        insert(&db, user(email, name, score)).await.unwrap();
    }
    db
}

async fn names(q: QueryBuilder<'_, SUser>) -> Vec<String> {
    q.order_by(SUser::NAME)
        .all()
        .await
        .unwrap()
        .iter()
        .map(|u| u.name.to_string())
        .collect()
}

#[test]
fn filters_follow_sql_null_semantics() {
    big_stack(filters_follow_sql_null_semantics_body);
}

async fn filters_follow_sql_null_semantics_body() {
    let db = seeded().await;
    // A NONE score matches no comparison, as SQL NULL would not.
    assert_eq!(
        names(SUser::filter(&db).lt(SUser::SCORE, 6)).await,
        ["ann", "bob"]
    );
    assert_eq!(
        names(SUser::filter(&db).ne(SUser::SCORE, 5)).await,
        ["ann", "cid"]
    );
    assert_eq!(
        names(SUser::filter(&db).is_null(SUser::SCORE)).await,
        ["Dan"]
    );
    assert_eq!(
        SUser::filter(&db)
            .is_not_null(SUser::SCORE)
            .count()
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        names(SUser::filter(&db).in_values(SUser::SCORE, vec![1.into(), 9.into()])).await,
        ["ann", "cid"]
    );
    assert!(
        names(SUser::filter(&db).in_values(SUser::SCORE, vec![]))
            .await
            .is_empty()
    );
}

#[test]
fn like_connectors_and_groups() {
    big_stack(like_connectors_and_groups_body);
}

async fn like_connectors_and_groups_body() {
    let db = seeded().await;
    assert_eq!(
        names(SUser::filter(&db).like(SUser::EMAIL, "%@x.io")).await,
        ["ann", "bob", "cid"]
    );
    assert_eq!(
        names(SUser::filter(&db).like(SUser::NAME, "d%")).await,
        Vec::<String>::new()
    );
    assert_eq!(
        names(SUser::filter(&db).ilike(SUser::NAME, "d_n")).await,
        ["Dan"]
    );
    assert_eq!(
        names(SUser::filter(&db).like(SUser::EMAIL, "a@x.i_")).await,
        ["ann"]
    );
    assert_eq!(
        names(
            SUser::filter(&db)
                .eq(SUser::NAME, "ann")
                .or()
                .eq(SUser::NAME, "bob")
        )
        .await,
        ["ann", "bob"]
    );
    assert_eq!(
        names(
            SUser::filter(&db)
                .like(SUser::EMAIL, "%x.io")
                .not()
                .eq(SUser::NAME, "bob")
        )
        .await,
        ["ann", "cid"]
    );
    assert_eq!(
        names(
            SUser::filter(&db)
                .gt(SUser::SCORE, 2)
                .xor()
                .like(SUser::EMAIL, "c%")
        )
        .await,
        ["bob"]
    );
    assert_eq!(
        names(
            SUser::filter(&db)
                .is_not_null(SUser::SCORE)
                .group(|q| q.eq(SUser::NAME, "ann").or().eq(SUser::NAME, "Dan"))
        )
        .await,
        ["ann"]
    );
}

#[test]
fn ordering_pagination_and_aggregates() {
    big_stack(ordering_pagination_and_aggregates_body);
}

async fn ordering_pagination_and_aggregates_body() {
    let db = seeded().await;
    let page = SUser::filter(&db)
        .order_by("-email")
        .limit(2)
        .offset(1)
        .all()
        .await
        .unwrap();
    let emails: Vec<_> = page.iter().map(|u| u.email.to_string()).collect();
    assert_eq!(emails, ["c@x.io", "b@x.io"]);
    // `after` orders by its column first.
    assert_eq!(
        names(SUser::filter(&db).after(SUser::EMAIL, "b@x.io")).await,
        ["cid", "Dan"]
    );
    let first = SUser::filter(&db)
        .order_by(SUser::EMAIL)
        .one()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.name.to_string(), "ann");

    assert_eq!(SUser::filter(&db).count().await.unwrap(), 4);
    assert!(
        SUser::filter(&db)
            .eq(SUser::NAME, "bob")
            .exists()
            .await
            .unwrap()
    );
    assert!(
        !SUser::filter(&db)
            .eq(SUser::NAME, "zed")
            .exists()
            .await
            .unwrap()
    );
    assert_eq!(
        SUser::filter(&db).sum(SUser::SCORE).await.unwrap(),
        Some(15.0)
    );
    assert_eq!(
        SUser::filter(&db).avg(SUser::SCORE).await.unwrap(),
        Some(5.0)
    );
    assert_eq!(
        SUser::filter(&db).min::<i32>(SUser::SCORE).await.unwrap(),
        Some(1)
    );
    assert_eq!(
        SUser::filter(&db).max::<String>(SUser::NAME).await.unwrap(),
        Some("cid".into())
    );
    assert_eq!(
        SUser::filter(&db)
            .is_null(SUser::SCORE)
            .sum(SUser::SCORE)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        SUser::filter(&db)
            .eq(SUser::NAME, "zed")
            .count()
            .await
            .unwrap(),
        0
    );

    let lite = SUser::filter(&db)
        .eq(SUser::NAME, "bob")
        .select_as::<SUserLite>()
        .all()
        .await
        .unwrap();
    assert_eq!(lite[0].email.to_string(), "b@x.io");
    let sql = SUser::filter(&db).eq(SUser::EMAIL, "b@x.io").explain();
    assert!(
        sql.starts_with("SELECT * FROM `s_user` WHERE `email` = $p1"),
        "{sql}"
    );
    let plan = SUser::filter(&db)
        .eq(SUser::EMAIL, "b@x.io")
        .explain_analyze()
        .await
        .unwrap();
    assert!(plan.contains("s_user_email_unique"), "{plan}");

    let bad = SUser::filter(&db).eq("emial", "x").all().await;
    assert!(matches!(bad, Err(Error::Invalid(_))));
}

// ── Relations ─────────────────────────────────────────────────────────────────

#[test]
fn foreign_keys_cascade_set_null_and_restrict_in_the_database() {
    big_stack(foreign_keys_cascade_set_null_and_restrict_in_the_database_body);
}

async fn foreign_keys_cascade_set_null_and_restrict_in_the_database_body() {
    let db = db().await;
    let u = insert(&db, user("a@b.c", "ann", None)).await.unwrap();
    let p = insert(&db, post(&u, "hello")).await.unwrap();
    let n = insert(
        &db,
        SNote {
            id: FieldUuid(Uuid::new_v4()),
            author_id: Some(ForeignKey::new(u.id.0)),
            post_id: Some(ForeignKey::new(p.id.0)),
        },
    )
    .await
    .unwrap();

    // ON DELETE RESTRICT: the note still points at the post.
    assert!(matches!(
        delete(&db, &p).await,
        Err(Error::ForeignKeyViolation { .. })
    ));
    let mut detached = n.clone();
    detached.post_id = None;
    update(&db, detached).await.unwrap();

    // ON DELETE CASCADE (post) and SET NULL (note.author_id), done by SurrealDB.
    delete(&db, &u).await.unwrap();
    assert_eq!(SPost::filter(&db).count().await.unwrap(), 0);
    let note = get::<SNote>(&db, &n.id.to_sql_value())
        .await
        .unwrap()
        .unwrap();
    assert!(note.author_id.is_none());
}

#[test]
fn select_related_prefetch_and_subqueries() {
    big_stack(select_related_prefetch_and_subqueries_body);
}

async fn select_related_prefetch_and_subqueries_body() {
    let db = db().await;
    let ann = insert(&db, user("a@b.c", "ann", None)).await.unwrap();
    let bob = insert(&db, user("b@b.c", "bob", None)).await.unwrap();
    for t in ["one", "two"] {
        insert(&db, post(&ann, t)).await.unwrap();
    }

    let posts = SPost::filter(&db)
        .select_related(SPost::AUTHOR_ID)
        .order_by(SPost::TITLE)
        .all()
        .await
        .unwrap();
    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0].related::<SUser>().email, ann.email);

    let users = SUser::filter(&db)
        .prefetch_related(SPost::AUTHOR_ID)
        .order_by(SUser::NAME)
        .all()
        .await
        .unwrap();
    assert_eq!(users[0].prefetched::<SPost>().len(), 2);
    assert!(users[1].prefetched::<SPost>().is_empty());

    let authors = SUser::filter(&db)
        .where_exists(SPost::AUTHOR_ID, |q| q.eq(SPost::TITLE, "two"))
        .all()
        .await
        .unwrap();
    assert_eq!(authors.len(), 1);
    assert_eq!(authors[0].id, ann.id);
    let lonely = SUser::filter(&db)
        .not()
        .where_exists(SPost::AUTHOR_ID, |q| q)
        .all()
        .await
        .unwrap();
    assert_eq!(lonely[0].id, bob.id);
    let by_ann = SPost::filter(&db)
        .in_subquery(SPost::AUTHOR_ID, SUser::ID, |q| q.eq(SUser::NAME, "ann"))
        .count()
        .await
        .unwrap();
    assert_eq!(by_ann, 2);
    let by_id = SPost::filter(&db)
        .eq(SPost::AUTHOR_ID, ann.id.0)
        .count()
        .await
        .unwrap();
    assert_eq!(by_id, 2);
}

#[test]
fn many_to_many() {
    big_stack(many_to_many_body);
}

async fn many_to_many_body() {
    let db = db().await;
    let blog = insert(
        &db,
        SBlog {
            id: FieldUuid(Uuid::new_v4()),
            title: FieldVarchar::new("b").unwrap(),
            tags: ManyToMany::empty(),
        },
    )
    .await
    .unwrap();
    let empty = insert(
        &db,
        SBlog {
            id: FieldUuid(Uuid::new_v4()),
            title: FieldVarchar::new("e").unwrap(),
            tags: ManyToMany::empty(),
        },
    )
    .await
    .unwrap();
    let tag = |n: &str| STag {
        id: FieldUuid(Uuid::new_v4()),
        name: FieldVarchar::new(n).unwrap(),
    };
    let (t1, t2) = (
        insert(&db, tag("orm")).await.unwrap(),
        insert(&db, tag("rust")).await.unwrap(),
    );

    M2M::<SBlog, STag>::add(&db, &blog, &t1).await.unwrap();
    M2M::<SBlog, STag>::add(&db, &blog, &t1).await.unwrap(); // idempotent
    M2M::<SBlog, STag>::add(&db, &blog, &t2).await.unwrap();
    assert_eq!(M2M::<SBlog, STag>::all(&db, &blog).await.unwrap().len(), 2);
    assert_eq!(
        M2M::<SBlog, STag>::reverse(&db, &t1).await.unwrap()[0].id,
        blog.id
    );
    assert!(M2M::<SBlog, STag>::exists(&db, &blog, &t2).await.unwrap());

    let blogs = SBlog::filter(&db)
        .prefetch_related(SBlog::TAGS)
        .order_by(SBlog::TITLE)
        .all()
        .await
        .unwrap();
    assert_eq!(blogs[0].prefetched::<STag>().len(), 2);
    assert!(blogs[1].prefetched::<STag>().is_empty());

    M2M::<SBlog, STag>::remove(&db, &blog, &t1).await.unwrap();
    assert!(!M2M::<SBlog, STag>::exists(&db, &blog, &t1).await.unwrap());
    M2M::<SBlog, STag>::set(&db, &empty, &[t1.clone(), t2.clone()])
        .await
        .unwrap();
    assert_eq!(M2M::<SBlog, STag>::all(&db, &empty).await.unwrap().len(), 2);
    // Deleting a tag cascades to the pivot.
    delete(&db, &t2).await.unwrap();
    assert_eq!(M2M::<SBlog, STag>::all(&db, &empty).await.unwrap().len(), 1);
    M2M::<SBlog, STag>::clear(&db, &empty).await.unwrap();
    assert!(
        M2M::<SBlog, STag>::all(&db, &empty)
            .await
            .unwrap()
            .is_empty()
    );
}

// ── Transactions, compound and bulk ops ───────────────────────────────────────

#[test]
fn transactions_commit_and_roll_back() {
    big_stack(transactions_commit_and_roll_back_body);
}

async fn transactions_commit_and_roll_back_body() {
    let db = db().await;
    if !db.supports_transactions() {
        // HTTP: every request stands alone.
        let r = atomic(&db, async |_tx| Ok(())).await;
        assert!(matches!(r, Err(Error::Invalid(_))), "{r:?}");
        return;
    }
    let u = atomic(&db, async |tx| {
        let u = insert(&*tx, user("a@b.c", "ann", None)).await?;
        insert(&*tx, post(&u, "in tx")).await?;
        assert_eq!(SPost::filter(&*tx).count().await?, 1);
        Ok(u)
    })
    .await
    .unwrap();
    assert_eq!(SPost::filter(&db).count().await.unwrap(), 1);

    let aborted: Result<()> = atomic(&db, async |tx| {
        insert(&*tx, post(&u, "rolled back")).await?;
        Err(Error::abort("no"))
    })
    .await;
    assert!(matches!(aborted, Err(Error::Aborted(_))));
    assert_eq!(SPost::filter(&db).count().await.unwrap(), 1);

    let failed: Result<()> = atomic(&db, async |tx| {
        insert(&*tx, user("b@b.c", "bob", None)).await?;
        insert(&*tx, user("a@b.c", "dup", None)).await?;
        Ok(())
    })
    .await;
    assert!(
        matches!(failed, Err(Error::UniqueViolation { .. })),
        "{failed:?}"
    );
    assert_eq!(SUser::filter(&db).count().await.unwrap(), 1);
}

#[test]
fn get_or_create_and_update_or_create() {
    big_stack(get_or_create_and_update_or_create_body);
}

async fn get_or_create_and_update_or_create_body() {
    let db = db().await;
    let lookup = || vec![("email", SqlValue::Text("a@b.c".into()))];
    let (u, created) = get_or_create(&db, lookup(), user("a@b.c", "ann", None))
        .await
        .unwrap();
    assert!(created);
    let (again, created) = get_or_create(&db, lookup(), user("a@b.c", "other", None))
        .await
        .unwrap();
    assert!(!created);
    assert_eq!(again.id, u.id);

    let saved = update_or_create(&db, lookup(), user("a@b.c", "renamed", Some(4)))
        .await
        .unwrap();
    assert_eq!(saved.id, u.id, "updates the existing record");
    assert_eq!(saved.name.to_string(), "renamed");
    let new = update_or_create(
        &db,
        vec![("email", SqlValue::Text("n@b.c".into()))],
        user("n@b.c", "new", None),
    )
    .await
    .unwrap();
    assert_ne!(new.id, u.id);
    assert_eq!(SUser::filter(&db).count().await.unwrap(), 2);
}

#[test]
fn bulk_update_and_upsert() {
    big_stack(bulk_update_and_upsert_body);
}

async fn bulk_update_and_upsert_body() {
    let db = db().await;
    let mut users = vec![user("a@b.c", "ann", Some(1)), user("b@b.c", "bob", Some(2))];
    assert_eq!(bulk_create(&db, &users).await.unwrap(), 2);
    users[0].score = Some(FieldInt(10));
    users[1].score = None;
    users[1].name = FieldVarchar::new("ignored").unwrap();
    let ghost = user("g@b.c", "ghost", None);
    let mut with_ghost = users.clone();
    with_ghost.push(ghost.clone());
    assert_eq!(bulk_update(&db, &with_ghost, &["score"]).await.unwrap(), 2);
    let bob = get::<SUser>(&db, &users[1].id.to_sql_value())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((bob.score, bob.name.to_string()), (None, "bob".to_string()));
    assert!(
        get::<SUser>(&db, &ghost.id.to_sql_value())
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        bulk_update(&db, &users, &["id"]).await,
        Err(Error::Invalid(_))
    ));

    // Upsert by email: the existing record (another id) is updated, the new one inserted.
    let mut again = user("a@b.c", "ann2", Some(7));
    again.id = FieldUuid(Uuid::new_v4());
    let n = bulk_upsert(&db, &[again, ghost.clone()], &["email"])
        .await
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(SUser::filter(&db).count().await.unwrap(), 3);
    let ann = SUser::filter(&db)
        .eq(SUser::EMAIL, "a@b.c")
        .one()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (ann.id, ann.name.to_string()),
        (users[0].id.clone(), "ann2".to_string())
    );
    // On the PK: the existing record is overwritten.
    let mut renamed = ghost.clone();
    renamed.name = FieldVarchar::new("casper").unwrap();
    assert_eq!(bulk_upsert(&db, &[renamed], &[]).await.unwrap(), 1);
    let g = get::<SUser>(&db, &ghost.id.to_sql_value())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(g.name.to_string(), "casper");
    // A conflict on another unique index than `conflict_on` is an error, as on Postgres.
    let mut clash = user("g@b.c", "clash", None);
    clash.id = FieldUuid(Uuid::new_v4());
    let r = bulk_upsert(&db, &[clash], &[]).await;
    assert!(matches!(r, Err(Error::UniqueViolation { .. })), "{r:?}");
}

#[test]
fn raw_surrealql() {
    big_stack(raw_surrealql_body);
}

async fn raw_surrealql_body() {
    let db = seeded().await;
    let rows: Vec<SUser> = raw(
        &db,
        "SELECT * FROM s_user WHERE score > $p1 ORDER BY score",
        vec![SqlValue::Int(4)],
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    let n: i64 = raw_scalar(&db, "SELECT count() FROM s_user GROUP ALL", vec![])
        .await
        .unwrap();
    assert_eq!(n, 4);
    let total: Option<i64> = raw_scalar(
        &db,
        "RETURN math::sum((SELECT VALUE score FROM s_user WHERE score != NONE))",
        vec![],
    )
    .await
    .unwrap();
    assert_eq!(total, Some(15));
    // Raw SurrealQL keeps SurrealQL semantics: NONE sorts below every number.
    let deleted = raw_execute(
        &db,
        "DELETE s_user WHERE score < $p1 RETURN BEFORE",
        vec![SqlValue::Int(6)],
    )
    .await
    .unwrap();
    assert_eq!(deleted, 3);
}

#[test]
fn backend_extension() {
    big_stack(backend_extension_body);
}

async fn backend_extension_body() {
    let db = db().await;
    let k = |v| SKind {
        id: FieldUuid(Uuid::new_v4()),
        small: FieldSmallInt(0),
        big: FieldBigInt(0),
        ratio: FieldFloat(0.0),
        precise: FieldDouble(0.0),
        price: FieldDecimal(Decimal::ZERO),
        blob: FieldBytes(vec![]),
        day: FieldDate(chrono::NaiveDate::MIN),
        at_time: FieldTime(chrono::NaiveTime::MIN),
        at: FieldDateTime(chrono::DateTime::UNIX_EPOCH),
        meta: FieldJson(v),
        maybe: Some(FieldText("x".into())),
    };
    insert(
        &db,
        k(serde_json::json!({"status": "published", "tags": ["a", "b"]})),
    )
    .await
    .unwrap();
    insert(
        &db,
        k(serde_json::json!({"status": "draft", "tags": ["c"]})),
    )
    .await
    .unwrap();
    let n = SKind::filter(&db)
        .json_path_eq(SKind::META, &["status"], "published")
        .count()
        .await
        .unwrap();
    assert_eq!(n, 1);
    let n = SKind::filter(&db)
        .json_path_eq(SKind::META, &["tags"], serde_json::json!(["c"]))
        .count()
        .await
        .unwrap();
    assert_eq!(n, 1);
}

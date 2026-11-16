//! SurrealDB errors as typed Rango errors. Messages never carry SurrealQL text or values.

use rango_orm_core::Error;
use surrealdb::types::{NotFoundError, QueryError};

/// Translate a SurrealDB error into a typed [`Error`].
pub(crate) fn db_err(e: surrealdb::Error) -> Error {
    if e.is_connection() {
        return Error::Connection(first_line(e.message()).to_string());
    }
    match e.query_details() {
        Some(QueryError::TransactionConflict) => return Error::Conflict,
        Some(QueryError::TimedOut { .. }) => return Error::Timeout,
        _ => {}
    }
    if let Some(NotFoundError::Table { name }) = e.not_found_details() {
        return Error::TableMissing {
            table: name.clone(),
        };
    }
    let msg = e.message();
    // Constraint names Rango puts in THROW (see `rango makemigrations`).
    if let Some(name) = tagged(msg, "rango:fk:") {
        return Error::ForeignKeyViolation {
            constraint: Some(name),
        };
    }
    if let Some(name) = tagged(msg, "rango:check:") {
        return Error::CheckViolation {
            constraint: Some(name),
        };
    }
    if e.is_already_exists() {
        // "Database record `user:⟨..⟩` already exists": the primary key.
        let table = between(msg, "`", "`")
            .and_then(|r| r.split(':').next())
            .map(|t| format!("{t}_pkey"));
        return Error::UniqueViolation { constraint: table };
    }
    if msg.starts_with("Database index `") && msg.contains("already contains") {
        return Error::UniqueViolation {
            constraint: between(msg, "`", "`").map(str::to_string),
        };
    }
    if msg.contains("with an ON DELETE REJECT clause") {
        return Error::ForeignKeyViolation { constraint: None };
    }
    // "Couldn't coerce value for field `email` of `user:1`: Expected `string` but found `NONE`"
    if let Some(rest) = msg.strip_prefix("Couldn't coerce value for field `") {
        let field = rest.split('`').next().unwrap_or_default();
        let table = between(rest, "of `", ":").unwrap_or_default();
        let constraint = Some(format!("{table}.{field}"));
        if msg.ends_with("but found `NONE`") {
            return Error::NotNullViolation { constraint };
        }
        let expected = between(rest, "Expected `", "`").unwrap_or("?");
        return Error::Invalid(format!(
            "field `{field}` of `{table}`: value does not match type `{expected}`"
        ));
    }
    // "Found 'x' for field `name`, with record `user:1`, but field must conform to: ..."
    if msg.contains("but field must conform to") {
        let field = between(msg, "for field `", "`").unwrap_or_default();
        let table = between(msg, "with record `", ":").unwrap_or_default();
        return Error::CheckViolation {
            constraint: Some(format!("{table}.{field}")),
        };
    }
    if e.is_validation() {
        // Parse errors quote the statement after the first line.
        return Error::Invalid(redact(first_line(msg)));
    }
    Error::Database {
        code: Some(e.kind_str().to_string()),
        message: redact(first_line(msg)),
    }
}

fn first_line(msg: &str) -> &str {
    msg.lines().next().unwrap_or(msg)
}

fn between<'a>(s: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let from = s.find(start)? + start.len();
    let len = s[from..].find(end)?;
    Some(&s[from..from + len])
}

/// The identifier after `tag` (`rango:fk:post_author_fkey` -> `post_author_fkey`).
fn tagged(msg: &str, tag: &str) -> Option<String> {
    let rest = &msg[msg.find(tag)? + tag.len()..];
    let name: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then_some(name)
}

/// Blank out quoted literals (`'...'`, `"..."`): they may hold bound values.
fn redact(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for c in msg.chars() {
        match quote {
            Some(q) => {
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == q {
                    out.push('…');
                    out.push(c);
                    quote = None;
                }
            }
            None => {
                out.push(c);
                if c == '\'' || c == '"' {
                    quote = Some(c);
                }
            }
        }
    }
    if quote.is_some() {
        out.push('…');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_literals() {
        assert_eq!(
            redact("found 'secret' and \"x\\\"y\""),
            "found '…' and \"…\""
        );
        assert_eq!(
            tagged("err: rango:fk:post_a_fkey", "rango:fk:").unwrap(),
            "post_a_fkey"
        );
        assert_eq!(
            between("index `u_email` already", "`", "`"),
            Some("u_email")
        );
    }
}

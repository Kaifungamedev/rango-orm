//! Rango SurrealDB URLs.
//!
//! ```text
//! surrealdb://[user:pass@]host[:port]/namespace/database[?auth=root|namespace|database]
//! surrealdb+ws://…   surrealdb+wss://…   surrealdb+http://…   surrealdb+https://…
//! surrealdb+mem://[namespace/database]
//! ```
//!
//! `surrealdb://` picks the transport: `wss` (TLS) for a remote host, `ws` for a loopback
//! address, so a production URL never falls back to cleartext. Namespace and database
//! default to `rango`. Credentials sign in as a root user unless `auth=` says otherwise.

use rango_orm_core::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthLevel {
    Root,
    Namespace,
    Database,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Credentials {
    pub level: AuthLevel,
    pub user: String,
    pub pass: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("level", &self.level)
            .field("user", &self.user)
            .field("pass", &"***")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Target {
    /// Endpoint for `surrealdb::engine::any::connect` (`wss://host:8000`, `mem://`).
    pub endpoint: String,
    pub namespace: String,
    pub database: String,
    pub credentials: Option<Credentials>,
}

const DEFAULT_NAME: &str = "rango";

pub(crate) fn parse(url: &str) -> Result<Target> {
    let invalid = |why: &str| Error::Invalid(format!("invalid SurrealDB URL: {why}"));
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| invalid("expected `surrealdb://...`"))?;
    let (rest, query) = match rest.split_once('?') {
        Some((r, q)) => (r, Some(q)),
        None => (rest, None),
    };

    let transport = match scheme.to_ascii_lowercase().as_str() {
        "surrealdb" | "surreal" => None,
        "surrealdb+ws" => Some("ws"),
        "surrealdb+wss" => Some("wss"),
        "surrealdb+http" => Some("http"),
        "surrealdb+https" => Some("https"),
        "surrealdb+mem" | "mem" => Some("mem"),
        _ => {
            return Err(invalid(
                "unknown scheme, expected surrealdb[+ws|+wss|+http|+https|+mem]",
            ));
        }
    };

    let mut level = AuthLevel::Root;
    for pair in query.into_iter().flat_map(|q| q.split('&')) {
        match pair.split_once('=') {
            Some(("auth", "root")) => level = AuthLevel::Root,
            Some(("auth", "namespace" | "ns")) => level = AuthLevel::Namespace,
            Some(("auth", "database" | "db")) => level = AuthLevel::Database,
            Some(("auth", _)) => return Err(invalid("auth must be root, namespace or database")),
            _ => return Err(invalid("unknown query parameter")),
        }
    }

    if transport == Some("mem") {
        let (namespace, database) = names(rest)?;
        return Ok(Target {
            endpoint: "mem://".to_string(),
            namespace,
            database,
            credentials: None,
        });
    }

    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let (userinfo, host) = match authority.rfind('@') {
        Some(i) => (Some(&authority[..i]), &authority[i + 1..]),
        None => (None, authority),
    };
    if host.is_empty() {
        return Err(invalid("missing host"));
    }
    let credentials = match userinfo {
        Some(info) => {
            let (user, pass) = info.split_once(':').unwrap_or((info, ""));
            Some(Credentials {
                level,
                user: percent_decode(user)?,
                pass: percent_decode(pass)?,
            })
        }
        None => None,
    };
    let transport = transport.unwrap_or_else(|| {
        if rango_orm_core::config::is_local_host(host_name(host)) {
            "ws"
        } else {
            "wss"
        }
    });
    let (namespace, database) = names(path)?;
    Ok(Target {
        endpoint: format!("{transport}://{host}"),
        namespace,
        database,
        credentials,
    })
}

/// `host` without its port (`[::1]:8000` -> `[::1]`, `db:8000` -> `db`).
fn host_name(host: &str) -> &str {
    if host.starts_with('[') {
        return host.split_inclusive(']').next().unwrap_or(host);
    }
    host.rsplit_once(':').map_or(host, |(h, _)| h)
}

/// `/namespace/database` (each part optional).
fn names(path: &str) -> Result<(String, String)> {
    let mut parts = path.split('/').filter(|p| !p.is_empty());
    let ns = parts.next().map(percent_decode).transpose()?;
    let db = parts.next().map(percent_decode).transpose()?;
    if parts.next().is_some() {
        return Err(Error::Invalid(
            "invalid SurrealDB URL: the path is /namespace/database".into(),
        ));
    }
    Ok((
        ns.unwrap_or_else(|| DEFAULT_NAME.to_string()),
        db.unwrap_or_else(|| DEFAULT_NAME.to_string()),
    ))
}

fn percent_decode(s: &str) -> Result<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = s
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| Error::Invalid("invalid SurrealDB URL: bad %-escape".into()))?;
            out.push(hex);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| Error::Invalid("invalid SurrealDB URL: not UTF-8".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_hosts_default_to_tls() {
        let t = parse("surrealdb://root:p%40ss@db.example.com:8000/app/main").unwrap();
        assert_eq!(t.endpoint, "wss://db.example.com:8000");
        assert_eq!((t.namespace.as_str(), t.database.as_str()), ("app", "main"));
        let c = t.credentials.unwrap();
        assert_eq!((c.user.as_str(), c.pass.as_str()), ("root", "p@ss"));
        assert_eq!(c.level, AuthLevel::Root);
        assert!(!format!("{c:?}").contains("p@ss"));

        assert_eq!(
            parse("surrealdb://localhost:8000").unwrap().endpoint,
            "ws://localhost:8000"
        );
        assert_eq!(
            parse("surrealdb://[::1]:8000/a/b").unwrap().endpoint,
            "ws://[::1]:8000"
        );
        assert_eq!(
            parse("surrealdb+http://10.0.0.5:8000/a/b?auth=database")
                .unwrap()
                .endpoint,
            "http://10.0.0.5:8000"
        );
    }

    #[test]
    fn memory_and_errors() {
        let t = parse("surrealdb+mem://").unwrap();
        assert_eq!(t.endpoint, "mem://");
        assert_eq!(t.namespace, "rango");
        assert_eq!(parse("surrealdb+mem://ns/db").unwrap().database, "db");
        assert!(parse("postgres://x").is_err());
        assert!(parse("surrealdb://h/a/b/c").is_err());
        assert!(parse("surrealdb://h/a?auth=admin").is_err());
        assert!(parse("surrealdb://u:%zz@h").is_err());
    }
}

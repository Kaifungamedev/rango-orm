pub mod dump;
pub mod export;
pub mod import;
pub mod init;
pub mod makemigrations;
pub mod migrate;
#[cfg(feature = "surrealdb")]
pub mod surreal;

/// Error of a SurrealDB command in a `rango` built without the `surrealdb` feature.
#[cfg(not(feature = "surrealdb"))]
pub(crate) const SURREAL_DISABLED: &str = "this rango was built without SurrealDB support \
     (the SurrealDB SDK is BUSL-1.1, so it is opt-in): reinstall with \
     `cargo install rango-orm-cli --features surrealdb`";

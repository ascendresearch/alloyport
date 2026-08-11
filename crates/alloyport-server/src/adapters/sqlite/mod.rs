//! `SQLite` implementations of server application ports.

pub(crate) mod assignment_delivery;
mod identity_registry;

pub use identity_registry::SqliteIdentityRegistry;

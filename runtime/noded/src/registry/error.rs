//! Registry errors, one variant per Python exception class in
//! ucloud_sandboxes/direct_registry.py; `message()` is Python's `str(exc)`.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    /// `DirectRegistryError`.
    Registry(String),
    /// `DirectRegistryError("direct registry is unreadable")`: SQLite or the
    /// file failed. `cause` is diagnostic only.
    Unreadable { cause: String },
    /// `DirectRegistryConflictError`.
    Conflict(String),
    /// `DirectRegistrationOwnedError`: another incarnation owns the ID.
    RegistrationOwned(String),
    /// `DirectRegistryCapacityUnavailable`: retryable, nothing was granted.
    CapacityUnavailable(String),
    /// `ManagedPrimaryOwnedError(job_id)`.
    ManagedPrimaryOwned { job_id: String },
    /// `ValueError`: an invalid argument or new record state.
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, RegistryError>;

pub const UNREADABLE: &str = "direct registry is unreadable";
pub const MANAGED_PRIMARY_OWNED: &str = "sandbox generation already owns another primary process";

impl RegistryError {
    pub fn registry(message: impl Into<String>) -> Self {
        RegistryError::Registry(message.into())
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        RegistryError::Conflict(message.into())
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        RegistryError::Invalid(message.into())
    }

    pub fn unreadable(cause: impl fmt::Display) -> Self {
        RegistryError::Unreadable { cause: cause.to_string() }
    }

    /// The Python class this error is raised as.
    pub fn python_class(&self) -> &'static str {
        match self {
            RegistryError::Registry(_) | RegistryError::Unreadable { .. } => "DirectRegistryError",
            RegistryError::Conflict(_) => "DirectRegistryConflictError",
            RegistryError::RegistrationOwned(_) => "DirectRegistrationOwnedError",
            RegistryError::CapacityUnavailable(_) => "DirectRegistryCapacityUnavailable",
            RegistryError::ManagedPrimaryOwned { .. } => "ManagedPrimaryOwnedError",
            RegistryError::Invalid(_) => "ValueError",
        }
    }

    /// Python's subclassing: every class but `ValueError` is a `DirectRegistryError`,
    /// and the last three are `DirectRegistryConflictError`s.
    pub fn is_conflict(&self) -> bool {
        matches!(
            self,
            RegistryError::Conflict(_)
                | RegistryError::RegistrationOwned(_)
                | RegistryError::CapacityUnavailable(_)
                | RegistryError::ManagedPrimaryOwned { .. }
        )
    }

    pub fn message(&self) -> &str {
        match self {
            RegistryError::Registry(m)
            | RegistryError::Conflict(m)
            | RegistryError::RegistrationOwned(m)
            | RegistryError::CapacityUnavailable(m)
            | RegistryError::Invalid(m) => m,
            RegistryError::Unreadable { .. } => UNREADABLE,
            RegistryError::ManagedPrimaryOwned { .. } => MANAGED_PRIMARY_OWNED,
        }
    }
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegistryError::Unreadable { cause } => write!(f, "{UNREADABLE} ({cause})"),
            other => f.write_str(other.message()),
        }
    }
}

impl std::error::Error for RegistryError {}

impl From<rusqlite::Error> for RegistryError {
    fn from(error: rusqlite::Error) -> Self {
        RegistryError::unreadable(error)
    }
}

impl From<std::io::Error> for RegistryError {
    fn from(error: std::io::Error) -> Self {
        RegistryError::unreadable(error)
    }
}

//! The SQLite file: DDL, identity, connection pragmas, creation, migration
//! and validation, exactly as `DirectSandboxRegistry` does them. Python
//! compares the stored `sqlite_schema.sql` text with its own literal, so the
//! DDL below is that literal byte for byte and is split the same way.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, OpenFlags, OptionalExtension};

use super::error::{RegistryError, Result};
use super::record::{DrainState, Registration};
use super::spec::is_digest;

pub const APPLICATION_ID: i64 = 0x5543_5247;
pub const SCHEMA_VERSION: i64 = 9;
/// Legacy `user_version`s an opener migrates in place.
pub const LEGACY_VERSIONS: [i64; 6] = [3, 4, 5, 6, 7, 8];
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(30);

/// Python's `DirectSandboxRegistry._SCHEMA`, verbatim.
pub const SCHEMA: &str = r#"
        CREATE TABLE registry_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            activity_revision INTEGER NOT NULL CHECK (activity_revision >= 0),
            runtime_compatibility_sha256 TEXT CHECK (
                runtime_compatibility_sha256 IS NULL OR (
                    length(runtime_compatibility_sha256) = 64 AND
                    runtime_compatibility_sha256 NOT GLOB '*[^0-9a-f]*'
                )
            ),
            drain_json TEXT NOT NULL CHECK (json_valid(drain_json))
        ) STRICT;
        CREATE TABLE registrations (
            sandbox_id TEXT PRIMARY KEY,
            image_id TEXT NOT NULL,
            record_json TEXT NOT NULL CHECK (json_valid(record_json))
        ) STRICT;
        CREATE TABLE generation_tombstones (
            sandbox_id TEXT PRIMARY KEY,
            generation INTEGER NOT NULL CHECK (generation > 0)
        ) STRICT;
        CREATE TABLE migration_tombstones (
            sandbox_id TEXT NOT NULL,
            migration_id TEXT NOT NULL,
            PRIMARY KEY (sandbox_id, migration_id)
        ) STRICT;
        CREATE INDEX registrations_image_id ON registrations (image_id);
        CREATE TABLE relay_wake_fences (
            sandbox_id TEXT NOT NULL,
            generation INTEGER NOT NULL CHECK (generation > 0),
            request_id TEXT NOT NULL,
            PRIMARY KEY (sandbox_id, generation, request_id)
        ) STRICT;
        CREATE TABLE managed_growth (
            sandbox_id TEXT PRIMARY KEY,
            generation INTEGER NOT NULL CHECK (generation > 0),
            job_id TEXT NOT NULL,
            launch_sha256 TEXT NOT NULL,
            memory_bytes INTEGER NOT NULL CHECK (memory_bytes > 0),
            phase TEXT NOT NULL CHECK (phase IN ('queued','active','safe','parked','terminal')),
            request_id TEXT NOT NULL
        ) STRICT;
        CREATE TABLE reflink_overlaps (
            sandbox_id TEXT NOT NULL,
            sandbox_generation INTEGER NOT NULL CHECK (sandbox_generation > 0),
            hibernation_generation INTEGER NOT NULL CHECK (hibernation_generation > 0),
            allocated_bytes INTEGER NOT NULL CHECK (allocated_bytes >= 0),
            manifest_sha256 TEXT NOT NULL CHECK (
                length(manifest_sha256) = 64 AND
                manifest_sha256 NOT GLOB '*[^0-9a-f]*'
            ),
            PRIMARY KEY (sandbox_id, sandbox_generation, hibernation_generation)
        ) STRICT;
        CREATE TABLE workspace_capacity (
            sandbox_id TEXT PRIMARY KEY,
            sandbox_generation INTEGER NOT NULL CHECK (sandbox_generation > 0),
            mount_epoch INTEGER NOT NULL CHECK (mount_epoch >= 0),
            released_mb INTEGER NOT NULL CHECK (released_mb >= 0)
        ) STRICT;
        CREATE TABLE registration_disk (
            sandbox_id TEXT PRIMARY KEY,
            sandbox_generation INTEGER NOT NULL CHECK (sandbox_generation > 0),
            reserved_mb INTEGER NOT NULL CHECK (reserved_mb >= 0),
            workspace_mb INTEGER NOT NULL CHECK (workspace_mb >= 0),
            memory_mb INTEGER NOT NULL CHECK (memory_mb >= 0),
            dynamic INTEGER NOT NULL CHECK (dynamic IN (0, 1))
        ) STRICT;
        INSERT INTO registry_metadata VALUES (
            1,
            0,
            NULL,
            '{"admission_open":true,"drain_activity_epoch":0,"draining":false,"token":""}'
        );
    "#;

/// `registration_disk` as schema version 8 created it.
const LEGACY_V8_REGISTRATION_DISK: &str = "CREATE TABLE registration_disk (\n            sandbox_id TEXT PRIMARY KEY,\n            sandbox_generation INTEGER NOT NULL CHECK (sandbox_generation > 0),\n            reserved_mb INTEGER NOT NULL CHECK (reserved_mb >= 0)\n        ) STRICT";

/// Schema stamp, metadata row and this connection's data version in one
/// statement, so one snapshot answers all of them. A connection's data
/// version moves on every other connection's commit, never its own.
pub const STAMPED_METADATA: &str = "SELECT schema_version, application_id, user_version, journal_mode, m.activity_revision, m.runtime_compatibility_sha256, m.drain_json, data_version FROM pragma_schema_version, pragma_application_id, pragma_user_version, pragma_journal_mode, pragma_data_version JOIN registry_metadata AS m ON m.singleton = 1";
const SCHEMA_STAMP: &str = "SELECT schema_version, application_id, user_version, journal_mode FROM pragma_schema_version, pragma_application_id, pragma_user_version, pragma_journal_mode";

/// `(schema_version, application_id, user_version, journal_mode)`: equal
/// stamps prove the validated schema is unchanged.
pub type Stamp = (i64, i64, i64, String);

/// The metadata row: activity revision, bound compatibility, drain state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Metadata {
    pub activity_revision: i64,
    pub runtime_compatibility_sha256: Option<String>,
    pub drain: DrainState,
}

/// Each `;`-separated chunk of the DDL, stripped, as Python splits it.
fn statements() -> impl Iterator<Item = &'static str> {
    SCHEMA.split(';').map(str::trim).filter(|statement| !statement.is_empty())
}

/// `{name: statement}` for every CREATE, as `sqlite_schema` must hold it.
fn expected_schema(legacy_version: Option<i64>) -> BTreeMap<String, String> {
    let mut expected: BTreeMap<String, String> = statements()
        .filter(|statement| statement.starts_with("CREATE "))
        .map(|statement| (statement.split_whitespace().nth(2).unwrap_or_default().to_owned(), statement.to_owned()))
        .collect();
    match legacy_version {
        Some(8) => {
            expected.insert("registration_disk".into(), LEGACY_V8_REGISTRATION_DISK.into());
        }
        Some(_) => {
            expected.remove("registration_disk");
        }
        None => {}
    }
    if let Some(version) = legacy_version.filter(|version| *version < 7) {
        expected.remove("workspace_capacity");
        if version < 6 {
            expected.remove("reflink_overlaps");
        }
        if version < 5 {
            expected.remove("managed_growth");
        }
        if version == 3 {
            expected.remove("relay_wake_fences");
        }
    }
    expected
}

/// `_connect`'s connection: busy timeout 30 s, autocommit, FULL sync.
pub fn open(path: &Path) -> Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let connection = Connection::open_with_flags(path, flags)?;
    connection.busy_timeout(BUSY_TIMEOUT)?;
    connection.execute_batch("PRAGMA trusted_schema = OFF; PRAGMA synchronous = FULL")?;
    Ok(connection)
}

pub fn versions(connection: &Connection) -> Result<(i64, i64)> {
    Ok((
        connection.query_row("PRAGMA application_id", [], |row| row.get(0))?,
        connection.query_row("PRAGMA user_version", [], |row| row.get(0))?,
    ))
}

pub fn schema_stamp(connection: &Connection) -> Result<Stamp> {
    Ok(connection.query_row(SCHEMA_STAMP, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?)
}

fn journal_mode(connection: &Connection) -> Result<String> {
    Ok(connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?)
}

/// `_validate_schema`: identity, WAL, and exactly the expected objects.
pub fn validate_schema(connection: &Connection, legacy_version: Option<i64>) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT name, sql FROM sqlite_schema WHERE type IN ('table', 'index', 'view', 'trigger') AND name NOT LIKE 'sqlite_%'",
    )?;
    let actual = statement
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let expected = expected_schema(legacy_version);
    let same_objects = actual.len() == expected.len()
        && actual.iter().all(|(name, sql)| sql.as_ref().is_some_and(|sql| expected.get(name) == Some(sql)));
    if versions(connection)? != (APPLICATION_ID, legacy_version.unwrap_or(SCHEMA_VERSION))
        || journal_mode(connection)? != "wal"
        || !same_objects
    {
        return Err(RegistryError::registry("direct registry schema is invalid"));
    }
    metadata(connection).map(drop)
}

/// `_enable_wal`: retried while another connection holds the file.
fn enable_wal(connection: &Connection) -> Result<String> {
    let deadline = Instant::now() + BUSY_TIMEOUT;
    loop {
        match connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get::<_, String>(0)) {
            Ok(mode) => return Ok(mode),
            Err(error) => {
                let text = error.to_string().to_lowercase();
                if !(text.contains("busy") || text.contains("locked")) || Instant::now() >= deadline {
                    return Err(error.into());
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

/// `_ensure_schema`: create an empty file, migrate a legacy one, validate.
pub fn ensure_schema(connection: &Connection) -> Result<()> {
    if versions(connection)? == (APPLICATION_ID, SCHEMA_VERSION) {
        return validate_schema(connection, None);
    }
    if enable_wal(connection)? != "wal" {
        return Err(RegistryError::registry("direct registry cannot enable durable journaling"));
    }
    connection.execute_batch("BEGIN IMMEDIATE")?;
    let has_schema = connection
        .query_row("SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1", [], |_| Ok(()))
        .optional()?
        .is_some();
    let (application_id, version) = versions(connection)?;
    if application_id == APPLICATION_ID && LEGACY_VERSIONS.contains(&version) {
        migrate(connection, version)?;
    }
    if versions(connection)? == (0, 0) && !has_schema {
        for statement in statements() {
            connection.execute_batch(statement)?;
        }
        connection.execute_batch(&format!(
            "PRAGMA application_id = {APPLICATION_ID}; PRAGMA user_version = {SCHEMA_VERSION}"
        ))?;
    }
    validate_schema(connection, None)?;
    connection.execute_batch("COMMIT")?;
    Ok(())
}

/// Versions 3 to 8: create the missing tables and give every registration
/// a fixed disk claim (version 8's ledger is rebuilt with dynamic columns).
fn migrate(connection: &Connection, version: i64) -> Result<()> {
    validate_schema(connection, Some(version))?;
    if version == 8 {
        connection.execute_batch("DROP TABLE registration_disk")?;
    }
    let mut missing = vec!["registration_disk"];
    if version < 7 {
        missing.push("workspace_capacity");
    }
    if version < 6 {
        missing.push("reflink_overlaps");
    }
    if version < 5 {
        missing.push("managed_growth");
    }
    if version == 3 {
        missing.push("relay_wake_fences");
    }
    for statement in statements() {
        if statement.starts_with("CREATE TABLE ")
            && statement.split_whitespace().nth(2).is_some_and(|name| missing.contains(&name))
        {
            connection.execute_batch(statement)?;
        }
    }
    let rows = {
        let mut statement = connection.prepare("SELECT sandbox_id, image_id, record_json FROM registrations")?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, SqlValue>(0)?, row.get::<_, SqlValue>(1)?, row.get::<_, SqlValue>(2)?))
            })?
            .collect::<rusqlite::Result<Vec<(SqlValue, SqlValue, SqlValue)>>>()?
    };
    for (sandbox_id, image_id, encoded) in rows {
        let record = decode_row(ValueRef::from(&sandbox_id), ValueRef::from(&image_id), ValueRef::from(&encoded))?;
        write_disk_claim(connection, &record)?;
    }
    connection.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))?;
    Ok(())
}

/// `_decode` of a raw row: three TEXT values, then the record codec.
pub fn decode_row(sandbox_id: ValueRef<'_>, image_id: ValueRef<'_>, encoded: ValueRef<'_>) -> Result<Registration> {
    let text = |value: ValueRef<'_>| match value {
        ValueRef::Text(bytes) => std::str::from_utf8(bytes).ok().map(str::to_owned),
        _ => None,
    };
    match (text(sandbox_id), text(image_id), text(encoded)) {
        (Some(sandbox_id), Some(image_id), Some(encoded)) => Registration::decode(&sandbox_id, &image_id, &encoded),
        _ => Err(RegistryError::registry("direct registration row is invalid")),
    }
}

/// `_write_disk_claim`: refresh a fixed claim; dynamic rows keep theirs.
pub fn write_disk_claim(connection: &Connection, record: &Registration) -> Result<()> {
    let reserved_mb = match record.quota_total_mb {
        Some(total) => total,
        None => record.spec.requested_disk_mb().map_err(RegistryError::Invalid)?,
    };
    connection
        .prepare_cached(
            "INSERT INTO registration_disk VALUES (?, ?, ?, 0, 0, 0) ON CONFLICT (sandbox_id) DO UPDATE SET \
             sandbox_generation=excluded.sandbox_generation, reserved_mb=excluded.reserved_mb \
             WHERE registration_disk.dynamic = 0",
        )?
        .execute(rusqlite::params![record.sandbox_id(), record.sandbox_generation, reserved_mb])?;
    Ok(())
}

/// `_metadata`: the singleton row, validated.
pub fn metadata(connection: &Connection) -> Result<Metadata> {
    let row = connection
        .prepare_cached(
            "SELECT activity_revision, runtime_compatibility_sha256, drain_json FROM registry_metadata WHERE singleton = 1",
        )?
        .query_row([], |row| Ok((row.get::<_, SqlValue>(0)?, row.get::<_, SqlValue>(1)?, row.get::<_, SqlValue>(2)?)))
        .optional()?;
    checked_metadata(row.as_ref().map(|(a, b, c)| (ValueRef::from(a), ValueRef::from(b), ValueRef::from(c))))
}

/// `_checked_metadata`: a non-negative clock and a NULL or digest binding.
pub fn checked_metadata(row: Option<(ValueRef<'_>, ValueRef<'_>, ValueRef<'_>)>) -> Result<Metadata> {
    let invalid = || RegistryError::registry("direct registry metadata is invalid");
    let (revision, compatibility, drain) = row.ok_or_else(invalid)?;
    let activity_revision = match revision {
        ValueRef::Integer(value) if value >= 0 => value,
        _ => return Err(invalid()),
    };
    let runtime_compatibility_sha256 = match compatibility {
        ValueRef::Null => None,
        ValueRef::Text(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) if is_digest(text) => Some(text.to_owned()),
            _ => return Err(invalid()),
        },
        _ => return Err(invalid()),
    };
    let drain = match drain {
        ValueRef::Text(bytes) => DrainState::decode(std::str::from_utf8(bytes).map_err(|_| invalid())?)?,
        _ => return Err(invalid()),
    };
    Ok(Metadata { activity_revision, runtime_compatibility_sha256, drain })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pyjson::sha256_hex as sha256;

    #[test]
    fn ddl_matches_the_golden_hashes() {
        let golden = [
            ("registry_metadata", 544, "0bd20fb8f287949727168d1ef47bb50f5d23ec2da42fe3f26c5146481df6db7c"),
            ("registrations", 192, "d22d949caec7069ec5aba12dbedce7020155d969f3265b2a0ac618620d73a0aa"),
            ("generation_tombstones", 157, "90553d23f70768cece7e650adb1ac23d4d466fb673fd944bdaf019a741d5844d"),
            ("migration_tombstones", 181, "208894e00b2817f44a1ac36e6e4e414aaa36d38a03acf52086d6827bfff40f76"),
            ("registrations_image_id", 63, "8382bd85de18313c528c21f52795018bc94097fb72ece77b20c69fe9822d1927"),
            ("relay_wake_fences", 250, "2e3b53c42bf633298e228a269aa4b43c6203a58972b24c5b5dd994d4a644c272"),
            ("managed_growth", 428, "2120ccc6dffdfad2c99a53c415b8691378b1727367740279c71a4e0cbe96dd90"),
            ("reflink_overlaps", 579, "97966ccf70a1637c5e7d5af90fc1e0516c274178733cd843ef9ced905e46f053"),
            ("workspace_capacity", 304, "5e37129acb54c21ca9c0f905cd2ae903d10423f8826af21601035298febea276"),
            ("registration_disk", 432, "209e01cda8912f78fe2795638c0fa42bc100f2a6c10ba23645e46fcde7baf252"),
        ];
        let expected = expected_schema(None);
        assert_eq!(expected.len(), golden.len());
        for (name, bytes, digest) in golden {
            assert_eq!((expected[name].len(), sha256(&expected[name]).as_str()), (bytes, digest), "{name}");
        }
        let legacy = &expected_schema(Some(8))["registration_disk"];
        assert_eq!(
            (legacy.len(), sha256(legacy).as_str()),
            (236, "c32f3fa976b629e60c9fb66606c9416ecf9ded4a7a26e6740417942936c7165c")
        );
        assert_eq!(expected_schema(Some(3)).len(), 5);
    }

    #[test]
    fn a_fresh_file_stores_the_ddl_verbatim() {
        let dir = std::env::temp_dir().join(format!("noded-schema-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let connection = open(&dir.join("registry.sqlite")).unwrap();
        ensure_schema(&connection).unwrap();
        validate_schema(&connection, None).unwrap();
        let stamp = schema_stamp(&connection).unwrap();
        assert_eq!((stamp.1, stamp.2, stamp.3.as_str()), (APPLICATION_ID, SCHEMA_VERSION, "wal"));
        let meta = metadata(&connection).unwrap();
        assert_eq!(
            (meta.activity_revision, meta.runtime_compatibility_sha256, meta.drain),
            (0, None, DrainState::default())
        );
        assert_eq!(connection.query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0)).unwrap(), 2);
        assert_eq!(connection.query_row("PRAGMA trusted_schema", [], |row| row.get::<_, i64>(0)).unwrap(), 0);
        connection.execute_batch("CREATE TABLE extra (x)").unwrap();
        assert_eq!(validate_schema(&connection, None).unwrap_err().message(), "direct registry schema is invalid");
        drop(connection);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

//! Snapshot files: the authoritative state at a WAL position, atomically
//! persisted.
//!
//! A [`Snapshot`] captures `TableStore::snapshot_tables()` (per-table
//! authoritative state: rows, versions, `next_row_id`, epoch) plus the store
//! counters and the **WAL LSN** at which the snapshot was taken. Derived
//! indexes are not serialized — `Table::from_state` rebuilds them (ADR-005
//! D6).
//!
//! Files are written to a `*.tmp` sibling and **atomically renamed** into
//! place, and carry header/body CRCs, so a crash leaves either a stale but
//! valid older snapshot or a complete new one — never a torn file
//! (ADR-005 D4).

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use nexum_core::binary::{crc32, get_u64, put_u64};
use nexum_core::{Error, Result, Timestamp};
use nexum_storage::TableState;
use nexum_table::TableStore;

/// The four-byte magic that prefixes a valid snapshot file.
pub const SNAPSHOT_MAGIC: &[u8; 4] = b"NEXS";
/// The current snapshot format version.
pub const SNAPSHOT_VERSION: u32 = 1;
/// Snapshot file name prefix (`snapshot-<lsn>.snap`).
pub const SNAPSHOT_PREFIX: &str = "snapshot-";
/// Snapshot file name suffix.
pub const SNAPSHOT_SUFFIX: &str = ".snap";

/// A captured authoritative state at a WAL position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The WAL LSN covered by this snapshot: records at or after this offset
    /// must be replayed on top of it.
    pub lsn: u64,
    /// The store's `next_table_id` at capture time.
    pub next_table_id: u64,
    /// The store's `next_transaction_id` at capture time.
    pub next_transaction_id: u64,
    /// Wall-clock capture time (metadata only).
    pub created_at: Timestamp,
    /// The per-table authoritative states, ordered by `TableId`.
    pub tables: Vec<TableState>,
}

impl Snapshot {
    /// Captures the current authoritative state of `store` at WAL position
    /// `lsn` (use `wal.lsn()`).
    pub fn capture(store: &TableStore, lsn: u64) -> Snapshot {
        Snapshot {
            lsn,
            next_table_id: store.next_table_id(),
            next_transaction_id: store.next_transaction_id(),
            created_at: Timestamp::now(),
            tables: store.snapshot_tables(),
        }
    }

    /// Writes the snapshot to `dir` atomically (temp file + rename) and
    /// returns the final path.
    pub fn write(&self, dir: &Path) -> Result<PathBuf> {
        std::fs::create_dir_all(dir).map_err(|e| {
            Error::internal(format!(
                "snapshot: cannot create dir '{}': {e}",
                dir.display()
            ))
        })?;
        let final_path = dir.join(snapshot_file_name(self.lsn));
        let tmp_path = dir.join(format!("{SNAPSHOT_PREFIX}{}.tmp", self.lsn));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| {
                Error::internal(format!(
                    "snapshot: cannot create '{}': {e}",
                    tmp_path.display()
                ))
            })?;

        // Header (40 bytes) + header CRC.
        let mut header = Vec::with_capacity(40);
        header.extend_from_slice(SNAPSHOT_MAGIC);
        header.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        put_u64(&mut header, self.lsn);
        put_u64(&mut header, self.next_table_id);
        put_u64(&mut header, self.next_transaction_id);
        put_u64(&mut header, self.created_at.as_millis());
        let header_crc = crc32(&header);
        file.write_all(&header)
            .map_err(|e| map_io(e, "writing header"))?;
        file.write_all(&header_crc.to_le_bytes())
            .map_err(|e| map_io(e, "writing header checksum"))?;

        // Body: length-prefixed table states + body CRC.
        let mut body = Vec::new();
        for state in &self.tables {
            let mut bytes = Vec::new();
            state.encode(&mut bytes);
            body.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            body.extend_from_slice(&bytes);
        }
        let body_crc = crc32(&body);
        file.write_all(&(body.len() as u64).to_le_bytes())
            .map_err(|e| map_io(e, "writing body length"))?;
        file.write_all(&body).map_err(|e| map_io(e, "writing body"))?;
        file.write_all(&body_crc.to_le_bytes())
            .map_err(|e| map_io(e, "writing body checksum"))?;
        file.sync_all().map_err(|e| {
            Error::internal(format!("snapshot: fsync failed: {e}"))
        })?;
        drop(file);

        std::fs::rename(&tmp_path, &final_path).map_err(|e| {
            Error::internal(format!(
                "snapshot: cannot rename '{}' to '{}': {e}",
                tmp_path.display(),
                final_path.display()
            ))
        })?;
        Ok(final_path)
    }

    /// Reads and validates a snapshot file. A missing file, bad magic, or
    /// CRC failure is an [`Error::internal`] — the caller decides whether to
    /// skip an invalid snapshot.
    pub fn read(path: &Path) -> Result<Snapshot> {
        let bytes = std::fs::read(path).map_err(|e| {
            Error::internal(format!(
                "snapshot: cannot read '{}': {e}",
                path.display()
            ))
        })?;
        let mut cursor: &[u8] = &bytes;

        let magic = take(&mut cursor, 4)?;
        if magic != SNAPSHOT_MAGIC {
            return Err(Error::internal(format!(
                "snapshot: '{}' does not start with the NEXS magic",
                path.display()
            )));
        }
        let version = u32::from_le_bytes(take(&mut cursor, 4)?.try_into().expect("4 bytes"));
        if version != SNAPSHOT_VERSION {
            return Err(Error::internal(format!(
                "snapshot: '{}' uses unsupported format version {version}",
                path.display()
            )));
        }
        let lsn = get_u64(&mut cursor)?;
        let next_table_id = get_u64(&mut cursor)?;
        let next_transaction_id = get_u64(&mut cursor)?;
        let created_at = Timestamp::from_millis(get_u64(&mut cursor)?);

        // Verify the header CRC over the 40 bytes we just consumed.
        let header_crc = u32::from_le_bytes(take(&mut cursor, 4)?.try_into().expect("4 bytes"));
        if crc32(&bytes[..40]) != header_crc {
            return Err(Error::internal(format!(
                "snapshot: '{}' header checksum mismatch",
                path.display()
            )));
        }

        // Body.
        let body_len = get_u64(&mut cursor)? as usize;
        let body = take(&mut cursor, body_len)?;
        let body_crc = u32::from_le_bytes(take(&mut cursor, 4)?.try_into().expect("4 bytes"));
        if crc32(body) != body_crc {
            return Err(Error::internal(format!(
                "snapshot: '{}' body checksum mismatch",
                path.display()
            )));
        }

        // Tables.
        let mut tables = Vec::new();
        let mut body_cursor = body;
        while !body_cursor.is_empty() {
            let state_len = u32::from_le_bytes(
                take(&mut body_cursor, 4)?.try_into().expect("4 bytes"),
            ) as usize;
            let state_bytes = take(&mut body_cursor, state_len)?;
            let mut state_cursor = state_bytes;
            tables.push(TableState::decode(&mut state_cursor)?);
        }

        Ok(Snapshot {
            lsn,
            next_table_id,
            next_transaction_id,
            created_at,
            tables,
        })
    }

    /// Finds the newest valid snapshot in `dir` (highest LSN), ignoring
    /// unrelated files, temp files, and unreadable/corrupt snapshots.
    pub fn find_latest(dir: &Path) -> Result<Option<(PathBuf, Snapshot)>> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(_) => return Ok(None), // missing dir: no snapshots
        };
        let mut best: Option<(u64, PathBuf, Snapshot)> = None;
        for entry in entries {
            let entry = entry.map_err(|e| map_io(e, "reading snapshot directory"))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.starts_with(SNAPSHOT_PREFIX)
                || !name.ends_with(SNAPSHOT_SUFFIX)
                || name.ends_with(".tmp")
            {
                continue;
            }
            let Some(lsn_part) = name
                .strip_prefix(SNAPSHOT_PREFIX)
                .and_then(|rest| rest.strip_suffix(SNAPSHOT_SUFFIX))
            else {
                continue;
            };
            let Ok(lsn) = lsn_part.parse::<u64>() else {
                continue;
            };
            let path = entry.path();
            let Ok(snapshot) = Snapshot::read(&path) else {
                continue; // skip invalid snapshots; keep going
            };
            if best
                .as_ref()
                .is_none_or(|(best_lsn, _, _)| lsn > *best_lsn)
            {
                best = Some((lsn, path, snapshot));
            }
        }
        Ok(best.map(|(_, path, snapshot)| (path, snapshot)))
    }
}

/// Builds the snapshot file name for an LSN.
pub fn snapshot_file_name(lsn: u64) -> String {
    format!("{SNAPSHOT_PREFIX}{lsn}{SNAPSHOT_SUFFIX}")
}

/// Wraps an I/O error in [`Error::internal`] with a context label.
fn map_io(error: std::io::Error, what: &str) -> Error {
    Error::internal(format!("snapshot: {what}: {error}"))
}

/// Takes exactly `len` bytes, or fails as truncated input.
fn take<'a>(cursor: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if cursor.len() < len {
        return Err(Error::internal(format!(
            "snapshot: truncated input (needed {len} bytes, have {})",
            cursor.len()
        )));
    }
    let (head, tail) = cursor.split_at(len);
    *cursor = tail;
    Ok(head)
}

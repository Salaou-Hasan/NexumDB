//! The write-ahead log ([`Wal`]).
//!
//! The WAL attaches at the committed-change boundary: callers append the
//! exact `Vec<Change>` returned by `Transaction::commit`, framed as a
//! transaction group:
//!
//! ```text
//! BEGIN_TX (tx_id) → CHANGE × n → COMMIT_TX (tx_id, n)
//! ```
//!
//! Every record is checksummed (CRC-32) and length-framed, so an incomplete
//! or corrupted tail is detected and dropped — never guessed (ADR-005 D7).
//! Recovery keeps a group only when its `COMMIT_TX` is present and matching,
//! which is what makes multi-table commits atomic on disk (ADR-005 D3).
//!
//! Durability is explicit: `append` returns only after the configured policy
//! has been applied. `[DurabilityPolicy::Flush]` survives a process crash;
//! `[DurabilityPolicy::Sync]` (fsync) survives power loss and is the durable
//! mode. On a write/fsync failure `append` truncates back to the pre-append
//! offset so no partial record ever breaks future appends (ADR-005 D5).
//!
//! The log is append-only; `Wal::open` validates the existing records and
//! truncates any *physically* invalid tail so subsequent appends remain
//! recoverable. (An incomplete transaction *group* is left alone — recovery
//! drops it — because its records are individually valid.)

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use nexum_core::binary::{crc32, get_u64, put_u64};
use nexum_core::{ChangeKind, Error, Result, RowId, TableId, Timestamp, TransactionId};
use nexum_storage::Change;

/// The four-byte magic that prefixes a valid WAL file.
pub const HEADER_MAGIC: &[u8; 4] = b"NEXW";
/// The current WAL format version.
pub const FORMAT_VERSION: u32 = 1;
/// Header size in bytes: magic (4) + version (4) + created_at (8).
pub const HEADER_LEN: u64 = 16;

/// The durability policy applied to every append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityPolicy {
    /// Write the records to the OS. Survives a process crash, not power loss.
    Flush,
    /// Write the records and `sync_all` (fsync). Survives power loss — the
    /// durable mode.
    Sync,
}

/// A log sequence number: the byte offset of a record within the WAL file.
///
/// [`Wal::append`] returns the LSN of the transaction's `COMMIT_TX` record —
/// the durability point — and snapshots record `Wal::lsn()` (the offset where
/// the next record will be written) so recovery can skip records already
/// incorporated into the snapshot (ADR-005 D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn(u64);

impl Lsn {
    /// Creates an LSN from a raw byte offset.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw byte offset.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One committed transaction read back from the log, in log order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredTx {
    /// The transaction id.
    pub tx_id: TransactionId,
    /// The LSN of the transaction's `COMMIT_TX` record (its durability point).
    pub commit_lsn: Lsn,
    /// The committed changes, in commit order.
    pub changes: Vec<Change>,
}

/// The write-ahead log.
#[derive(Debug)]
pub struct Wal {
    file: File,
    path: PathBuf,
    next_lsn: u64,
    policy: DurabilityPolicy,
    /// Whether [`Wal::open`] found and truncated a physically invalid tail.
    /// Preserved so recovery can report it honestly even though the invalid
    /// bytes were already removed.
    truncated_on_open: bool,
}

/// Record kinds.
const KIND_BEGIN: u8 = 1;
const KIND_CHANGE: u8 = 2;
const KIND_COMMIT: u8 = 3;

/// The result of reading one record at an offset.
enum ReadOutcome {
    /// `(kind, payload, offset_of_next_record)`.
    Record(u8, Vec<u8>, u64),
    /// The file ends exactly at a record boundary — a clean end of log.
    End,
    /// Some bytes remain but not a full record — an incomplete tail.
    IncompleteTail,
    /// The checksum does not match — a corrupted record.
    Corrupted,
}

impl Wal {
    /// Creates a new (empty) log at `path`, writing a fresh header.
    pub fn create(path: &Path, policy: DurabilityPolicy) -> Result<Wal> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| {
                Error::internal(format!("wal: cannot create '{}': {e}", path.display()))
            })?;
        let mut wal = Wal {
            file,
            path: path.to_path_buf(),
            next_lsn: HEADER_LEN,
            policy,
            truncated_on_open: false,
        };
        wal.write_header()?;
        Ok(wal)
    }

    /// Opens an existing log for appending.
    ///
    /// Validates the header, scans the records, and **truncates any
    /// physically invalid tail** (short or checksum-failed records) so that
    /// subsequent appends are always recoverable. An incomplete transaction
    /// *group* is not physically invalid and is left in place (recovery drops
    /// it, since it never committed).
    ///
    /// Returns [`Error::not_found`] if the file does not exist.
    pub fn open(path: &Path, policy: DurabilityPolicy) -> Result<Wal> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| Error::not_found(format!("wal: cannot open '{}': {e}", path.display())))?;
        let len = file.metadata()?.len();
        if len == 0 {
            // Truncated-to-zero (or freshly created) log: write a header.
            let mut wal = Wal {
                file,
                path: path.to_path_buf(),
                next_lsn: HEADER_LEN,
                policy,
                truncated_on_open: false,
            };
            wal.write_header()?;
            return Ok(wal);
        }
        if len < HEADER_LEN {
            return Err(Error::internal(format!(
                "wal: '{}' is {len} bytes, shorter than the {HEADER_LEN}-byte header",
                path.display()
            )));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; HEADER_LEN as usize];
        file.read_exact(&mut header)?;
        if &header[0..4] != HEADER_MAGIC {
            return Err(Error::internal(format!(
                "wal: '{}' does not start with the NEXW magic",
                path.display()
            )));
        }
        let version = u32::from_le_bytes(header[4..8].try_into().expect("4 bytes"));
        if version != FORMAT_VERSION {
            return Err(Error::internal(format!(
                "wal: '{}' uses unsupported format version {version}",
                path.display()
            )));
        }

        // Scan records; the last valid boundary is where appends continue.
        let mut last_valid = HEADER_LEN;
        let mut offset = HEADER_LEN;
        while let ReadOutcome::Record(_, _, next) = read_record(&mut file, offset)? {
            offset = next;
            last_valid = next;
        }
        let truncated_on_open = last_valid != file.metadata()?.len();
        if truncated_on_open {
            file.set_len(last_valid)?;
            if policy == DurabilityPolicy::Sync {
                file.sync_all()?;
            }
        }
        Ok(Wal {
            file,
            path: path.to_path_buf(),
            next_lsn: last_valid,
            policy,
            truncated_on_open,
        })
    }

    /// Returns the log's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the current durability policy.
    pub fn policy(&self) -> DurabilityPolicy {
        self.policy
    }

    /// Returns `true` if [`Wal::open`] found and truncated a physically
    /// invalid tail (a crash artifact). The invalid bytes are already gone;
    /// this flag lets recovery report the drop honestly.
    pub fn truncated_on_open(&self) -> bool {
        self.truncated_on_open
    }

    /// Returns the LSN where the next record will be written.
    pub fn lsn(&self) -> Lsn {
        Lsn::from_u64(self.next_lsn)
    }

    /// Appends a committed transaction and applies the durability policy.
    ///
    /// The transaction is framed as `BEGIN_TX → CHANGE×n → COMMIT_TX` and
    /// written in one syscall, then flushed (and fsynced under
    /// `[DurabilityPolicy::Sync]`). Returns the LSN of the `COMMIT_TX`
    /// record — the point at which the transaction became durable.
    ///
    /// On any write/fsync failure the file is truncated back to the
    /// pre-append offset so no partial record is left behind, and the error
    /// is returned: the transaction is *committed in memory but not durable*.
    pub fn append(&mut self, tx_id: TransactionId, changes: &[Change]) -> Result<Lsn> {
        let start = self.next_lsn;
        let mut buf = Vec::new();
        let mut cursor = start;

        let mut begin_payload = Vec::new();
        put_u64(&mut begin_payload, tx_id.as_u64());
        cursor += write_record(&mut buf, KIND_BEGIN, &begin_payload) as u64;

        for change in changes {
            let mut payload = Vec::new();
            encode_change(&mut payload, change);
            cursor += write_record(&mut buf, KIND_CHANGE, &payload) as u64;
        }

        let commit_lsn = cursor;
        let mut commit_payload = Vec::new();
        put_u64(&mut commit_payload, tx_id.as_u64());
        put_u64(&mut commit_payload, changes.len() as u64);
        cursor += write_record(&mut buf, KIND_COMMIT, &commit_payload) as u64;

        let result = self.file.write_all(&buf).and_then(|()| self.apply_policy());
        if let Err(error) = result {
            // Never leave a partial frame behind.
            let _ = self.file.set_len(start);
            let _ = self.file.sync_all();
            return Err(Error::internal(format!(
                "wal: append failed (transaction {tx_id} is not durable): {error}"
            )));
        }
        self.next_lsn = cursor;
        Ok(Lsn::from_u64(commit_lsn))
    }

    /// Explicitly flushes buffered writes to the OS.
    pub fn flush(&mut self) -> Result<()> {
        self.file
            .flush()
            .map_err(|e| Error::internal(format!("wal: flush failed: {e}")))
    }

    /// Explicitly fsyncs the log to stable storage.
    pub fn sync(&mut self) -> Result<()> {
        self.file
            .sync_all()
            .map_err(|e| Error::internal(format!("wal: fsync failed: {e}")))
    }

    /// Reads every committed transaction back from the log, in log order.
    ///
    /// Returns `(transactions, truncated_tail)`. `truncated_tail` is `true`
    /// when the scan stopped early on an incomplete or corrupted record — the
    /// rest of the log is unrecoverable and must be discarded. An incomplete
    /// transaction group (missing `COMMIT_TX`) is dropped silently.
    ///
    /// Malformed *well-checksummed* content (a `CHANGE` outside a group, a
    /// mismatched commit framing) is an [`Error::internal`] — that is a bug
    /// in the writer, not a crash artifact.
    pub fn recover_changes(&mut self) -> Result<(Vec<RecoveredTx>, bool)> {
        self.file.seek(SeekFrom::Start(HEADER_LEN))?;
        let mut txs = Vec::new();
        let mut open: Option<(TransactionId, Vec<Change>)> = None;
        let mut offset = HEADER_LEN;
        let mut truncated = false;

        loop {
            match read_record(&mut self.file, offset)? {
                ReadOutcome::Record(kind, payload, next_offset) => {
                    match kind {
                        KIND_BEGIN => {
                            let tx_id = decode_tx_id(&payload)?;
                            // A new BEGIN with an open group means the previous
                            // group never committed — discard it.
                            open = Some((tx_id, Vec::new()));
                        }
                        KIND_CHANGE => {
                            let change = decode_change(&payload)?;
                            match &mut open {
                                Some((_, changes)) => changes.push(change),
                                None => {
                                    return Err(Error::internal(
                                        "wal: change record outside a transaction group",
                                    ));
                                }
                            }
                        }
                        KIND_COMMIT => {
                            let (tx_id, count) = decode_commit(&payload)?;
                            let (open_tx, changes) = open.take().ok_or_else(|| {
                                Error::internal("wal: commit record without a begin record")
                            })?;
                            if open_tx != tx_id || changes.len() as u64 != count {
                                return Err(Error::internal(format!(
                                    "wal: commit framing mismatch for transaction {tx_id}"
                                )));
                            }
                            txs.push(RecoveredTx {
                                tx_id,
                                commit_lsn: Lsn::from_u64(offset),
                                changes,
                            });
                        }
                        _ => {
                            return Err(Error::internal(format!(
                                "wal: unknown record kind {kind}"
                            )));
                        }
                    }
                    offset = next_offset;
                }
                ReadOutcome::End => break,
                ReadOutcome::IncompleteTail | ReadOutcome::Corrupted => {
                    truncated = true;
                    break;
                }
            }
        }
        Ok((txs, truncated))
    }

    fn write_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut header = Vec::with_capacity(HEADER_LEN as usize);
        header.extend_from_slice(HEADER_MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        put_u64(&mut header, Timestamp::now().as_millis());
        self.file.write_all(&header)?;
        self.apply_policy()?;
        Ok(())
    }

    fn apply_policy(&mut self) -> std::io::Result<()> {
        match self.policy {
            DurabilityPolicy::Flush => self.file.flush(),
            DurabilityPolicy::Sync => self.file.sync_all(),
        }
    }
}

/// Appends one framed, checksummed record; returns its total byte length.
fn write_record(out: &mut Vec<u8>, kind: u8, payload: &[u8]) -> usize {
    let record_len = 4 + 1 + payload.len() + 4;
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
    let mut crc_input = Vec::with_capacity(1 + payload.len());
    crc_input.push(kind);
    crc_input.extend_from_slice(payload);
    out.extend_from_slice(&crc32(&crc_input).to_le_bytes());
    record_len
}

/// Reads one record at `offset`; the offset of the next record is returned
/// with the record.
///
/// Distinguishes a clean end of log (exactly at a record boundary) from an
/// incomplete tail (some bytes but not a full record) — the latter is a
/// crash artifact and is flagged for truncation.
fn read_record(file: &mut File, offset: u64) -> Result<ReadOutcome> {
    file.seek(SeekFrom::Start(offset))?;

    let mut len_buf = [0u8; 4];
    let read = read_some(file, &mut len_buf)?;
    if read == 0 {
        return Ok(ReadOutcome::End);
    }
    if read < 4 {
        return Ok(ReadOutcome::IncompleteTail);
    }
    let payload_len = u32::from_le_bytes(len_buf) as usize;

    // Never trust a length field from disk before validating it against the
    // actual file size: a corrupt length must yield an incomplete-tail
    // verdict, not a multi-GiB allocation attempt (framing robustness,
    // ADR-005 D7). A record spans payload_len + 9 bytes: 4 length + 1 kind
    // + payload + 4 CRC.
    let remaining = file.metadata()?.len().saturating_sub(offset);
    if payload_len as u64 + 9 > remaining {
        return Ok(ReadOutcome::IncompleteTail);
    }

    let mut kind_buf = [0u8; 1];
    if read_some(file, &mut kind_buf)? < 1 {
        return Ok(ReadOutcome::IncompleteTail);
    }
    let mut payload = vec![0u8; payload_len];
    if read_some(file, &mut payload)? < payload_len {
        return Ok(ReadOutcome::IncompleteTail);
    }
    let mut crc_buf = [0u8; 4];
    if read_some(file, &mut crc_buf)? < 4 {
        return Ok(ReadOutcome::IncompleteTail);
    }

    let stored = u32::from_le_bytes(crc_buf);
    let mut crc_input = Vec::with_capacity(1 + payload_len);
    crc_input.push(kind_buf[0]);
    crc_input.extend_from_slice(&payload);
    if crc32(&crc_input) != stored {
        return Ok(ReadOutcome::Corrupted);
    }
    Ok(ReadOutcome::Record(
        kind_buf[0],
        payload,
        offset + 4 + 1 + payload_len as u64 + 4,
    ))
}

/// Reads up to `buf.len()` bytes; returns the number actually read.
/// Returns `0` only at a clean end of file.
fn read_some(file: &mut File, buf: &mut [u8]) -> Result<usize> {
    let mut read = 0;
    while read < buf.len() {
        match file.read(&mut buf[read..]) {
            Ok(0) => break,
            Ok(n) => read += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(Error::internal(format!("wal: read failed: {e}"))),
        }
    }
    Ok(read)
}

fn decode_tx_id(payload: &[u8]) -> Result<TransactionId> {
    let mut cursor = payload;
    Ok(TransactionId::from_u64(get_u64(&mut cursor)?))
}

fn decode_commit(payload: &[u8]) -> Result<(TransactionId, u64)> {
    let mut cursor = payload;
    let tx_id = TransactionId::from_u64(get_u64(&mut cursor)?);
    let count = get_u64(&mut cursor)?;
    Ok((tx_id, count))
}

/// Encodes a [`Change`] into a `CHANGE` record payload.
fn encode_change(out: &mut Vec<u8>, change: &Change) {
    put_u64(out, change.table_id().as_u64());
    out.push(change_kind_tag(change.kind()));
    put_u64(out, change.row_id().as_u64());
    put_opt_row(out, change.old_row());
    put_opt_row(out, change.new_row());
    put_opt_version(out, change.old_version());
    put_opt_version(out, change.new_version());
}

/// Decodes a `CHANGE` record payload back into a [`Change`].
fn decode_change(payload: &[u8]) -> Result<Change> {
    let mut cursor = payload;
    let table_id = TableId::from_u64(get_u64(&mut cursor)?);
    let kind = change_kind_from_tag(take_byte(&mut cursor)?)?;
    let row_id = RowId::from_u64(get_u64(&mut cursor)?);
    let old_row = get_opt_row(&mut cursor)?;
    let new_row = get_opt_row(&mut cursor)?;
    let old_version = get_opt_version(&mut cursor)?;
    let new_version = get_opt_version(&mut cursor)?;

    Ok(match kind {
        ChangeKind::Insert => Change::insert(
            table_id,
            row_id,
            new_row.ok_or_else(|| Error::internal("wal: insert change lacks a new row"))?,
            new_version.ok_or_else(|| Error::internal("wal: insert change lacks a new version"))?,
        ),
        ChangeKind::Update => Change::update(
            table_id,
            row_id,
            old_row.ok_or_else(|| Error::internal("wal: update change lacks an old row"))?,
            old_version
                .ok_or_else(|| Error::internal("wal: update change lacks an old version"))?,
            new_row.ok_or_else(|| Error::internal("wal: update change lacks a new row"))?,
            new_version.ok_or_else(|| Error::internal("wal: update change lacks a new version"))?,
        ),
        ChangeKind::Delete => Change::delete(
            table_id,
            row_id,
            old_row.ok_or_else(|| Error::internal("wal: delete change lacks an old row"))?,
            old_version
                .ok_or_else(|| Error::internal("wal: delete change lacks an old version"))?,
        ),
    })
}

fn put_opt_row(out: &mut Vec<u8>, row: Option<&nexum_core::Row>) {
    match row {
        Some(row) => {
            out.push(1);
            nexum_core::binary::put_row(out, row);
        }
        None => out.push(0),
    }
}

fn get_opt_row(cursor: &mut &[u8]) -> Result<Option<nexum_core::Row>> {
    match take_byte(cursor)? {
        0 => Ok(None),
        1 => Ok(Some(nexum_core::binary::get_row(cursor)?)),
        other => Err(Error::internal(format!(
            "wal: invalid optional-row flag {other}"
        ))),
    }
}

fn put_opt_version(out: &mut Vec<u8>, version: Option<nexum_core::Version>) {
    match version {
        Some(version) => {
            out.push(1);
            put_u64(out, version.as_u64());
        }
        None => out.push(0),
    }
}

fn get_opt_version(cursor: &mut &[u8]) -> Result<Option<nexum_core::Version>> {
    match take_byte(cursor)? {
        0 => Ok(None),
        1 => Ok(Some(nexum_core::Version::from_u64(get_u64(cursor)?))),
        other => Err(Error::internal(format!(
            "wal: invalid optional-version flag {other}"
        ))),
    }
}

fn take_byte(cursor: &mut &[u8]) -> Result<u8> {
    if cursor.is_empty() {
        return Err(Error::internal("wal: unexpected end of payload"));
    }
    let (head, tail) = cursor.split_at(1);
    *cursor = tail;
    Ok(head[0])
}

const fn change_kind_tag(kind: ChangeKind) -> u8 {
    match kind {
        ChangeKind::Insert => 1,
        ChangeKind::Update => 2,
        ChangeKind::Delete => 3,
    }
}

fn change_kind_from_tag(tag: u8) -> Result<ChangeKind> {
    Ok(match tag {
        1 => ChangeKind::Insert,
        2 => ChangeKind::Update,
        3 => ChangeKind::Delete,
        _ => return Err(Error::internal(format!("wal: unknown change kind {tag}"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::Version;
    use nexum_core::row;

    fn sample_changes() -> Vec<Change> {
        vec![
            Change::insert(
                TableId::from_u64(0),
                RowId::from_u64(3),
                row![1u64, 10u64, 100i32],
                Version::ZERO,
            ),
            Change::update(
                TableId::from_u64(0),
                RowId::from_u64(1),
                row![1u64, 10u64, 100i32],
                Version::ZERO,
                row![1u64, 10u64, 50i32],
                Version::from_u64(1),
            ),
            Change::delete(
                TableId::from_u64(1),
                RowId::from_u64(7),
                row![2u64, "gone".to_string()],
                Version::from_u64(4),
            ),
        ]
    }

    #[test]
    fn change_encoding_roundtrips() {
        let changes = sample_changes();
        for change in &changes {
            let mut payload = Vec::new();
            encode_change(&mut payload, change);
            let decoded = decode_change(&payload).unwrap();
            assert_eq!(&decoded, change);
        }
    }

    #[test]
    fn lsn_displays_and_orders() {
        let a = Lsn::from_u64(10);
        let b = Lsn::from_u64(20);
        assert!(a < b);
        assert_eq!(format!("{a}"), "10");
        assert_eq!(b.as_u64(), 20);
    }
}

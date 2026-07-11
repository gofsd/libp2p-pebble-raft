//! SQLite-backed durable storage for the raft learner: the replicated log,
//! `currentTerm`/snapshot bookkeeping, and the KV table itself -- the wasm
//! analog of `pkg/store` (same `kv` table shape) plus what `pkg/daemon`
//! gets for free from `raft-boltdb`/`raft.FileSnapshotStore` on desktop.
//!
//! Built on `sqlite-wasm-rs`, which compiles the real SQLite C amalgamation
//! to `wasm32-unknown-unknown` and exposes the standard `libsqlite3` C API
//! directly (see its README) -- there is no higher-level query builder, so
//! this module hand-rolls the handful of prepared statements the learner
//! needs. Runs against the in-memory VFS by default; `open` accepts any
//! `sqlite-wasm-rs`-registered VFS name (e.g. its OPFS `sahpool` VFS, for
//! persistence across page reloads in the Worker that owns this store --
//! see `web-app/README.md`).
#![cfg(target_arch = "wasm32")]

use std::ffi::{c_void, CString};
use std::ptr;

use sqlite_wasm_rs as ffi;

pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sqlite_store: {}", self.0)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// `SQLITE_TRANSIENT`: not bindgen-generated (it's the C macro
/// `((sqlite3_destructor_type)-1)`, not a real symbol), so every bind call
/// below tells SQLite to copy the bytes immediately rather than assume our
/// Rust buffer outlives the statement.
fn sqlite_transient() -> ffi::sqlite3_destructor_type {
    unsafe { std::mem::transmute::<isize, ffi::sqlite3_destructor_type>(-1) }
}

fn check(db: *mut ffi::sqlite3, rc: i32) -> Result<()> {
    if rc == ffi::SQLITE_OK {
        return Ok(());
    }
    let msg = unsafe {
        let p = ffi::sqlite3_errmsg(db);
        if p.is_null() {
            format!("sqlite error {rc}")
        } else {
            std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
        }
    };
    Err(Error(msg))
}

struct Stmt {
    db: *mut ffi::sqlite3,
    ptr: *mut ffi::sqlite3_stmt,
}

impl Stmt {
    fn prepare(db: *mut ffi::sqlite3, sql: &str) -> Result<Self> {
        let csql = CString::new(sql).unwrap();
        let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(db, csql.as_ptr(), -1, &mut stmt as *mut _, ptr::null_mut())
        };
        check(db, rc)?;
        Ok(Stmt { db, ptr: stmt })
    }

    fn bind_blob(&self, idx: i32, data: &[u8]) -> Result<()> {
        let rc = unsafe {
            ffi::sqlite3_bind_blob(
                self.ptr,
                idx,
                data.as_ptr() as *const c_void,
                data.len() as i32,
                sqlite_transient(),
            )
        };
        check(self.db, rc)
    }

    fn bind_i64(&self, idx: i32, v: i64) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_bind_int64(self.ptr, idx, v) };
        check(self.db, rc)
    }

    fn bind_blob_opt(&self, idx: i32, data: Option<&[u8]>) -> Result<()> {
        match data {
            Some(d) => self.bind_blob(idx, d),
            None => {
                let rc = unsafe { ffi::sqlite3_bind_null(self.ptr, idx) };
                check(self.db, rc)
            }
        }
    }

    fn column_blob_opt(&self, col: i32) -> Option<Vec<u8>> {
        let b = self.column_blob(col);
        if b.is_empty() && unsafe { ffi::sqlite3_column_type(self.ptr, col) } == ffi::SQLITE_NULL {
            None
        } else {
            Some(b)
        }
    }

    /// Steps a statement expected to produce no rows (INSERT/UPDATE/DELETE).
    fn exec(&self) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_step(self.ptr) };
        if rc != ffi::SQLITE_DONE {
            return Err(Error(format!("step: unexpected code {rc}")));
        }
        Ok(())
    }

    /// Steps once; `Some(true)` if a row is available, `Some(false)` at
    /// end-of-results.
    fn step_row(&self) -> Result<bool> {
        let rc = unsafe { ffi::sqlite3_step(self.ptr) };
        match rc {
            _ if rc == ffi::SQLITE_ROW => Ok(true),
            _ if rc == ffi::SQLITE_DONE => Ok(false),
            _ => Err(Error(format!("step: unexpected code {rc}"))),
        }
    }

    fn column_blob(&self, col: i32) -> Vec<u8> {
        unsafe {
            let p = ffi::sqlite3_column_blob(self.ptr, col);
            let n = ffi::sqlite3_column_bytes(self.ptr, col) as usize;
            if p.is_null() || n == 0 {
                Vec::new()
            } else {
                std::slice::from_raw_parts(p as *const u8, n).to_vec()
            }
        }
    }

    fn column_i64(&self, col: i32) -> i64 {
        unsafe { ffi::sqlite3_column_int64(self.ptr, col) }
    }
}

impl Drop for Stmt {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_finalize(self.ptr);
        }
    }
}

pub struct SqliteStore {
    db: *mut ffi::sqlite3,
}

// A wasm module instance is single-threaded (see shmring's README, which
// makes the same argument for its own wasm backend); this store is only
// ever touched from the Worker that owns it.
unsafe impl Send for SqliteStore {}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS raft_log (idx INTEGER PRIMARY KEY, term INTEGER NOT NULL, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS raft_state (
    id INTEGER PRIMARY KEY CHECK (id = 0),
    current_term INTEGER NOT NULL DEFAULT 0,
    last_included_index INTEGER NOT NULL DEFAULT 0,
    last_included_term INTEGER NOT NULL DEFAULT 0,
    commit_index INTEGER NOT NULL DEFAULT 0,
    last_applied INTEGER NOT NULL DEFAULT 0,
    voted_for BLOB
);
INSERT OR IGNORE INTO raft_state (id) VALUES (0);
";

impl SqliteStore {
    /// Opens (creating if necessary) a database at `filename` through
    /// `vfs` (pass `None` for SQLite's default -- `sqlite-wasm-rs`
    /// registers its in-memory VFS as that default; pass `Some("opfs-sahpool")`,
    /// once that VFS has been installed via `sqlite-wasm-vfs`, for
    /// persistence across reloads in a Worker).
    pub fn open(filename: &str, vfs: Option<&str>) -> Result<Self> {
        let cfilename = CString::new(filename).unwrap();
        let cvfs = vfs.map(|v| CString::new(v).unwrap());
        let mut db: *mut ffi::sqlite3 = ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                cfilename.as_ptr(),
                &mut db as *mut _,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                cvfs.as_ref().map_or(ptr::null(), |c| c.as_ptr()),
            )
        };
        if rc != ffi::SQLITE_OK || db.is_null() {
            return Err(Error(format!("open: code {rc}")));
        }
        let store = SqliteStore { db };
        store.exec_batch(SCHEMA)?;
        Ok(store)
    }

    fn exec_batch(&self, sql: &str) -> Result<()> {
        let csql = CString::new(sql).unwrap();
        let rc = unsafe {
            ffi::sqlite3_exec(
                self.db,
                csql.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        check(self.db, rc)
    }

    // --- kv table (mirrors pkg/store.Store) ---

    pub fn kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let stmt = Stmt::prepare(self.db, "SELECT value FROM kv WHERE key = ?1")?;
        stmt.bind_blob(1, key)?;
        if stmt.step_row()? {
            Ok(Some(stmt.column_blob(0)))
        } else {
            Ok(None)
        }
    }

    pub fn kv_set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let stmt = Stmt::prepare(
            self.db,
            "INSERT INTO kv(key, value) VALUES(?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )?;
        stmt.bind_blob(1, key)?;
        stmt.bind_blob(2, value)?;
        stmt.exec()
    }

    pub fn kv_delete(&self, key: &[u8]) -> Result<()> {
        let stmt = Stmt::prepare(self.db, "DELETE FROM kv WHERE key = ?1")?;
        stmt.bind_blob(1, key)?;
        stmt.exec()
    }

    /// Replaces the entire kv table's contents -- used by InstallSnapshot
    /// restore, matching `pkg/store.LoadAll`'s "delete everything, then
    /// reinsert" semantics.
    pub fn kv_replace_all(&self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        self.exec_batch("BEGIN")?;
        let del = Stmt::prepare(self.db, "DELETE FROM kv")?;
        del.exec()?;
        drop(del);
        for (k, v) in entries {
            let stmt = Stmt::prepare(self.db, "INSERT INTO kv(key, value) VALUES(?1, ?2)")?;
            stmt.bind_blob(1, k)?;
            stmt.bind_blob(2, v)?;
            stmt.exec()?;
        }
        self.exec_batch("COMMIT")
    }

    // --- raft_log table ---

    pub fn log_append(&self, index: u64, term: u64, data: &[u8]) -> Result<()> {
        let stmt = Stmt::prepare(
            self.db,
            "INSERT INTO raft_log(idx, term, data) VALUES(?1, ?2, ?3) \
             ON CONFLICT(idx) DO UPDATE SET term = excluded.term, data = excluded.data",
        )?;
        stmt.bind_i64(1, index as i64)?;
        stmt.bind_i64(2, term as i64)?;
        stmt.bind_blob(3, data)?;
        stmt.exec()
    }

    pub fn log_get(&self, index: u64) -> Result<Option<(u64, Vec<u8>)>> {
        let stmt = Stmt::prepare(self.db, "SELECT term, data FROM raft_log WHERE idx = ?1")?;
        stmt.bind_i64(1, index as i64)?;
        if stmt.step_row()? {
            Ok(Some((stmt.column_i64(0) as u64, stmt.column_blob(1))))
        } else {
            Ok(None)
        }
    }

    pub fn log_get_term(&self, index: u64) -> Result<Option<u64>> {
        Ok(self.log_get(index)?.map(|(term, _)| term))
    }

    /// Deletes every log entry with index >= `from` -- used to drop a
    /// conflicting suffix on an AppendEntries consistency-check failure,
    /// matching `raft.Raft`'s own leader-side log reconciliation.
    pub fn log_truncate_from(&self, from: u64) -> Result<()> {
        let stmt = Stmt::prepare(self.db, "DELETE FROM raft_log WHERE idx >= ?1")?;
        stmt.bind_i64(1, from as i64)?;
        stmt.exec()
    }

    /// Deletes every log entry with index <= `through` -- used after an
    /// InstallSnapshot restore, whose contents supersede any log prefix up
    /// to and including `LastLogIndex`.
    pub fn log_truncate_through(&self, through: u64) -> Result<()> {
        let stmt = Stmt::prepare(self.db, "DELETE FROM raft_log WHERE idx <= ?1")?;
        stmt.bind_i64(1, through as i64)?;
        stmt.exec()
    }

    pub fn log_last_index(&self) -> Result<u64> {
        let stmt = Stmt::prepare(self.db, "SELECT COALESCE(MAX(idx), 0) FROM raft_log")?;
        stmt.step_row()?;
        Ok(stmt.column_i64(0) as u64)
    }

    // --- raft_state (currentTerm + snapshot bookkeeping) ---

    pub fn current_term(&self) -> Result<u64> {
        let stmt = Stmt::prepare(self.db, "SELECT current_term FROM raft_state WHERE id = 0")?;
        stmt.step_row()?;
        Ok(stmt.column_i64(0) as u64)
    }

    pub fn set_current_term(&self, term: u64) -> Result<()> {
        let stmt = Stmt::prepare(
            self.db,
            "UPDATE raft_state SET current_term = ?1 WHERE id = 0",
        )?;
        stmt.bind_i64(1, term as i64)?;
        stmt.exec()
    }

    pub fn last_included(&self) -> Result<(u64, u64)> {
        let stmt = Stmt::prepare(
            self.db,
            "SELECT last_included_index, last_included_term FROM raft_state WHERE id = 0",
        )?;
        stmt.step_row()?;
        Ok((stmt.column_i64(0) as u64, stmt.column_i64(1) as u64))
    }

    pub fn set_last_included(&self, index: u64, term: u64) -> Result<()> {
        let stmt = Stmt::prepare(
            self.db,
            "UPDATE raft_state SET last_included_index = ?1, last_included_term = ?2 WHERE id = 0",
        )?;
        stmt.bind_i64(1, index as i64)?;
        stmt.bind_i64(2, term as i64)?;
        stmt.exec()
    }

    pub fn commit_index(&self) -> Result<u64> {
        let stmt = Stmt::prepare(self.db, "SELECT commit_index FROM raft_state WHERE id = 0")?;
        stmt.step_row()?;
        Ok(stmt.column_i64(0) as u64)
    }

    pub fn set_commit_index(&self, index: u64) -> Result<()> {
        let stmt = Stmt::prepare(
            self.db,
            "UPDATE raft_state SET commit_index = ?1 WHERE id = 0",
        )?;
        stmt.bind_i64(1, index as i64)?;
        stmt.exec()
    }

    pub fn last_applied(&self) -> Result<u64> {
        let stmt = Stmt::prepare(self.db, "SELECT last_applied FROM raft_state WHERE id = 0")?;
        stmt.step_row()?;
        Ok(stmt.column_i64(0) as u64)
    }

    pub fn set_last_applied(&self, index: u64) -> Result<()> {
        let stmt = Stmt::prepare(
            self.db,
            "UPDATE raft_state SET last_applied = ?1 WHERE id = 0",
        )?;
        stmt.bind_i64(1, index as i64)?;
        stmt.exec()
    }

    pub fn voted_for(&self) -> Result<Option<Vec<u8>>> {
        let stmt = Stmt::prepare(self.db, "SELECT voted_for FROM raft_state WHERE id = 0")?;
        stmt.step_row()?;
        Ok(stmt.column_blob_opt(0))
    }

    /// `None` clears the vote (called when the term advances -- a fresh
    /// term has cast no vote yet).
    pub fn set_voted_for(&self, candidate_id: Option<&[u8]>) -> Result<()> {
        let stmt = Stmt::prepare(self.db, "UPDATE raft_state SET voted_for = ?1 WHERE id = 0")?;
        stmt.bind_blob_opt(1, candidate_id)?;
        stmt.exec()
    }
}

impl Drop for SqliteStore {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_close(self.db);
        }
    }
}

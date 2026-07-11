//! The raft learner's decision logic: what to do with a decoded
//! [`crate::raft_wire`] RPC, backed by [`crate::sqlite_store::SqliteStore`].
//! This is a real (if minimal) `hashicorp/raft`-compatible follower state
//! machine restricted to what a non-voter needs -- it never campaigns
//! (no candidate/leader states, no election timer), but AppendEntries,
//! RequestVote/RequestPreVote, and InstallSnapshot are handled the same
//! way `raft.Raft`'s own RPC handlers in `raft.go` would: term checks, the
//! `PrevLogEntry`/`PrevLogTerm` consistency check with conflicting-suffix
//! truncation, commit-index advancement, and in-order FSM apply via
//! [`crate::fsm`].
#![cfg(target_arch = "wasm32")]

use crate::fsm;
use crate::raft_wire::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestPreVoteRequest, RequestPreVoteResponse, RequestVoteRequest, RequestVoteResponse,
    RpcHeader, TimeoutNowRequest, TimeoutNowResponse, PROTOCOL_VERSION,
};
use crate::sqlite_store::{Result, SqliteStore};

pub struct Learner {
    store: SqliteStore,
    self_id: Vec<u8>,
    self_addr: Vec<u8>,
}

impl Learner {
    pub fn new(store: SqliteStore, self_id: Vec<u8>, self_addr: Vec<u8>) -> Self {
        Learner {
            store,
            self_id,
            self_addr,
        }
    }

    fn my_header(&self) -> RpcHeader {
        RpcHeader {
            protocol_version: PROTOCOL_VERSION,
            id: Some(self.self_id.clone()),
            addr: Some(self.self_addr.clone()),
        }
    }

    fn last_log_index_term(&self) -> Result<(u64, u64)> {
        let last_index = self.store.log_last_index()?;
        if last_index == 0 {
            let (li, lt) = self.store.last_included()?;
            Ok((li, lt))
        } else {
            let term = self.store.log_get_term(last_index)?.unwrap_or(0);
            Ok((last_index, term))
        }
    }

    /// Applies every committed-but-unapplied log entry to the FSM (the kv
    /// table), in order -- the local mirror of `raft.Raft`'s FSM-apply
    /// loop, and what makes `Get` observe a Set once it's replicated here.
    fn apply_committed(&self) -> Result<()> {
        let commit_index = self.store.commit_index()?;
        let (last_included_index, _) = self.store.last_included()?;
        let mut last_applied = self.store.last_applied()?.max(last_included_index);
        while last_applied < commit_index {
            let next = last_applied + 1;
            if let Some((_, data)) = self.store.log_get(next)? {
                if let Ok(cmd) = fsm::decode_command(&data) {
                    match cmd.op {
                        fsm::Op::Set => self.store.kv_set(&cmd.key, &cmd.value)?,
                        fsm::Op::Del => self.store.kv_delete(&cmd.key)?,
                    }
                }
                // A command that fails to decode is skipped, not fatal --
                // matches pkg/kvfsm.FSM.Apply, which returns an
                // ApplyResult.Err for one bad entry without aborting raft.
            }
            last_applied = next;
            self.store.set_last_applied(last_applied)?;
        }
        Ok(())
    }

    pub fn handle_append_entries(
        &self,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let reject = |term: u64, last_log: u64| AppendEntriesResponse {
            header: self.my_header(),
            term,
            last_log,
            success: false,
            no_retry_backoff: false,
        };

        let mut current_term = self.store.current_term()?;
        if req.term < current_term {
            return Ok(reject(current_term, self.store.log_last_index()?));
        }
        if req.term > current_term {
            self.store.set_current_term(req.term)?;
            self.store.set_voted_for(None)?;
            current_term = req.term;
        }

        let (last_included_index, last_included_term) = self.store.last_included()?;

        // PrevLogEntry/PrevLogTerm consistency check (raft.go's
        // appendEntries): 0 means "start of log", always consistent.
        if req.prev_log_entry > 0 {
            let prev_term = if req.prev_log_entry == last_included_index {
                Some(last_included_term)
            } else {
                self.store.log_get_term(req.prev_log_entry)?
            };
            match prev_term {
                None => return Ok(reject(current_term, self.store.log_last_index()?)),
                Some(t) if t != req.prev_log_term => {
                    // Conflicting entry at this index: our suffix from here
                    // on cannot be trusted, so drop it and let the leader
                    // retry further back.
                    self.store.log_truncate_from(req.prev_log_entry)?;
                    return Ok(reject(current_term, self.store.log_last_index()?));
                }
                _ => {}
            }
        }

        let mut last_new_index = req.prev_log_entry.max(last_included_index);
        for entry in &req.entries {
            if entry.index <= last_included_index {
                // Already compacted into a snapshot; nothing to do.
                last_new_index = last_new_index.max(entry.index);
                continue;
            }
            match self.store.log_get_term(entry.index)? {
                Some(existing_term) if existing_term == entry.term => {
                    // Identical entry already present -- no-op, matching
                    // raft.go's "skip if already have this exact entry".
                }
                Some(_) => {
                    // Conflicting entry at this index: everything from here
                    // on is invalid and must be replaced.
                    self.store.log_truncate_from(entry.index)?;
                    self.store.log_append(
                        entry.index,
                        entry.term,
                        entry.data.as_deref().unwrap_or(&[]),
                    )?;
                }
                None => {
                    self.store.log_append(
                        entry.index,
                        entry.term,
                        entry.data.as_deref().unwrap_or(&[]),
                    )?;
                }
            }
            last_new_index = entry.index;
        }

        if req.leader_commit_index > self.store.commit_index()? {
            let new_commit = req.leader_commit_index.min(last_new_index);
            self.store.set_commit_index(new_commit)?;
        }
        self.apply_committed()?;

        Ok(AppendEntriesResponse {
            header: self.my_header(),
            term: current_term,
            last_log: self.store.log_last_index()?,
            success: true,
            no_retry_backoff: false,
        })
    }

    pub fn handle_request_vote(&self, req: &RequestVoteRequest) -> Result<RequestVoteResponse> {
        let mut current_term = self.store.current_term()?;
        if req.term < current_term {
            return Ok(RequestVoteResponse {
                header: self.my_header(),
                term: current_term,
                peers: None,
                granted: false,
            });
        }
        if req.term > current_term {
            self.store.set_current_term(req.term)?;
            self.store.set_voted_for(None)?;
            current_term = req.term;
        }

        let (last_index, last_term) = self.last_log_index_term()?;
        let candidate_up_to_date = req.last_log_term > last_term
            || (req.last_log_term == last_term && req.last_log_index >= last_index);

        let candidate_id = req.header.id.clone().or_else(|| req.candidate.clone());
        let voted_for = self.store.voted_for()?;
        let can_vote = match &voted_for {
            None => true,
            Some(v) => Some(v.clone()) == candidate_id,
        };

        let granted = can_vote && candidate_up_to_date;
        if granted {
            self.store.set_voted_for(candidate_id.as_deref())?;
        }

        Ok(RequestVoteResponse {
            header: self.my_header(),
            term: current_term,
            peers: None,
            granted,
        })
    }

    /// Pre-vote (protocol version 3+, `raft.go`'s `electSelf` optimization
    /// pre-check): unlike a real vote, this never persists `voted_for` --
    /// its entire purpose is to let a candidate probe whether it *would*
    /// win an election without perturbing anyone's term/vote state, so a
    /// partitioned node's futile campaigning can't disrupt real leadership.
    pub fn handle_request_pre_vote(
        &self,
        req: &RequestPreVoteRequest,
    ) -> Result<RequestPreVoteResponse> {
        let current_term = self.store.current_term()?;
        if req.term < current_term {
            return Ok(RequestPreVoteResponse {
                header: self.my_header(),
                term: current_term,
                granted: false,
            });
        }
        let (last_index, last_term) = self.last_log_index_term()?;
        let granted = req.last_log_term > last_term
            || (req.last_log_term == last_term && req.last_log_index >= last_index);
        Ok(RequestPreVoteResponse {
            header: self.my_header(),
            term: current_term.max(req.term),
            granted,
        })
    }

    /// Restores the kv table wholesale from `body`, the raw
    /// `pkg/store.DumpAll`-formatted stream that follows `req`'s msgpack
    /// header on the wire (`req.size` bytes -- see
    /// `raft_wire::InstallSnapshotRequest::size`'s doc comment; the caller
    /// reading off the real libp2p stream is responsible for reading
    /// exactly that many bytes into `body`).
    pub fn handle_install_snapshot(
        &self,
        req: &InstallSnapshotRequest,
        body: &[u8],
    ) -> Result<InstallSnapshotResponse> {
        let entries = match fsm::decode_snapshot_entries(body) {
            Ok(e) => e,
            Err(_) => {
                return Ok(InstallSnapshotResponse {
                    header: self.my_header(),
                    term: self.store.current_term()?,
                    success: false,
                })
            }
        };

        self.store.kv_replace_all(&entries)?;
        self.store.log_truncate_through(req.last_log_index)?;
        self.store
            .set_last_included(req.last_log_index, req.last_log_term)?;

        let mut current_term = self.store.current_term()?;
        if req.term > current_term {
            self.store.set_current_term(req.term)?;
            current_term = req.term;
        }
        if req.last_log_index > self.store.commit_index()? {
            self.store.set_commit_index(req.last_log_index)?;
        }
        // The snapshot already reflects application up through its own
        // last-included index -- matches pkg/kvfsm.FSM.Restore, which
        // replaces the store wholesale rather than replaying a log prefix.
        self.store.set_last_applied(req.last_log_index)?;

        Ok(InstallSnapshotResponse {
            header: self.my_header(),
            term: current_term,
            success: true,
        })
    }

    /// A learner never campaigns (see the module doc comment), so there is
    /// no election to start -- just acknowledge, matching how a real
    /// `raft.Raft` in a role other than follower-about-to-campaign would
    /// have nothing useful to do with this RPC either.
    pub fn handle_timeout_now(&self, _req: &TimeoutNowRequest) -> Result<TimeoutNowResponse> {
        Ok(TimeoutNowResponse {
            header: self.my_header(),
        })
    }

    /// This tab's own locally replicated read -- may lag a moment behind a
    /// Set that just committed on the leader, the same caveat any raft
    /// follower's local read already carries (see `pkg/daemon.handleGet`).
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.store.kv_get(key)
    }
}

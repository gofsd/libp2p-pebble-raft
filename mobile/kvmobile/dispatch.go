package kvmobile

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

// This file ties the QR-scan -> catalog -> dispatch -> execution-log flow
// together: ResolveQRGroup turns a scanned QR payload into the group and
// its available commands (catalog.go), SubmitCommand dispatches one as a
// durable, replicated request plus a low-latency Execute poke to whoever
// executes it, and WatchCommandLog/QueryCommandLog read back the
// execution log the target device writes with AppendCommandLog as it
// works. Like catalog.go, every operation here is a plain EventGet/
// EventLogAppend/EventListRange/EventExecute call -- no new capnp wire
// schema.
//
// kvmobile only dispatches and records; it never interprets or runs a
// Command itself -- that's the target device's own application logic,
// watching for requests (WatchExecute, or ListCommandRequests as a
// catch-up fallback -- see SubmitCommand's doc comment on why Execute
// delivery alone isn't reliable enough to be the only path) and reporting
// back via AppendCommandLog.

// qrGroupPayload is the JSON a scanned QR code should decode to --
// deliberately minimal (just the group id), not a raw capnp
// pkg/shmevent.Msg: that's this daemon's internal wire format between a
// caller and its own node, the wrong tool for a printed/displayed QR
// code.
type qrGroupPayload struct {
	GroupID string `json:"group_id"`
}

// GroupView bundles a Group with its currently available Commands --
// ResolveQRGroup's result shape, so the calling UI can render the command
// list screen from one call instead of GetGroup + ListCommands.
type GroupView struct {
	Group    Group     `json:"group"`
	Commands []Command `json:"commands"`
}

// ResolveQRGroup decodes qrPayloadJSON (see qrGroupPayload), checks the
// caller is a participant of the named group, and returns a GroupView --
// the group's definition plus its current command list -- as one JSON
// object. Returns an error if the payload is malformed, the group doesn't
// exist, or the caller isn't a participant of it.
func ResolveQRGroup(qrPayloadJSON string) (string, error) {
	var payload qrGroupPayload
	if err := json.Unmarshal([]byte(qrPayloadJSON), &payload); err != nil {
		return "", fmt.Errorf("kvmobile: decode QR payload: %w", err)
	}
	if payload.GroupID == "" {
		return "", fmt.Errorf("kvmobile: QR payload missing group_id")
	}

	// Checked explicitly (rather than just letting ListCommands's own
	// check below surface it) so a non-participant gets a clear error
	// immediately instead of GetGroup succeeding first and being thrown
	// away.
	if err := requireGroupParticipant(payload.GroupID); err != nil {
		return "", err
	}

	groupJSON, err := GetGroup(payload.GroupID)
	if err != nil {
		return "", err
	}
	var group Group
	if err := json.Unmarshal([]byte(groupJSON), &group); err != nil {
		return "", fmt.Errorf("kvmobile: decode group: %w", err)
	}

	commandsJSON, err := ListCommands(payload.GroupID)
	if err != nil {
		return "", err
	}
	var commands []Command
	if err := json.Unmarshal([]byte(commandsJSON), &commands); err != nil {
		return "", fmt.Errorf("kvmobile: decode commands: %w", err)
	}

	out, err := json.Marshal(GroupView{Group: group, Commands: commands})
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode group view: %w", err)
	}
	return string(out), nil
}

// logCommandExecKind is the fixed pkg/logrecord Kind every
// AppendCommandLog entry is stored under, keyed by instance id (globally
// unique, not scoped to a group -- see newInstanceID) rather than a
// per-group Kind the way Command/CommandRequest are, since a caller
// tracking one dispatch already knows exactly which instance id it wants,
// with no need to enumerate "every log entry in group G".
const logCommandExecKind = "cmdlog"

// commandRequestLogKind returns the pkg/logrecord Kind every
// SubmitCommand dispatch (CommandRequest) belonging to groupID is stored
// under -- same per-group-namespacing reasoning as commandLogKind in
// catalog.go, so ListCommandRequests can enumerate a group's pending
// requests with one prefix scan.
func commandRequestLogKind(groupID string) string {
	return "cmdreq:" + groupID
}

// executePoke is the small JSON envelope SubmitCommand/AppendCommandLog
// send over Execute as an optional low-latency nudge -- see
// WatchCommandLog's doc comment for what it's for and why WatchCommandLog
// itself doesn't depend on receiving it. Type is "cmd_req" (a new
// SubmitCommand dispatch) or "cmd_log" (a new AppendCommandLog entry); an
// app with its own WatchExecute callback can decode this itself to decide
// what to react to.
type executePoke struct {
	Type       string `json:"type"`
	GroupID    string `json:"group_id,omitempty"`
	CommandID  string `json:"command_id,omitempty"`
	InstanceID string `json:"instance_id"`
}

// newInstanceID returns a fresh random hex id for one SubmitCommand
// dispatch -- globally unique (not scoped to a group) since
// GetCommandRequest/QueryCommandLog/WatchCommandLog all key off it alone.
func newInstanceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("kvmobile: generate instance id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// CommandRequest is SubmitCommand's durable record of one dispatch --
// GetCommandRequest/ListCommandRequests read it back. There's no
// update/delete for these, only the single revision SubmitCommand writes.
type CommandRequest struct {
	InstanceID  string    `json:"instance_id"`
	GroupID     string    `json:"group_id"`
	CommandID   string    `json:"command_id"`
	RequestedBy string    `json:"requested_by"`
	Inputs      string    `json:"inputs,omitempty"` // caller-defined JSON, opaque to kvmobile
	RequestedAt time.Time `json:"requested_at"`
}

func recordToCommandRequest(h revisionHistory) CommandRequest {
	return CommandRequest{
		InstanceID:  h.latest.UnitID,
		GroupID:     h.latest.Fields["group_id"],
		CommandID:   h.latest.Fields["command_id"],
		RequestedBy: h.latest.AuthorPeerID,
		Inputs:      h.latest.Fields["inputs"],
		RequestedAt: h.latest.Timestamp,
	}
}

// SubmitCommand dispatches commandID (which must already exist in
// groupID -- see CreateCommand) with inputsJSON (caller-defined, opaque
// to kvmobile -- typically the JSON object a form built from the
// Command's FormSchema produced) as a durable, replicated
// CommandRequest, then sends the command's TargetPeerID a low-latency
// Execute poke naming the new instance id (best-effort: a failed poke
// doesn't fail the dispatch, since the durable request is the real
// source of truth -- see ListCommandRequests for the target's catch-up
// path if the poke never arrives). Returns the instance id, which the
// caller uses with GetCommandRequest/QueryCommandLog/WatchCommandLog to
// track this specific dispatch. Requires the caller to already be a
// participant of groupID.
//
// kvmobile only dispatches and records the request; actually running
// commandID is the target device's own application logic (see
// AppendCommandLog for how it reports back).
func SubmitCommand(groupID, commandID, inputsJSON string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	if err := requireGroupParticipant(groupID); err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	h, err := scanRevisions(ctx, sess, commandLogKind(groupID), commandID)
	if err != nil {
		return "", fmt.Errorf("kvmobile: submit command: %w", err)
	}
	if !h.found || h.latest.Fields["deleted"] == "true" {
		return "", fmt.Errorf("kvmobile: command %s not found in group %s", commandID, groupID)
	}
	targetPeerID := h.latest.Fields["target_peer_id"]

	instanceID, err := newInstanceID()
	if err != nil {
		return "", err
	}

	fields := map[string]string{
		"group_id":   groupID,
		"command_id": commandID,
	}
	if inputsJSON != "" {
		fields["inputs"] = inputsJSON
	}
	if err := appendRecord(ctx, sess, commandRequestLogKind(groupID), instanceID, fields, ""); err != nil {
		return "", fmt.Errorf("kvmobile: submit command: %w", err)
	}

	if poke, err := json.Marshal(executePoke{Type: "cmd_req", GroupID: groupID, CommandID: commandID, InstanceID: instanceID}); err == nil {
		_ = Execute(targetPeerID, string(poke))
	}

	return instanceID, nil
}

// GetCommandRequest returns instanceID's dispatch record within groupID
// as a JSON CommandRequest, or an error if it doesn't exist or the caller
// isn't a participant of groupID. Typically called by the target device
// after receiving a "cmd_req" Execute poke (see executePoke), to learn
// which command and inputs it names.
func GetCommandRequest(groupID, instanceID string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	if err := requireGroupParticipant(groupID); err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	h, err := scanRevisions(ctx, sess, commandRequestLogKind(groupID), instanceID)
	if err != nil {
		return "", fmt.Errorf("kvmobile: get command request: %w", err)
	}
	if !h.found {
		return "", fmt.Errorf("kvmobile: command request %s not found in group %s", instanceID, groupID)
	}

	out, err := json.Marshal(recordToCommandRequest(h))
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode command request: %w", err)
	}
	return string(out), nil
}

// ListCommandRequests returns every dispatch request currently recorded
// for groupID as a JSON array of CommandRequest (`"[]"` when none exist),
// oldest first. How a target device catches up on requests it might have
// missed an Execute poke for -- pokes are unreplicated and dropped if the
// device wasn't running to receive them (see SubmitCommand's doc
// comment) -- e.g. on app startup, or polled periodically alongside
// WatchExecute as a reliability fallback.
func ListCommandRequests(groupID string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	if err := requireGroupParticipant(groupID); err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	ids, err := listUnitIDs(ctx, sess, commandRequestLogKind(groupID))
	if err != nil {
		return "", fmt.Errorf("kvmobile: list command requests: %w", err)
	}

	requests := []CommandRequest{}
	for _, id := range ids {
		h, err := scanRevisions(ctx, sess, commandRequestLogKind(groupID), id)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list command requests: %w", err)
		}
		if !h.found {
			continue
		}
		requests = append(requests, recordToCommandRequest(h))
	}

	out, err := json.Marshal(requests)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode command requests: %w", err)
	}
	return string(out), nil
}

// AppendCommandLog writes one execution-log entry for instanceID --
// SubmitCommand's target device calls this as it works through a
// command, and QueryCommandLog/WatchCommandLog is how the requester (and
// anyone else who knows instanceID) reads it back. Also sends
// requesterPeerID a low-latency Execute poke, best-effort (see
// SubmitCommand's doc comment on why a failed poke doesn't fail the
// call) -- requesterPeerID normally comes from
// GetCommandRequest(...).RequestedBy. Pass "" for requesterPeerID to skip
// the poke.
func AppendCommandLog(requesterPeerID, instanceID, fieldsJSON, narrative string) error {
	if instanceID == "" {
		return fmt.Errorf("kvmobile: instance id must not be empty")
	}
	if err := LogAppend(logCommandExecKind, instanceID, fieldsJSON, narrative); err != nil {
		return err
	}

	if requesterPeerID != "" {
		if poke, err := json.Marshal(executePoke{Type: "cmd_log", InstanceID: instanceID}); err == nil {
			_ = Execute(requesterPeerID, string(poke))
		}
	}
	return nil
}

// QueryCommandLog lists every AppendCommandLog entry for instanceID with
// a timestamp in [since, until], oldest first, up to limit records -- a
// thin wrapper over LogQuery(logCommandExecKind, instanceID, ...) so
// callers don't need to know that Kind convention themselves. since/until
// are RFC3339 or "" (since "" = unbounded, until "" = now); limit is a
// count or "" (no limit).
func QueryCommandLog(instanceID, since, until, limit string) (string, error) {
	return LogQuery(logCommandExecKind, instanceID, since, until, limit)
}

// LogCallback is a gomobile-bindable interface Kotlin implements to
// receive WatchCommandLog's periodic updates -- the same reverse-binding
// pattern ExecuteCallback uses.
type LogCallback interface {
	// OnRecords is called whenever a poll finds new records since the
	// last one, as a JSON array (never called with an empty array). Runs
	// on WatchCommandLog's own goroutine, never the caller's.
	OnRecords(recordsJSON string)
}

// watchCommandLogPollInterval bounds how often runCommandLogWatch
// re-queries QueryCommandLog. Unlike WatchExecute's drain of the
// in-memory executeInbox, this is a real replicated-store read each tick
// (see QueryCommandLog), so it's deliberately longer than
// watchExecutePollInterval.
const watchCommandLogPollInterval = 1500 * time.Millisecond

// commandLogWatch is one active WatchCommandLog loop's stop handle.
type commandLogWatch struct {
	cancel context.CancelFunc
	done   chan struct{}
}

var (
	commandLogWatchMu sync.Mutex
	commandLogWatches = map[string]commandLogWatch{}
)

// WatchCommandLog polls QueryCommandLog(instanceID, ...) on a timer and
// invokes cb.OnRecords with whatever's new since the last poll, until
// StopWatchCommandLog(instanceID) is called. A second WatchCommandLog
// call for the same instanceID replaces the first (stopping it first);
// different instanceIDs run independently and concurrently.
//
// Unlike WatchExecute, this is timer-based, not driven by EventExecute
// delivery: PollExecute's queue (executeInbox in pkg/daemon) has exactly
// one consumer slot per device -- a second independent drainer here would
// race WatchExecute for the same notifications and silently steal ones
// meant for it (see that field's own doc comment). AppendCommandLog does
// still send a low-latency Execute poke to the requester as an optional
// accelerant for a caller that's *also* running its own WatchExecute and
// wants to react to a "cmd_log" notification (see executePoke) by
// triggering an immediate QueryCommandLog itself, but WatchCommandLog
// doesn't depend on that -- it works standalone, at the cost of up to
// watchCommandLogPollInterval of extra latency versus a genuine push.
func WatchCommandLog(instanceID string, cb LogCallback) error {
	if cb == nil {
		return fmt.Errorf("kvmobile: WatchCommandLog: cb must not be nil")
	}
	if instanceID == "" {
		return fmt.Errorf("kvmobile: WatchCommandLog: instanceID must not be empty")
	}

	commandLogWatchMu.Lock()
	defer commandLogWatchMu.Unlock()
	stopCommandLogWatchLocked(instanceID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	commandLogWatches[instanceID] = commandLogWatch{cancel: cancel, done: done}

	go runCommandLogWatch(ctx, done, instanceID, cb)
	return nil
}

// StopWatchCommandLog stops instanceID's watcher, if any, and waits for
// it to actually exit before returning. Safe to call when nothing is
// running for it (a no-op).
func StopWatchCommandLog(instanceID string) {
	commandLogWatchMu.Lock()
	defer commandLogWatchMu.Unlock()
	stopCommandLogWatchLocked(instanceID)
}

// stopCommandLogWatchLocked requires commandLogWatchMu already held.
func stopCommandLogWatchLocked(instanceID string) {
	w, ok := commandLogWatches[instanceID]
	if !ok {
		return
	}
	w.cancel()
	<-w.done
	delete(commandLogWatches, instanceID)
}

// runCommandLogWatch is WatchCommandLog's background loop body.
func runCommandLogWatch(ctx context.Context, done chan struct{}, instanceID string, cb LogCallback) {
	defer close(done)

	// since tracks the timestamp just past the newest record already
	// delivered to cb, so each round only asks for what's new.
	var since time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(watchCommandLogPollInterval):
		}

		sinceStr := ""
		if !since.IsZero() {
			sinceStr = since.Format(time.RFC3339Nano)
		}
		out, err := QueryCommandLog(instanceID, sinceStr, "", "")
		if err != nil {
			continue
		}
		var records []logrecord.Record
		if err := json.Unmarshal([]byte(out), &records); err != nil || len(records) == 0 {
			continue
		}

		cb.OnRecords(out)
		since = records[len(records)-1].Timestamp.Add(time.Nanosecond)
	}
}

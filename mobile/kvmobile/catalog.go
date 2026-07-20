package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// This file implements a caller-defined catalog of "groups" (named
// containers, publicly listable) and "commands" (actionable operations
// scoped to a group, each naming a TargetPeerID that executes it and a
// FormSchema describing the inputs its submission form should collect).
//
// Both are stored as pkg/logrecord.Record chains -- append-only, with
// "update" meaning a fresh record under the same ID and "delete" meaning a
// tombstone record (Fields["deleted"] == "true"); readers always fold a
// unitID's full revision history down to its latest entry (see
// scanRevisions). This reuses pkg/logrecord's own replication/durability
// and needs no new capnp wire schema -- every operation here is a plain
// EventLogAppend/EventListRange call, exactly like LogAppend/LogQuery.
//
// "Participant of group G" is a confirmed pkg/shmevent KindLogPermit
// record for logKind commandLogKind(G) -- see IsGroupParticipant.
// Deliberately the *same* string Commands are stored under, so
// participation and command-namespace access are one fact rather than two
// that could drift apart, and so a future switch to server-side
// enforcement (pkg/daemon's Config.RequirePermitForLog, which today only
// gates a *different* peer's forwarded request, never a device's own
// local calls -- see that field's doc comment) would apply to exactly the
// right data with no further changes. Today the check is client-side
// only: every binding below that should be participant-gated calls
// requireGroupParticipant itself, but nothing stops a caller from working
// around these bindings entirely and calling LogAppend/LogQuery directly.

// logGroupKind is the fixed pkg/logrecord Kind every Group definition is
// stored under. Unlike Command, Group listing/reading has no participation
// gate (see CreateGroup's doc comment: it's a public catalog), so it
// doesn't need a per-group Kind the way commandLogKind does.
const logGroupKind = "group"

// commandLogKind returns the pkg/logrecord Kind every Command belonging to
// groupID is stored under, *and* the RequestLogPermit/ConfirmLogPermit/
// RevokeLogPermit logKind that gates participation in that same group --
// see this file's doc comment for why those are deliberately the same
// string.
func commandLogKind(groupID string) string {
	return "command:" + groupID
}

// maxCatalogIDLen bounds Group/Command IDs, enforced by validateCatalogID
// -- listUnitIDs's fixed-width upper bound is built from this same
// constant, so it's provably wide enough to cover every possible key
// under a kind regardless of which unitIDs actually exist.
const maxCatalogIDLen = 256

func validateCatalogID(id string) error {
	if id == "" {
		return fmt.Errorf("kvmobile: id must not be empty")
	}
	if len(id) > maxCatalogIDLen {
		return fmt.Errorf("kvmobile: id exceeds %d bytes", maxCatalogIDLen)
	}
	return nil
}

// IsGroupParticipant reports whether this device's own peer id currently
// holds a confirmed permit for groupID (see commandLogKind) -- "is a
// participant of group G". Every Command CRUD/list/get binding below
// requires this before proceeding; Group create/read/list do not (a
// public catalog) -- only UpdateGroup/DeleteGroup do.
//
// Enforced client-side only, in kvmobile itself: nothing in pkg/daemon
// independently blocks a local caller from reading or writing its own
// already-replicated store (Config.RequirePermitForLog only gates a
// *different* peer's forwarded request -- see that field's doc comment in
// pkg/daemon), so this check is only as strong as every caller actually
// going through these bindings rather than around them.
func IsGroupParticipant(groupID string) (bool, error) {
	sess, err := currentSession()
	if err != nil {
		return false, err
	}
	key, err := shmevent.LogPermitKey(shmevent.StatusConfirmed, commandLogKind(groupID), []byte(PeerID()))
	if err != nil {
		return false, fmt.Errorf("kvmobile: is group participant: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if _, err := sess.Get(ctx, string(key)); err != nil {
		return false, nil
	}
	return true, nil
}

// RequestGroupParticipation lodges a pending request for targetPeerID to
// participate in groupID -- a thin, group-scoped wrapper over
// RequestLogPermit(commandLogKind(groupID), ...) so callers don't need to
// know that naming convention themselves.
func RequestGroupParticipation(groupID, targetPeerID, metadata string) error {
	return RequestLogPermit(commandLogKind(groupID), targetPeerID, metadata)
}

// ConfirmGroupParticipation promotes a pending participation request for
// targetPeerID in groupID to confirmed. Only takes effect if this device
// is itself a raft voter (see ConfirmLogPermit's doc comment).
func ConfirmGroupParticipation(groupID, targetPeerID string) error {
	return ConfirmLogPermit(commandLogKind(groupID), targetPeerID)
}

// RevokeGroupParticipation deletes a confirmed participation record for
// targetPeerID in groupID outright.
func RevokeGroupParticipation(groupID, targetPeerID string) error {
	return RevokeLogPermit(commandLogKind(groupID), targetPeerID)
}

func requireGroupParticipant(groupID string) error {
	ok, err := IsGroupParticipant(groupID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("kvmobile: not a participant of group %s", groupID)
	}
	return nil
}

// revisionHistory is scanRevisions' result: a unitID's latest revision,
// plus who/when first created it (kept separately since "latest"
// overwrites Timestamp/AuthorPeerID on every update).
type revisionHistory struct {
	latest    logrecord.Record
	createdAt time.Time
	createdBy string
	found     bool
}

// scanRevisions folds every logrecord.Record for (kind, unitID) down to
// its latest revision -- Group/Command's "current state" under the
// append-only/latest-wins scheme this file's doc comment describes.
func scanRevisions(ctx context.Context, sess *shmclient.Session, kind, unitID string) (revisionHistory, error) {
	lo, hi := logrecord.ScanBounds(kind, unitID, time.Unix(0, 0), time.Now())
	var h revisionHistory
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return revisionHistory{}, err
		}
		if !ok {
			return h, nil
		}
		rec, err := logrecord.Decode(value)
		if err != nil {
			return revisionHistory{}, err
		}
		if !h.found {
			h.createdAt = rec.Timestamp
			h.createdBy = rec.AuthorPeerID
		}
		h.latest = rec
		h.found = true
		lo = append(append([]byte{}, key...), 0x00)
	}
}

// listUnitIDs enumerates every distinct unitID that has ever logged a
// record of kind (see logrecord.KindPrefix), in ascending key order --
// multiple revisions of the same unitID are deduplicated, keeping
// first-seen order. The upper bound is sized from maxCatalogIDLen so it's
// provably wide enough regardless of which unitIDs actually exist under
// kind.
func listUnitIDs(ctx context.Context, sess *shmclient.Session, kind string) ([]string, error) {
	prefix := logrecord.KindPrefix(kind)
	lo := prefix
	hi := make([]byte, len(prefix)+2+maxCatalogIDLen+8+8)
	copy(hi, prefix)
	for i := len(prefix); i < len(hi); i++ {
		hi[i] = 0xFF
	}

	seen := map[string]bool{}
	var ids []string
	for {
		key, _, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return nil, err
		}
		if !ok {
			return ids, nil
		}
		_, unitID, _, err := logrecord.ParseKey(key)
		if err != nil {
			return nil, err
		}
		if !seen[unitID] {
			seen[unitID] = true
			ids = append(ids, unitID)
		}
		lo = append(append([]byte{}, key...), 0x00)
	}
}

// appendRecord builds and appends one logrecord.Record -- the shared tail
// end LogAppend and every Group/Command write below reduce to.
func appendRecord(ctx context.Context, sess *shmclient.Session, kind, unitID string, fields map[string]string, narrative string) error {
	rnd, err := logrecord.NewRand()
	if err != nil {
		return fmt.Errorf("kvmobile: %w", err)
	}
	ts := time.Now()
	key, err := logrecord.BuildKey(kind, unitID, ts, rnd)
	if err != nil {
		return fmt.Errorf("kvmobile: %w", err)
	}
	rec := logrecord.Record{
		Kind:         kind,
		UnitID:       unitID,
		Timestamp:    ts,
		AuthorPeerID: PeerID(),
		Fields:       fields,
		Narrative:    narrative,
	}
	value, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("kvmobile: %w", err)
	}
	if err := sess.LogAppend(ctx, key, value); err != nil {
		return fmt.Errorf("kvmobile: %w", err)
	}
	return nil
}

// Group is a caller-defined command group: a named, publicly listable
// container for a set of Commands. Participation (see IsGroupParticipant)
// gates Command access within it, not the Group definition itself.
type Group struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedBy   string    `json:"created_by"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func recordToGroup(h revisionHistory) Group {
	return Group{
		ID:          h.latest.UnitID,
		Name:        h.latest.Fields["name"],
		Description: h.latest.Narrative,
		CreatedBy:   h.createdBy,
		CreatedAt:   h.createdAt,
		UpdatedAt:   h.latest.Timestamp,
	}
}

// CreateGroup defines a new command group under id (or appends a fresh
// revision over an existing/deleted one -- see UpdateGroup, the same
// operation under a different name). Unlike UpdateGroup/DeleteGroup, this
// has no participation requirement: Groups are a public catalog, so any
// cluster member may propose one.
func CreateGroup(id, name, description string) error {
	return putGroup(id, name, description, false, false)
}

// UpdateGroup appends a new revision for id's name/description. Requires
// the caller to already be a participant of id, unlike CreateGroup.
func UpdateGroup(id, name, description string) error {
	return putGroup(id, name, description, false, true)
}

// DeleteGroup appends a tombstone revision for id -- GetGroup/ListGroups
// exclude it afterward. Existing Command records under it aren't
// themselves deleted, just unreachable through the catalog. Requires the
// caller to already be a participant of id.
func DeleteGroup(id string) error {
	return putGroup(id, "", "", true, true)
}

func putGroup(id, name, description string, deleted, requireParticipant bool) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}
	if err := validateCatalogID(id); err != nil {
		return err
	}
	if requireParticipant {
		if err := requireGroupParticipant(id); err != nil {
			return err
		}
	}

	fields := map[string]string{"name": name}
	if deleted {
		fields["deleted"] = "true"
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := appendRecord(ctx, sess, logGroupKind, id, fields, description); err != nil {
		return fmt.Errorf("kvmobile: put group: %w", err)
	}
	return nil
}

// GetGroup returns id's current definition as a JSON Group, or an error
// if it doesn't exist or was deleted.
func GetGroup(id string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	h, err := scanRevisions(ctx, sess, logGroupKind, id)
	if err != nil {
		return "", fmt.Errorf("kvmobile: get group: %w", err)
	}
	if !h.found || h.latest.Fields["deleted"] == "true" {
		return "", fmt.Errorf("kvmobile: group %s not found", id)
	}

	out, err := json.Marshal(recordToGroup(h))
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode group: %w", err)
	}
	return string(out), nil
}

// ListGroups returns every non-deleted Group as a JSON array (`"[]"` when
// none exist). No participation check -- see CreateGroup's doc comment.
func ListGroups() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	ids, err := listUnitIDs(ctx, sess, logGroupKind)
	if err != nil {
		return "", fmt.Errorf("kvmobile: list groups: %w", err)
	}

	groups := []Group{}
	for _, id := range ids {
		h, err := scanRevisions(ctx, sess, logGroupKind, id)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list groups: %w", err)
		}
		if !h.found || h.latest.Fields["deleted"] == "true" {
			continue
		}
		groups = append(groups, recordToGroup(h))
	}

	out, err := json.Marshal(groups)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode groups: %w", err)
	}
	return string(out), nil
}

// FormField describes one input a Command's submission form should
// collect -- purely descriptive metadata for the calling UI to render a
// form from; kvmobile does not validate submitted values against it.
type FormField struct {
	Name     string   `json:"name"`
	Label    string   `json:"label"`
	Type     string   `json:"type"`
	Required bool     `json:"required,omitempty"`
	Options  []string `json:"options,omitempty"`
}

// Command is a single actionable operation belonging to a Group, executed
// by TargetPeerID and described by FormSchema for the calling UI to
// render an input form from -- kvmobile does not itself interpret or run
// a Command, only defines/discovers it (SubmitCommand, not added yet,
// only dispatches and audits one).
type Command struct {
	ID           string      `json:"id"`
	GroupID      string      `json:"group_id"`
	TargetPeerID string      `json:"target_peer_id"`
	Name         string      `json:"name"`
	Description  string      `json:"description"`
	FormSchema   []FormField `json:"form_schema,omitempty"`
	CreatedBy    string      `json:"created_by"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
}

func recordToCommand(h revisionHistory) (Command, error) {
	rec := h.latest
	c := Command{
		ID:           rec.UnitID,
		GroupID:      rec.Fields["group_id"],
		TargetPeerID: rec.Fields["target_peer_id"],
		Name:         rec.Fields["name"],
		Description:  rec.Narrative,
		CreatedBy:    h.createdBy,
		CreatedAt:    h.createdAt,
		UpdatedAt:    rec.Timestamp,
	}
	if schemaJSON := rec.Fields["form_schema"]; schemaJSON != "" {
		if err := json.Unmarshal([]byte(schemaJSON), &c.FormSchema); err != nil {
			return Command{}, fmt.Errorf("kvmobile: decode form schema: %w", err)
		}
	}
	return c, nil
}

// CreateCommand defines commandID as belonging to groupID, executable by
// targetPeerID and described by formSchemaJSON (a JSON array of
// FormField, or "" for none) -- the calling UI renders its submission
// form from that schema. Like CreateGroup/UpdateGroup, this and
// UpdateCommand are the same append operation, just named for intent.
// Requires the caller to already be a participant of groupID (see
// IsGroupParticipant) -- unlike CreateGroup, Command writes are always
// gated.
func CreateCommand(id, groupID, targetPeerID, name, description, formSchemaJSON string) error {
	return putCommand(id, groupID, targetPeerID, name, description, formSchemaJSON, false)
}

// UpdateCommand is CreateCommand's alias for the "this id already exists"
// case -- see CreateCommand's doc comment.
func UpdateCommand(id, groupID, targetPeerID, name, description, formSchemaJSON string) error {
	return putCommand(id, groupID, targetPeerID, name, description, formSchemaJSON, false)
}

// DeleteCommand appends a tombstone revision for id within groupID --
// GetCommand/ListCommands exclude it afterward. Requires the caller to
// already be a participant of groupID.
func DeleteCommand(groupID, id string) error {
	return putCommand(id, groupID, "", "", "", "", true)
}

func putCommand(id, groupID, targetPeerID, name, description, formSchemaJSON string, deleted bool) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}
	if err := validateCatalogID(id); err != nil {
		return err
	}
	if groupID == "" {
		return fmt.Errorf("kvmobile: command group_id must not be empty")
	}
	if err := requireGroupParticipant(groupID); err != nil {
		return err
	}
	if !deleted && targetPeerID == "" {
		return fmt.Errorf("kvmobile: command target_peer_id must not be empty")
	}

	fields := map[string]string{
		"group_id":       groupID,
		"target_peer_id": targetPeerID,
		"name":           name,
	}
	if deleted {
		fields["deleted"] = "true"
	} else if formSchemaJSON != "" {
		var schema []FormField
		if err := json.Unmarshal([]byte(formSchemaJSON), &schema); err != nil {
			return fmt.Errorf("kvmobile: decode form schema: %w", err)
		}
		fields["form_schema"] = formSchemaJSON
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := appendRecord(ctx, sess, commandLogKind(groupID), id, fields, description); err != nil {
		return fmt.Errorf("kvmobile: put command: %w", err)
	}
	return nil
}

// GetCommand returns commandID's current definition within groupID as a
// JSON Command, or an error if it doesn't exist, was deleted, or the
// caller isn't a participant of groupID.
func GetCommand(groupID, id string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	if err := requireGroupParticipant(groupID); err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	h, err := scanRevisions(ctx, sess, commandLogKind(groupID), id)
	if err != nil {
		return "", fmt.Errorf("kvmobile: get command: %w", err)
	}
	if !h.found || h.latest.Fields["deleted"] == "true" {
		return "", fmt.Errorf("kvmobile: command %s not found in group %s", id, groupID)
	}

	cmd, err := recordToCommand(h)
	if err != nil {
		return "", fmt.Errorf("kvmobile: get command: %w", err)
	}
	out, err := json.Marshal(cmd)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode command: %w", err)
	}
	return string(out), nil
}

// ListCommands returns every non-deleted Command currently defined under
// groupID as a JSON array (`"[]"` when none exist), or an error if the
// caller isn't a participant of groupID -- the binding behind "if the
// current peer is a participant of this group, they see the list of
// available commands."
func ListCommands(groupID string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}
	if err := requireGroupParticipant(groupID); err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	ids, err := listUnitIDs(ctx, sess, commandLogKind(groupID))
	if err != nil {
		return "", fmt.Errorf("kvmobile: list commands: %w", err)
	}

	commands := []Command{}
	for _, id := range ids {
		h, err := scanRevisions(ctx, sess, commandLogKind(groupID), id)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list commands: %w", err)
		}
		if !h.found || h.latest.Fields["deleted"] == "true" {
			continue
		}
		cmd, err := recordToCommand(h)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list commands: %w", err)
		}
		commands = append(commands, cmd)
	}

	out, err := json.Marshal(commands)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode commands: %w", err)
	}
	return string(out), nil
}

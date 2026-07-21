package kvctl

import (
	"context"
	"fmt"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// This file implements the group-based ACL catalog: Group (id, name),
// Command (id, name, peer_id -- where it may be executed), GroupCommand
// (a many-to-many command<->group link, replacing an earlier one-group-
// per-command field), and PeerGroup (a peer's group membership,
// replacing an earlier confirmed-permit-under-a-synthetic-kind
// convention) -- all daemon-enforced shmevent.SystemKeyPrefix records
// (see shmevent.KindGroup's doc comment in pkg/shmevent/system.go), not
// the pkg/logrecord convention this file used before. Any single current
// raft voter may create/update/delete any of these four kinds directly
// (no second-voter confirmation, see shmevent.EventGroupPut's doc
// comment) -- and pkg/daemon itself enforces that, unlike the old scheme,
// which nothing outside these bindings independently checked.
//
// dispatch.go's SubmitCommand/CommandRequest/CommandLog machinery is
// unaffected by this file: it's a separate, still pkg/logrecord-based
// mechanism (a durable request+response conversation, not ACL
// configuration) that now keys off commandID alone instead of groupID,
// and gates on isPermittedForCommand (this file) instead of group
// participation.

// openCurrentSession opens a pkg/shmclient.Session for the registry's
// current node, alongside its own peer id (needed as AuthorPeerID for
// dispatch.go's logrecord writes) -- the registry.Open+reg.Current()+
// shmclient.Open sequence every other kvctl function already repeats per
// call, factored out here because catalog.go/dispatch.go's functions call
// it many times each. A *shmclient.Session has no Close/teardown to worry
// about (see its own doc comment) -- it's just a signing key holder for
// per-call shmring round trips, safe to open fresh on every call the way
// kvctl's existing functions already do.
func openCurrentSession(ctx context.Context) (sess *shmclient.Session, selfPeerID string, err error) {
	reg, err := registry.Open()
	if err != nil {
		return nil, "", err
	}
	peerID, err := reg.Current()
	if err != nil {
		return nil, "", err
	}
	sess, err = shmclient.Open(ctx, peerID)
	if err != nil {
		return nil, "", fmt.Errorf("kvctl: open session: %w", err)
	}
	return sess, peerID, nil
}

// maxCatalogIDLen bounds Group/Command ids (validateCatalogID) and every
// pkg/logrecord unitID dispatch.go's still-logrecord-based mechanism
// writes -- kindPrefixBounds's fixed-width upper bound is built from this
// same constant, so it's provably wide enough to cover every possible key
// under a kind regardless of which unitIDs actually exist.
const maxCatalogIDLen = 256

func validateCatalogID(id string) error {
	if id == "" {
		return fmt.Errorf("kvctl: id must not be empty")
	}
	if len(id) > maxCatalogIDLen {
		return fmt.Errorf("kvctl: id exceeds %d bytes", maxCatalogIDLen)
	}
	return nil
}

// revisionHistory is scanRevisions' result: a unitID's latest revision,
// plus who/when first created it (kept separately since "latest"
// overwrites Timestamp/AuthorPeerID on every update). Used by
// dispatch.go's still-logrecord-based CommandRequest/CommandLog
// machinery -- Group/Command themselves no longer use it (see this file's
// doc comment).
type revisionHistory struct {
	latest    logrecord.Record
	createdAt time.Time
	createdBy string
	found     bool
}

// scanRevisions folds every logrecord.Record for (kind, unitID) down to
// its latest revision.
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

// kindPrefixBounds returns the [lo, hi] key range covering every record
// of the given kind, across every unitID and timestamp -- the shared
// bound construction behind listUnitIDs and ListExecutionsByPeer's
// per-kind prefix scans.
func kindPrefixBounds(kind string) (lo, hi []byte) {
	prefix := logrecord.KindPrefix(kind)
	lo = prefix
	hi = make([]byte, len(prefix)+2+maxCatalogIDLen+8+8)
	copy(hi, prefix)
	for i := len(prefix); i < len(hi); i++ {
		hi[i] = 0xFF
	}
	return lo, hi
}

// listUnitIDs enumerates every distinct unitID that has ever logged a
// record of kind (see logrecord.KindPrefix), in ascending key order --
// multiple revisions of the same unitID are deduplicated, keeping
// first-seen order.
func listUnitIDs(ctx context.Context, sess *shmclient.Session, kind string) ([]string, error) {
	lo, hi := kindPrefixBounds(kind)

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

// appendRecord builds and appends one logrecord.Record, attributed to
// authorPeerID -- the shared tail end every dispatch.go write in this
// package reduces to.
func appendRecord(ctx context.Context, sess *shmclient.Session, kind, unitID, authorPeerID string, fields map[string]string, narrative string) error {
	rnd, err := logrecord.NewRand()
	if err != nil {
		return fmt.Errorf("kvctl: %w", err)
	}
	ts := time.Now()
	key, err := logrecord.BuildKey(kind, unitID, ts, rnd)
	if err != nil {
		return fmt.Errorf("kvctl: %w", err)
	}
	rec := logrecord.Record{
		Kind:         kind,
		UnitID:       unitID,
		Timestamp:    ts,
		AuthorPeerID: authorPeerID,
		Fields:       fields,
		Narrative:    narrative,
	}
	value, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("kvctl: %w", err)
	}
	if err := sess.LogAppend(ctx, key, value); err != nil {
		return fmt.Errorf("kvctl: %w", err)
	}
	return nil
}

// Group is a named container Commands can be linked to via
// CreateGroupCommand -- peers become permitted to submit/execute a
// command by being added to a group linked to it (AddPeerToGroup).
type Group struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// systemKeyIDOffset is how many leading bytes of a shmevent.SystemKey
// (kind + status placeholder) precede the trailing ID field on a
// GroupKey/CommandKey/ClusterMemberKey -- mirrors ListClusterMembers'
// own key[3:] slicing in cluster_list.go.
const systemKeyIDOffset = 3

// PutGroup implements `mage creategroup`/`mage updategroup <id> <name>`:
// creates or updates (single step -- see shmevent.KindGroup's doc
// comment) the Group record id=name on the current node. Only a current
// raft voter may do this; pkg/daemon rejects it otherwise.
func PutGroup(id, name string) error {
	if err := validateCatalogID(id); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.PutGroup(ctx, id, name); err != nil {
		return fmt.Errorf("kvctl: put group: %w", err)
	}
	return nil
}

// DeleteGroup implements `mage deletegroup <id>`: deletes Group id,
// cascading to every GroupCommand/PeerGroup record referencing it (see
// kvfsm.OpCascadeDelete). Only a current raft voter may do this.
func DeleteGroup(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.DeleteGroup(ctx, id); err != nil {
		return fmt.Errorf("kvctl: delete group: %w", err)
	}
	return nil
}

// GetGroup implements `mage getgroup <id>`.
func GetGroup(id string) (Group, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return Group{}, err
	}
	value, err := sess.Get(ctx, string(shmevent.GroupKey([]byte(id))))
	if err != nil {
		return Group{}, fmt.Errorf("kvctl: group %s not found", id)
	}
	return Group{ID: id, Name: shmevent.DecodeGroupPayload([]byte(value))}, nil
}

// ListGroups implements `mage listgroups`: returns every Group (nil, not
// an error, when none exist).
func ListGroups() ([]Group, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return nil, err
	}
	lo, hi := shmevent.GroupKeyBounds()
	var groups []Group
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list groups: %w", err)
		}
		if !ok {
			return groups, nil
		}
		if len(key) < systemKeyIDOffset {
			return nil, fmt.Errorf("kvctl: malformed group key %x", key)
		}
		groups = append(groups, Group{ID: string(key[systemKeyIDOffset:]), Name: shmevent.DecodeGroupPayload(value)})
		lo = append(append([]byte{}, key...), 0x00)
	}
}

// Command is a single submittable/executable operation: PeerID is where
// it runs, gated by whichever groups it's linked to (CreateGroupCommand).
type Command struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	PeerID string `json:"peer_id"`
}

// PutCommand implements `mage createcommand`/`mage updatecommand <id>
// <name> <peerID>`: creates or updates the Command record
// id={name, peerID}. Only a current raft voter may do this.
func PutCommand(id, name, peerID string) error {
	if err := validateCatalogID(id); err != nil {
		return err
	}
	if peerID == "" {
		return fmt.Errorf("kvctl: command peer_id must not be empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.PutCommand(ctx, id, name, []byte(peerID)); err != nil {
		return fmt.Errorf("kvctl: put command: %w", err)
	}
	return nil
}

// DeleteCommand implements `mage deletecommand <id>`: deletes Command id,
// cascading to every GroupCommand record referencing it. Only a current
// raft voter may do this.
func DeleteCommand(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.DeleteCommand(ctx, id); err != nil {
		return fmt.Errorf("kvctl: delete command: %w", err)
	}
	return nil
}

// GetCommand implements `mage getcommand <id>`.
func GetCommand(id string) (Command, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return Command{}, err
	}
	value, err := sess.Get(ctx, string(shmevent.CommandKey([]byte(id))))
	if err != nil {
		return Command{}, fmt.Errorf("kvctl: command %s not found", id)
	}
	name, peerID, err := shmevent.DecodeCommandPayload([]byte(value))
	if err != nil {
		return Command{}, fmt.Errorf("kvctl: decode command %s: %w", id, err)
	}
	return Command{ID: id, Name: name, PeerID: string(peerID)}, nil
}

// ListCommands implements `mage listcommands`: returns every Command
// (nil, not an error, when none exist).
func ListCommands() ([]Command, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return nil, err
	}
	lo, hi := shmevent.CommandKeyBounds()
	var commands []Command
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list commands: %w", err)
		}
		if !ok {
			return commands, nil
		}
		if len(key) < systemKeyIDOffset {
			return nil, fmt.Errorf("kvctl: malformed command key %x", key)
		}
		id := string(key[systemKeyIDOffset:])
		name, peerID, err := shmevent.DecodeCommandPayload(value)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list commands: decode %s: %w", id, err)
		}
		commands = append(commands, Command{ID: id, Name: name, PeerID: string(peerID)})
		lo = append(append([]byte{}, key...), 0x00)
	}
}

// CreateGroupCommand implements `mage addcommandtogroup <commandID>
// <groupID>`: links commandID to groupID -- peers added to groupID
// (AddPeerToGroup) become permitted to submit/execute commandID. Only a
// current raft voter may do this.
func CreateGroupCommand(commandID, groupID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.PutGroupCommand(ctx, []byte(commandID), []byte(groupID)); err != nil {
		return fmt.Errorf("kvctl: create group-command: %w", err)
	}
	return nil
}

// DeleteGroupCommand implements `mage removecommandfromgroup <commandID>
// <groupID>`: unlinks commandID from groupID.
func DeleteGroupCommand(commandID, groupID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.DeleteGroupCommand(ctx, []byte(commandID), []byte(groupID)); err != nil {
		return fmt.Errorf("kvctl: delete group-command: %w", err)
	}
	return nil
}

// ListGroupsForCommand implements `mage listgroupsforcommand <commandID>`:
// returns every group id commandID is linked to.
func ListGroupsForCommand(commandID string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return nil, err
	}
	lo, hi, err := shmevent.GroupCommandBounds([]byte(commandID))
	if err != nil {
		return nil, err
	}
	var groupIDs []string
	for {
		key, _, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list groups for command: %w", err)
		}
		if !ok {
			return groupIDs, nil
		}
		_, groupID, err := shmevent.ParseGroupCommandKey(key)
		if err != nil {
			return nil, err
		}
		groupIDs = append(groupIDs, string(groupID))
		lo = append(append([]byte{}, key...), 0x00)
	}
}

// AddPeerToGroup implements `mage addpeertogroup <peerID> <groupID>`:
// grants peerID membership in groupID. Only a current raft voter may do
// this.
func AddPeerToGroup(peerID, groupID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.PutPeerGroup(ctx, []byte(peerID), []byte(groupID)); err != nil {
		return fmt.Errorf("kvctl: add peer to group: %w", err)
	}
	return nil
}

// RemovePeerFromGroup implements `mage removepeerfromgroup <peerID>
// <groupID>`: revokes peerID's membership in groupID.
func RemovePeerFromGroup(peerID, groupID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return err
	}
	if err := sess.DeletePeerGroup(ctx, []byte(peerID), []byte(groupID)); err != nil {
		return fmt.Errorf("kvctl: remove peer from group: %w", err)
	}
	return nil
}

// ListGroupsForPeer implements `mage listgroupsforpeer <peerID>`: returns
// every group id peerID belongs to.
func ListGroupsForPeer(peerID string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	sess, _, err := openCurrentSession(ctx)
	if err != nil {
		return nil, err
	}
	lo, hi, err := shmevent.PeerGroupBounds([]byte(peerID))
	if err != nil {
		return nil, err
	}
	var groupIDs []string
	for {
		key, _, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return nil, fmt.Errorf("kvctl: list groups for peer: %w", err)
		}
		if !ok {
			return groupIDs, nil
		}
		_, groupID, err := shmevent.ParsePeerGroupKey(key)
		if err != nil {
			return nil, err
		}
		groupIDs = append(groupIDs, string(groupID))
		lo = append(append([]byte{}, key...), 0x00)
	}
}

// isPermittedForCommand reports whether peerID may submit/execute
// commandID: true if some group G satisfies both PeerGroup(peerID, G) and
// GroupCommand(commandID, G). Scans GroupCommandBounds(commandID) first
// (a command is expected to be linked to few groups, unlike a peer, which
// may belong to many) and point-checks PeerGroupKey(peerID, group) for
// each hit -- scan the smaller side, point-check the other -- the first
// match short-circuits.
func isPermittedForCommand(ctx context.Context, sess *shmclient.Session, peerID, commandID string) (bool, error) {
	lo, hi, err := shmevent.GroupCommandBounds([]byte(commandID))
	if err != nil {
		return false, err
	}
	for {
		key, _, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		_, groupID, err := shmevent.ParseGroupCommandKey(key)
		if err != nil {
			return false, err
		}
		peerGroupKey, err := shmevent.PeerGroupKey([]byte(peerID), groupID)
		if err != nil {
			return false, err
		}
		if _, err := sess.Get(ctx, string(peerGroupKey)); err == nil {
			return true, nil
		}
		lo = append(append([]byte{}, key...), 0x00)
	}
}

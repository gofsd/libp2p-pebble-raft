package kvmobile

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// pollUntilTrue retries check until it reports true, or fails the test
// after timeout -- the shared retry shape every catalog test below needs
// since a write forwarded through raft (LogAppend, permit
// request/confirm) becomes locally readable asynchronously, same reason
// pkg/kvctl's own cross-node tests poll.
func pollUntilTrue(t *testing.T, timeout time.Duration, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if err != nil {
			lastErr = err
		} else if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s (last error: %v)", timeout, lastErr)
}

// grantSelfParticipation makes this device a confirmed participant of
// groupID -- request-then-confirm against its own session, which works
// because a kvmobile follower always joins as a full raft voter (see
// ConfirmLogPermit's doc comment), not because of any special-casing
// here.
func grantSelfParticipation(t *testing.T, groupID string) {
	t.Helper()
	if err := RequestGroupParticipation(groupID, PeerID(), ""); err != nil {
		t.Fatalf("RequestGroupParticipation: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		return true, ConfirmGroupParticipation(groupID, PeerID())
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		return IsGroupParticipant(groupID)
	})
}

// TestGroupCRUD drives Create/Get/List/Update/Delete against a real
// leader: Update/Delete must refuse before this device is a confirmed
// participant of the group and succeed after, and Delete's tombstone must
// exclude the group from both Get and List afterward.
func TestGroupCRUD(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const groupID = "grp-1"
	if err := CreateGroup(groupID, "Group One", "first group"); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	var g Group
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := GetGroup(groupID)
		if err != nil {
			return false, err
		}
		return true, json.Unmarshal([]byte(out), &g)
	})
	if g.ID != groupID || g.Name != "Group One" || g.Description != "first group" {
		t.Fatalf("GetGroup = %+v, unexpected", g)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroups()
		if err != nil {
			return false, err
		}
		var groups []Group
		if err := json.Unmarshal([]byte(out), &groups); err != nil {
			return false, err
		}
		for _, gr := range groups {
			if gr.ID == groupID {
				return true, nil
			}
		}
		return false, nil
	})

	if err := UpdateGroup(groupID, "Renamed", "updated desc"); err == nil {
		t.Fatalf("UpdateGroup before participation: want error, got none")
	}

	grantSelfParticipation(t, groupID)

	if err := UpdateGroup(groupID, "Renamed", "updated desc"); err != nil {
		t.Fatalf("UpdateGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := GetGroup(groupID)
		if err != nil {
			return false, err
		}
		if err := json.Unmarshal([]byte(out), &g); err != nil {
			return false, err
		}
		return g.Name == "Renamed", nil
	})
	if g.Description != "updated desc" {
		t.Fatalf("GetGroup after update Description = %q, want %q", g.Description, "updated desc")
	}

	if err := DeleteGroup(groupID); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetGroup(groupID)
		return err != nil, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListGroups()
		if err != nil {
			return false, err
		}
		var groups []Group
		if err := json.Unmarshal([]byte(out), &groups); err != nil {
			return false, err
		}
		for _, gr := range groups {
			if gr.ID == groupID {
				return false, nil
			}
		}
		return true, nil
	})
}

// TestGroupParticipationLifecycle drives IsGroupParticipant/
// RequestGroupParticipation/ConfirmGroupParticipation/
// RevokeGroupParticipation end to end.
func TestGroupParticipationLifecycle(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const groupID = "grp-participation"

	ok, err := IsGroupParticipant(groupID)
	if err != nil {
		t.Fatalf("IsGroupParticipant (before): %v", err)
	}
	if ok {
		t.Fatalf("IsGroupParticipant (before) = true, want false")
	}

	grantSelfParticipation(t, groupID)

	if err := RevokeGroupParticipation(groupID, PeerID()); err != nil {
		t.Fatalf("RevokeGroupParticipation: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		ok, err := IsGroupParticipant(groupID)
		if err != nil {
			return false, err
		}
		return !ok, nil
	})
}

// TestCommandCRUD drives Create/Get/List/Update/Delete for Commands,
// including the participation gate (unlike Group, every Command operation
// -- reads included -- requires it) and the FormSchema JSON round-trip.
func TestCommandCRUD(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	followerID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	const groupID = "grp-cmds"

	if err := CreateCommand("cmd-1", groupID, followerID, "Reboot", "restart the device", ""); err == nil {
		t.Fatalf("CreateCommand before participation: want error, got none")
	}

	grantSelfParticipation(t, groupID)

	schema := []FormField{{Name: "delay_seconds", Label: "Delay (seconds)", Type: "number", Required: true}}
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}
	if err := CreateCommand("cmd-1", groupID, followerID, "Reboot", "restart the device", string(schemaJSON)); err != nil {
		t.Fatalf("CreateCommand: %v", err)
	}

	var cmd Command
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := GetCommand(groupID, "cmd-1")
		if err != nil {
			return false, err
		}
		return true, json.Unmarshal([]byte(out), &cmd)
	})
	if cmd.Name != "Reboot" || cmd.TargetPeerID != followerID || cmd.GroupID != groupID {
		t.Fatalf("GetCommand = %+v, unexpected", cmd)
	}
	if len(cmd.FormSchema) != 1 || cmd.FormSchema[0].Name != "delay_seconds" {
		t.Fatalf("GetCommand FormSchema = %+v, want one field named delay_seconds", cmd.FormSchema)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListCommands(groupID)
		if err != nil {
			return false, err
		}
		var cmds []Command
		if err := json.Unmarshal([]byte(out), &cmds); err != nil {
			return false, err
		}
		for _, c := range cmds {
			if c.ID == "cmd-1" {
				return true, nil
			}
		}
		return false, nil
	})

	if err := UpdateCommand("cmd-1", groupID, followerID, "Reboot Now", "restart immediately", ""); err != nil {
		t.Fatalf("UpdateCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := GetCommand(groupID, "cmd-1")
		if err != nil {
			return false, err
		}
		// A fresh Command per attempt: json.Unmarshal only overwrites
		// fields present in the source JSON, so reusing the outer cmd
		// across iterations would leave a stale FormSchema (omitempty,
		// absent once cleared) from an earlier revision's decode.
		var fresh Command
		if err := json.Unmarshal([]byte(out), &fresh); err != nil {
			return false, err
		}
		cmd = fresh
		return cmd.Name == "Reboot Now", nil
	})
	if len(cmd.FormSchema) != 0 {
		t.Fatalf("GetCommand after update FormSchema = %+v, want empty (update passed no schema)", cmd.FormSchema)
	}

	if err := DeleteCommand(groupID, "cmd-1"); err != nil {
		t.Fatalf("DeleteCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := GetCommand(groupID, "cmd-1")
		return err != nil, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		out, err := ListCommands(groupID)
		if err != nil {
			return false, err
		}
		var cmds []Command
		if err := json.Unmarshal([]byte(out), &cmds); err != nil {
			return false, err
		}
		return len(cmds) == 0, nil
	})

	if _, err := ListCommands("some-other-group-never-joined"); err == nil {
		t.Fatalf("ListCommands for non-participant group: want error, got none")
	}
}

// TestCatalogEmptyListsAreEmptyArrays checks ListGroups/ListCommands
// return "[]", never "null", when nothing matches -- same convention
// LogQuery already established.
func TestCatalogEmptyListsAreEmptyArrays(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	out, err := ListGroups()
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	if out != "[]" {
		t.Fatalf("ListGroups (empty) = %q, want %q", out, "[]")
	}

	grantSelfParticipation(t, "empty-group")
	out, err = ListCommands("empty-group")
	if err != nil {
		t.Fatalf("ListCommands: %v", err)
	}
	if out != "[]" {
		t.Fatalf("ListCommands (empty) = %q, want %q", out, "[]")
	}
}

// TestCatalogIDValidation checks CreateGroup rejects an empty or
// oversized id before ever touching the daemon.
func TestCatalogIDValidation(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := CreateGroup("", "x", "y"); err == nil {
		t.Fatalf("CreateGroup with empty id: want error, got none")
	}
	if err := CreateGroup(strings.Repeat("a", maxCatalogIDLen+1), "x", "y"); err == nil {
		t.Fatalf("CreateGroup with oversized id: want error, got none")
	}
}

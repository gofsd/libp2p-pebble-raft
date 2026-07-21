package kvctl_test

import (
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// fastRaftArgs shortens hashicorp/raft's WAN-appropriate default timeouts
// for a same-machine test -- see TestAddSetGetAcrossNodes's identical
// constant for why.
var fastRaftArgs = []string{
	"-raft-heartbeat-timeout", "300ms",
	"-raft-election-timeout", "300ms",
	"-raft-leader-lease-timeout", "250ms",
}

// pollUntilTrue retries check until it reports true, or fails the test
// after timeout -- every group-based ACL catalog write is a raft commit,
// asynchronously visible to a subsequent local read.
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

// TestGroupCRUD drives PutGroup/GetGroup/ListGroups/DeleteGroup against a
// real, single-node leader -- always a raft voter, so every write here
// succeeds unconditionally (no participation gate exists anymore, unlike
// the old logrecord-based scheme -- see catalog.go's doc comment).
func TestGroupCRUD(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	if _, err := kvctl.AddNodeWithArgs(root, fastRaftArgs); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	const groupID = "grp-1"
	if err := kvctl.PutGroup(groupID, "Group One"); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}

	var g kvctl.Group
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		g, err = kvctl.GetGroup(groupID)
		return err == nil, err
	})
	if g.ID != groupID || g.Name != "Group One" {
		t.Fatalf("GetGroup = %+v, unexpected", g)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groups, err := kvctl.ListGroups()
		if err != nil {
			return false, err
		}
		for _, gr := range groups {
			if gr.ID == groupID {
				return true, nil
			}
		}
		return false, nil
	})

	if err := kvctl.PutGroup(groupID, "Renamed"); err != nil {
		t.Fatalf("PutGroup (update): %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		g, err = kvctl.GetGroup(groupID)
		if err != nil {
			return false, err
		}
		return g.Name == "Renamed", nil
	})

	if err := kvctl.DeleteGroup(groupID); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetGroup(groupID)
		return err != nil, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groups, err := kvctl.ListGroups()
		if err != nil {
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

// TestCommandCRUD drives PutCommand/GetCommand/ListCommands/DeleteCommand.
func TestCommandCRUD(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutCommand("cmd-1", "Reboot", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}

	var cmd kvctl.Command
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		cmd, err = kvctl.GetCommand("cmd-1")
		return err == nil, err
	})
	if cmd.Name != "Reboot" || cmd.PeerID != leaderID {
		t.Fatalf("GetCommand = %+v, unexpected", cmd)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		commands, err := kvctl.ListCommands()
		if err != nil {
			return false, err
		}
		for _, c := range commands {
			if c.ID == "cmd-1" {
				return true, nil
			}
		}
		return false, nil
	})

	if err := kvctl.PutCommand("cmd-1", "Reboot Now", leaderID); err != nil {
		t.Fatalf("PutCommand (update): %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		fresh, err := kvctl.GetCommand("cmd-1")
		if err != nil {
			return false, err
		}
		cmd = fresh
		return cmd.Name == "Reboot Now", nil
	})

	if err := kvctl.DeleteCommand("cmd-1"); err != nil {
		t.Fatalf("DeleteCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-1")
		return err != nil, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		commands, err := kvctl.ListCommands()
		if err != nil {
			return false, err
		}
		return len(commands) == 0, nil
	})
}

// TestGroupCommandAndPeerGroupLinkingGatesSubmitCommand drives the full
// group-based ACL chain end to end: a peer with no PeerGroup membership
// at all must be refused by SubmitCommand; linking commandID to a group
// (CreateGroupCommand) alone still isn't enough; adding the peer to that
// group (AddPeerToGroup) is what finally permits it; removing the peer
// from the group revokes access again.
func TestGroupCommandAndPeerGroupLinkingGatesSubmitCommand(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutGroup("grp-1", "Group One"); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand("cmd-1", "Reboot", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-1")
		return err == nil, nil
	})

	if _, err := kvctl.SubmitCommand("cmd-1", ""); err == nil {
		t.Fatalf("SubmitCommand before any group link: want error, got none")
	}

	if err := kvctl.CreateGroupCommand("cmd-1", "grp-1"); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForCommand("cmd-1")
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 1 && groupIDs[0] == "grp-1", nil
	})

	// Linked to a group, but leaderID isn't a member of it yet.
	if _, err := kvctl.SubmitCommand("cmd-1", ""); err == nil {
		t.Fatalf("SubmitCommand before peer joined the group: want error, got none")
	}

	if err := kvctl.AddPeerToGroup(leaderID, "grp-1"); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForPeer(leaderID)
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 1 && groupIDs[0] == "grp-1", nil
	})

	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = kvctl.SubmitCommand("cmd-1", `{"delay":5}`)
		return err == nil, err
	})
	if instanceID == "" {
		t.Fatalf("SubmitCommand returned empty instance id")
	}

	if err := kvctl.RemovePeerFromGroup(leaderID, "grp-1"); err != nil {
		t.Fatalf("RemovePeerFromGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.SubmitCommand("cmd-1", "")
		return err != nil, nil
	})
}

// TestDeleteGroupCascadesToRelations checks DeleteGroup removes every
// GroupCommand/PeerGroup record referencing it (kvfsm.OpCascadeDelete),
// so a peer that was only permitted via the deleted group loses access,
// and ListGroupsForCommand/ListGroupsForPeer no longer mention it.
func TestDeleteGroupCascadesToRelations(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutGroup("grp-cascade", "Cascade Group"); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand("cmd-cascade", "Reboot", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-cascade")
		return err == nil, nil
	})
	if err := kvctl.CreateGroupCommand("cmd-cascade", "grp-cascade"); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	if err := kvctl.AddPeerToGroup(leaderID, "grp-cascade"); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.SubmitCommand("cmd-cascade", "")
		return err == nil, err
	})

	if err := kvctl.DeleteGroup("grp-cascade"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForCommand("cmd-cascade")
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 0, nil
	})
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForPeer(leaderID)
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 0, nil
	})
	if _, err := kvctl.SubmitCommand("cmd-cascade", ""); err == nil {
		t.Fatalf("SubmitCommand after group cascade-deleted: want error, got none")
	}
}

// TestDeleteCommandCascadesToGroupCommand checks DeleteCommand removes
// every GroupCommand record referencing it.
func TestDeleteCommandCascadesToGroupCommand(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutGroup("grp-cmd-cascade", "Group"); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand("cmd-to-delete", "Reboot", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-to-delete")
		return err == nil, nil
	})
	if err := kvctl.CreateGroupCommand("cmd-to-delete", "grp-cmd-cascade"); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForCommand("cmd-to-delete")
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 1, nil
	})

	if err := kvctl.DeleteCommand("cmd-to-delete"); err != nil {
		t.Fatalf("DeleteCommand: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		groupIDs, err := kvctl.ListGroupsForCommand("cmd-to-delete")
		if err != nil {
			return false, err
		}
		return len(groupIDs) == 0, nil
	})
}

// TestCatalogEmptyListsAreEmptyArrays checks ListGroups/ListCommands
// return a zero-length (nil) slice, not an error, when nothing matches.
func TestCatalogEmptyListsAreEmptyArrays(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	if _, err := kvctl.AddNodeWithArgs(root, fastRaftArgs); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	groups, err := kvctl.ListGroups()
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("ListGroups (empty) = %+v, want none", groups)
	}

	commands, err := kvctl.ListCommands()
	if err != nil {
		t.Fatalf("ListCommands: %v", err)
	}
	if len(commands) != 0 {
		t.Fatalf("ListCommands (empty) = %+v, want none", commands)
	}
}

// TestCatalogIDValidation checks PutGroup rejects an empty or oversized
// id before ever touching the daemon.
func TestCatalogIDValidation(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	if _, err := kvctl.AddNodeWithArgs(root, fastRaftArgs); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := kvctl.PutGroup("", "x"); err == nil {
		t.Fatalf("PutGroup with empty id: want error, got none")
	}
	oversized := make([]byte, 257)
	for i := range oversized {
		oversized[i] = 'a'
	}
	if err := kvctl.PutGroup(string(oversized), "x"); err == nil {
		t.Fatalf("PutGroup with oversized id: want error, got none")
	}
}

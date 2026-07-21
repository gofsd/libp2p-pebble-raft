package kvctl_test

import (
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestSubmitCommandIndexesExecutionsByPeer drives SubmitCommand and
// checks ListExecutionsByPeer surfaces the resulting dispatch under both
// the requester's and the target's peer id, with the right Role each
// time -- mirrors mobile/kvmobile/dispatch_test.go's identical test.
func TestSubmitCommandIndexesExecutionsByPeer(t *testing.T) {
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

	const groupID = "grp-exec"
	const targetPeerID = "some-target-peer-id"

	if err := kvctl.PutGroup(groupID, "Exec Group"); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand("cmd-1", "Reboot", targetPeerID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand("cmd-1", groupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	if err := kvctl.AddPeerToGroup(leaderID, groupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-1")
		return err == nil, nil
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

	findExecution := func(execs []kvctl.CommandExecution) (kvctl.CommandExecution, bool) {
		for _, e := range execs {
			if e.InstanceID == instanceID {
				return e, true
			}
		}
		return kvctl.CommandExecution{}, false
	}

	var requesterEntry kvctl.CommandExecution
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		execs, err := kvctl.ListExecutionsByPeer(leaderID)
		if err != nil {
			return false, err
		}
		e, ok := findExecution(execs)
		requesterEntry = e
		return ok, nil
	})
	if requesterEntry.Role != "requester" || requesterEntry.RequestedBy != leaderID ||
		requesterEntry.TargetPeerID != targetPeerID || requesterEntry.CommandID != "cmd-1" {
		t.Fatalf("ListExecutionsByPeer(requester) entry = %+v, unexpected", requesterEntry)
	}

	var targetEntry kvctl.CommandExecution
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		execs, err := kvctl.ListExecutionsByPeer(targetPeerID)
		if err != nil {
			return false, err
		}
		e, ok := findExecution(execs)
		targetEntry = e
		return ok, nil
	})
	if targetEntry.Role != "target" {
		t.Fatalf("ListExecutionsByPeer(target) entry role = %q, want %q", targetEntry.Role, "target")
	}
}

// TestSubmitCommandSelfTargetWritesOneIndexEntry checks SubmitCommand
// doesn't double-index a dispatch when the requester is also the
// command's target.
func TestSubmitCommandSelfTargetWritesOneIndexEntry(t *testing.T) {
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

	const groupID = "grp-self-target"

	if err := kvctl.PutGroup(groupID, "Self Target Group"); err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if err := kvctl.PutCommand("cmd-self", "Self", leaderID); err != nil {
		t.Fatalf("PutCommand: %v", err)
	}
	if err := kvctl.CreateGroupCommand("cmd-self", groupID); err != nil {
		t.Fatalf("CreateGroupCommand: %v", err)
	}
	if err := kvctl.AddPeerToGroup(leaderID, groupID); err != nil {
		t.Fatalf("AddPeerToGroup: %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		_, err := kvctl.GetCommand("cmd-self")
		return err == nil, nil
	})

	var instanceID string
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		instanceID, err = kvctl.SubmitCommand("cmd-self", "")
		return err == nil, err
	})

	var execs []kvctl.CommandExecution
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		var err error
		execs, err = kvctl.ListExecutionsByPeer(leaderID)
		return err == nil, err
	})

	count := 0
	for _, e := range execs {
		if e.InstanceID == instanceID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("ListExecutionsByPeer(self) has %d entries for instance %s, want exactly 1", count, instanceID)
	}
}

// TestLatestCommandLog drives AppendCommandLog + LatestCommandLog: it
// must error before any entry exists for an instance, and always
// reflect whichever entry was appended most recently.
func TestLatestCommandLog(t *testing.T) {
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

	const instanceID = "instance-latest-1"

	if _, err := kvctl.LatestCommandLog(instanceID); err == nil {
		t.Fatalf("LatestCommandLog before any entry: want error, got none")
	}

	if err := kvctl.AppendCommandLog("", instanceID, map[string]string{"status": "running"}, "starting up"); err != nil {
		t.Fatalf("AppendCommandLog (1): %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		rec, err := kvctl.LatestCommandLog(instanceID)
		if err != nil {
			return false, err
		}
		return rec.Narrative == "starting up", nil
	})

	if err := kvctl.AppendCommandLog("", instanceID, map[string]string{"status": "done"}, "finished ok"); err != nil {
		t.Fatalf("AppendCommandLog (2): %v", err)
	}
	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		rec, err := kvctl.LatestCommandLog(instanceID)
		if err != nil {
			return false, err
		}
		return rec.Narrative == "finished ok" && rec.Fields["status"] == "done", nil
	})
}

// TestLatestCommandLogReturnsOutputIntact checks a normal-sized
// AppendCommandLog entry round-trips through LatestCommandLog intact --
// see mobile/kvmobile/dispatch_test.go's identical test for why this
// doesn't need its own truncation logic (shmevent.ValueSize already
// bounds every write).
func TestLatestCommandLogReturnsOutputIntact(t *testing.T) {
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

	const instanceID = "instance-latest-normal"
	output := "exit code 0: " + strings.Repeat("ok ", 50)
	if err := kvctl.AppendCommandLog("", instanceID, map[string]string{"status": "done"}, output); err != nil {
		t.Fatalf("AppendCommandLog: %v", err)
	}

	pollUntilTrue(t, 10*time.Second, func() (bool, error) {
		rec, err := kvctl.LatestCommandLog(instanceID)
		if err != nil {
			return false, err
		}
		if rec.Narrative != output {
			return false, nil
		}
		return true, nil
	})
}

// TestAppendCommandLogRejectsOversizedEntry checks AppendCommandLog
// surfaces a clear error for a narrative too large for
// shmevent.ValueSize, rather than silently accepting or corrupting it.
func TestAppendCommandLogRejectsOversizedEntry(t *testing.T) {
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

	hugeOutput := strings.Repeat("x", shmevent.ValueSize*4)
	if err := kvctl.AppendCommandLog("", "instance-oversized", nil, hugeOutput); err == nil {
		t.Fatalf("AppendCommandLog with oversized narrative: want error, got none")
	}
}

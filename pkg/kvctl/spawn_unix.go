//go:build !windows

package kvctl

import (
	"os"
	"os/exec"
	"syscall"
)

// detach configures cmd to run in its own session, so it survives the
// spawning mage process exiting.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

// isAlive reports whether pid refers to a running process. os.FindProcess
// always succeeds on Unix regardless of whether pid exists, so liveness has
// to be tested with a signal-0 probe (a standard Unix idiom: signal 0 does
// nothing but still reports ESRCH if the process is gone).
func isAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

// terminate asks pid to shut down gracefully (SIGTERM, caught by the
// daemon's normal ctx-cancellation shutdown path -- see pkg/daemon.Run) --
// used by Leave/Rm to restart a node against a different data dir without
// requiring the operator to stop it manually first, unlike every other
// kvctl operation.
func terminate(pid int) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return proc.Signal(syscall.SIGTERM)
}

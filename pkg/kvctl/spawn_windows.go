//go:build windows

package kvctl

import (
	"os"
	"os/exec"
)

// detach is a no-op on Windows for now: the spawned process still gets its
// own console-less process (no SysProcAttr needed for the desktop MVP), but
// it is not fully decoupled from the parent's job object the way the unix
// Setsid path is. Revisit if Windows needs real session detachment.
func detach(cmd *exec.Cmd) {}

// isAlive reports whether pid refers to a running process. Unlike Unix,
// os.FindProcess on Windows actually opens a handle to the process and
// fails if it doesn't exist, so no extra signal probe is needed (and a
// signal-0-style probe wouldn't work here anyway -- os.Process.Signal only
// supports os.Kill on Windows).
func isAlive(pid int) bool {
	_, err := os.FindProcess(pid)
	return err == nil
}

// terminate asks pid to shut down -- a hard kill on Windows (see
// isAlive's doc comment: os.Process.Signal only supports os.Kill here, no
// graceful-shutdown signal equivalent to Unix's SIGTERM is available
// through this API) -- used by Leave/Rm to restart a node against a
// different data dir without requiring the operator to stop it manually
// first, unlike every other kvctl operation.
func terminate(pid int) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return proc.Kill()
}

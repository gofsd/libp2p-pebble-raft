//go:build windows

package kvctl

import "os/exec"

// detach is a no-op on Windows for now: the spawned process still gets its
// own console-less process (no SysProcAttr needed for the desktop MVP), but
// it is not fully decoupled from the parent's job object the way the unix
// Setsid path is. Revisit if Windows needs real session detachment.
func detach(cmd *exec.Cmd) {}

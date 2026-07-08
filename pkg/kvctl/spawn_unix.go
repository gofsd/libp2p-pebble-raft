//go:build !windows

package kvctl

import (
	"os/exec"
	"syscall"
)

// detach configures cmd to run in its own session, so it survives the
// spawning mage process exiting.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

//go:build !windows

package e2erun

import (
	"os"
	"syscall"
)

// isAlive reports whether pid refers to a running process -- same
// signal-0-probe idiom as pkg/kvctl's isAlive (os.FindProcess always
// succeeds on Unix regardless of whether pid exists).
func isAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

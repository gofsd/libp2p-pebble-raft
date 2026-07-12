//go:build windows

package e2erun

import "os"

// isAlive mirrors pkg/kvctl's Windows isAlive: os.FindProcess actually
// opens a handle and fails if the pid doesn't exist, so no extra probe is
// needed.
func isAlive(pid int) bool {
	_, err := os.FindProcess(pid)
	return err == nil
}

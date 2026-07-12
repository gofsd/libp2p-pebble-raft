package e2erun

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
)

// sshRun runs remoteCmd on host via `ssh host remoteCmd` (a single string,
// executed by the remote shell -- callers must quote their own arguments),
// streaming stdout/stderr to this process's, and returns an error including
// captured output on failure.
func sshRun(host, remoteCmd string) error {
	cmd := exec.Command("ssh", host, remoteCmd)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ssh %s %q: %w\n%s", host, remoteCmd, err, out.String())
	}
	return nil
}

// sshOutput is like sshRun but returns stdout on success (stderr still
// folded into the error on failure).
func sshOutput(host, remoteCmd string) (string, error) {
	cmd := exec.Command("ssh", host, remoteCmd)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("ssh %s %q: %w\n%s", host, remoteCmd, err, stderr.String())
	}
	return stdout.String(), nil
}

// sshOutputAnyExit is like sshOutput but always returns whatever the
// remote command wrote to stdout/stderr, even on a nonzero exit --
// callers whose remote command prints meaningful output before exiting
// nonzero (kvctl-cli sendevent does this for its EventError case) need
// that output, not just an opaque error.
func sshOutputAnyExit(host, remoteCmd string) (stdout, stderr string, err error) {
	cmd := exec.Command("ssh", host, remoteCmd)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err = cmd.Run()
	return outBuf.String(), errBuf.String(), err
}

// scp copies localPath to host:remotePath.
func scp(localPath, host, remotePath string) error {
	cmd := exec.Command("scp", localPath, host+":"+remotePath)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("scp %s %s:%s: %w", localPath, host, remotePath, err)
	}
	return nil
}

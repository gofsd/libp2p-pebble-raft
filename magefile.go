//go:build mage

package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// Default target to run if none is specified
var Default = Status

// Status prints the current latest version based on Git tags
func Status() error {
	v, err := getCurrentVersion()
	if err != nil {
		fmt.Println("No current version found. Start by bumping a version (e.g., mage patch).")
		return nil
	}
	fmt.Printf("Current Version: %s\n", v.String())
	return nil
}

// Patch bumps the patch version (e.g., 1.0.0 -> 1.0.1)
func Patch() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncPatch() }, "")
}

// Minor bumps the minor version (e.g., 1.0.0 -> 1.1.0)
func Minor() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncMinor() }, "")
}

// Major bumps the major version (e.g., 1.0.0 -> 2.0.0)
func Major() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncMajor() }, "")
}

// Alpha bumps or creates an alpha prerelease stage (e.g., 1.0.0 -> 1.0.1-alpha.1)
func Alpha() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncPatch() }, "alpha")
}

// Beta bumps or creates a beta prerelease stage (e.g., 1.0.0 -> 1.0.1-beta.1)
func Beta() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncPatch() }, "beta")
}

// RC bumps or creates a release candidate stage (e.g., 1.0.0 -> 1.0.1-rc.1)
func RC() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncPatch() }, "rc")
}

// --- Helper Functions ---

func getCurrentVersion() (*semver.Version, error) {
	// Get the latest tag from git sorted by creation date
	cmd := exec.Command("git", "describe", "--tags", "--abbrev=0")
	out, err := cmd.Output()
	if err != nil {
		// If no tags exist, start at v0.0.0
		return semver.NewVersion("v0.0.0")
	}

	tag := strings.TrimSpace(string(out))
	return semver.NewVersion(tag)
}

func bump(bumpFn func(*semver.Version) semver.Version, stage string) error {
	current, err := getCurrentVersion()
	if err != nil {
		return err
	}

	var next semver.Version

	if stage != "" {
		// If it's already on the same prerelease stage, increment the prerelease number (e.g., alpha.1 -> alpha.2)
		if strings.Contains(current.Prerelease(), stage) {
			// FIX: IncPatch() only returns 1 value, no error.
			next = current.IncPatch()

			// Re-apply the pre-release increment strategy
			parts := strings.Split(current.Prerelease(), ".")
			if len(parts) == 2 {
				var num int
				fmt.Sscanf(parts[1], "%d", &num)
				next, _ = current.SetPrerelease(fmt.Sprintf("%s.%d", stage, num+1))
			}
		} else {
			// Brand new prerelease stage: Bump patch and append stage.1 (e.g., 1.0.1-alpha.1)
			temp := bumpFn(current)
			next, err = temp.SetPrerelease(stage + ".1")
			if err != nil {
				return err
			}
		}
	} else {
		// Standard production release (clears out any alpha/beta tags)
		next = bumpFn(current)
	}

	nextTag := "v" + next.String()
	fmt.Printf("Bumping version: %s ➡️ %s\n", "v"+current.String(), nextTag)

	// Create git tag
	gitTag := exec.Command("git", "tag", "-a", nextTag, "-m", fmt.Sprintf("Release %s", nextTag))
	gitTag.Stdout = os.Stdout
	gitTag.Stderr = os.Stderr
	if err := gitTag.Run(); err != nil {
		return fmt.Errorf("failed to create git tag: %w", err)
	}

	fmt.Printf("Successfully created tag %s! Run 'git push --tags' to deploy.\n", nextTag)
	return nil
}

// Test runs only the fast unit tests (ignoring integration and e2e)
func Test() error {
	fmt.Println("Running Unit Tests...")
	// We pass -short flag so you can optionally skip longer unit tests if needed
	cmd := exec.Command("go", "test", "-v", "-short", "./...")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Integration runs both unit tests and integration tests
func Integration() error {
	fmt.Println("Running Integration Tests...")
	// -tags=integration tells Go to include files with the '//go:build integration' tag
	cmd := exec.Command("go", "test", "-v", "-tags=integration", "./...")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// E2e runs the end-to-end test suite
func E2e() error {
	fmt.Println("Running End-to-End (E2E) Tests...")
	// -tags=e2e tells Go to include files with the '//go:build e2e' tag
	cmd := exec.Command("go", "test", "-v", "-tags=e2e", "./...")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// TestAll runs absolutely every test type sequentially
func TestAll() error {
	fmt.Println("Executing complete test matrix...")

	if err := Test(); err != nil {
		return err
	}
	if err := Integration(); err != nil {
		return err
	}
	if err := E2e(); err != nil {
		return err
	}

	fmt.Println("🎉 All test suites passed successfully!")
	return nil
}

// TestCurrent runs ONLY the tests that have been created or modified against origin/main
func TestCurrent() error {
	fmt.Println("🔍 Detecting changed or new test files...")

	// 1. Get the list of changed files compared to the main remote branch
	// (You can change "origin/main" to a specific tag like "v1.0.0" if preferred)
	cmd := exec.Command("git", "diff", "origin/main", "--name-only")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to check git diff: %w. Make sure you have fetched from remote", err)
	}

	// 2. Filter for files that are Go tests and track their packages (directories)
	changedPackages := make(map[string]bool)
	files := strings.Split(out.String(), "\n")

	for _, file := range files {
		file = strings.TrimSpace(file)
		// Only care about Go test files that still exist (weren't deleted)
		if strings.HasSuffix(file, "_test.go") {
			if _, err := os.Stat(file); err == nil {
				// Convert file path to package path (e.g., "calculator/calc_test.go" -> "./calculator")
				dir := filepath.Dir(file)
				pkg := "./" + dir
				changedPackages[pkg] = true
			}
		}
	}

	// 3. If no tests changed, exit early
	if len(changedPackages) == 0 {
		fmt.Println("✅ No new or updated tests found in this version.")
		return nil
	}

	// 4. Build the go test command for the specific packages
	var pkgs []string
	for pkg := range changedPackages {
		pkgs = append(pkgs, pkg)
	}

	fmt.Printf("🚀 Running tests in modified packages: %s\n", strings.Join(pkgs, ", "))

	testArgs := append([]string{"test", "-v"}, pkgs...)
	testCmd := exec.Command("go", testArgs...)
	testCmd.Stdout = os.Stdout
	testCmd.Stderr = os.Stderr

	return testCmd.Run()
}

// Build compiles the project for multiple platforms
func Build() {
	mg.Deps(BuildLinux, BuildWindows, BuildAndroid)
}

func BuildLinux() error {
	fmt.Println("Building for Linux...")
	// Implementation for go build...
	return nil
}

func BuildWindows() error {
	fmt.Println("Building for Windows...")
	// Implementation for go build...
	return nil
}

func BuildAndroid() error {
	fmt.Println("Building Android AAR...")
	// Implementation for gomobile bind...
	return nil
}

// BuildAndRunDocker builds the relay image and recreates the container if it already exists.
// Usage: mage buildandrundocker
func BuildAndRunDocker() error {
	const (
		imageName     = "p2p-relay-app"
		containerName = "p2p-relay-container"
	)

	fmt.Println("🐳 Checking for existing Docker container...")

	// Check if the container exists (running or stopped)
	// 'docker ps -a -q' returns the container ID if found, or empty string if not
	id, _ := sh.Output("docker", "ps", "-a", "-q", "-f", "name=^/"+containerName+"$")

	if id != "" {
		fmt.Printf("🔄 Found existing container (%s). Recreating...\n", containerName)

		// Stop the container if it's currently running
		_ = sh.Run("docker", "stop", containerName)

		// Remove the old container
		if err := sh.Run("docker", "rm", containerName); err != nil {
			return fmt.Errorf("failed to remove old container: %w", err)
		}
		fmt.Println("🗑️  Old container removed successfully.")
	}

	// 1. Build the new Docker image from the current directory
	fmt.Printf("🛠️  Building Docker image: %s...\n", imageName)
	if err := sh.RunV("docker", "build", "-t", imageName, "."); err != nil {
		return fmt.Errorf("docker build failed: %w", err)
	}

	// 2. Run the new container in detached mode (-d)
	fmt.Printf("🚀 Launching new container: %s...\n", containerName)
	if err := sh.RunV("docker", "run", "-d", "--name", containerName, "-p", "4001:4001", imageName); err != nil {
		return fmt.Errorf("failed to run new docker container: %w", err)
	}

	fmt.Fprintln(os.Stderr, "\n=======================================================")
	fmt.Println("✅ Relay container is running in the background!")
	fmt.Println("👉 To view the live logs & Multiaddr, run:")
	fmt.Printf("   docker logs %s\n", containerName)
	fmt.Println("👉 To read the relay.txt file from inside the container, run:")
	fmt.Printf("   docker exec %s cat relay.txt\n", containerName)
	fmt.Fprintln(os.Stderr, "=======================================================")

	return nil
}

// BuildAndRunPodman builds the relay image and recreates the container using Podman.
// Usage: mage buildandrunpodman
func BuildAndRunPodman() error {
	const (
		imageName     = "localhost/p2p-relay-app"
		containerName = "p2p-relay-container"
	)

	fmt.Println("🦭 Checking for existing Podman container...")

	// Check if the container exists (running or stopped)
	// 'podman ps -a -q' returns the container ID if found
	id, _ := sh.Output("podman", "ps", "-a", "-q", "-f", "name=^/"+containerName+"$")

	if id != "" {
		fmt.Printf("🔄 Found existing container (%s). Recreating...\n", containerName)

		// Stop the container if it's currently running
		_ = sh.Run("podman", "stop", containerName)

		// Remove the old container
		if err := sh.Run("podman", "rm", containerName); err != nil {
			return fmt.Errorf("failed to remove old container: %w", err)
		}
		fmt.Println("🗑️  Old container removed successfully.")
	}

	// 1. Build the new Podman image from the current directory
	fmt.Printf("🛠️  Building Podman image: %s...\n", imageName)
	if err := sh.RunV("podman", "build", "-t", imageName, "."); err != nil {
		return fmt.Errorf("podman build failed: %w", err)
	}

	// 2. Run the new container in detached mode (-d)
	fmt.Printf("🚀 Launching new container: %s...\n", containerName)
	fmt.Printf("🚀 Launching new container using Host Networking: %s...\n", containerName)
	if err := sh.RunV("podman", "run", "-d", "--name", containerName, "--net=host", imageName); err != nil {
		return fmt.Errorf("failed to run new podman container: %w", err)
	}

	fmt.Fprintln(os.Stderr, "\n=======================================================")
	fmt.Println("✅ Relay container is running via Podman in the background!")
	fmt.Println("👉 To view the live logs & Multiaddr, run:")
	fmt.Printf("   podman logs %s\n", containerName)
	fmt.Println("👉 To read the relay.txt file from inside the container, run:")
	fmt.Printf("   podman exec %s cat relay.txt\n", containerName)
	fmt.Fprintln(os.Stderr, "=======================================================")

	return nil
}

// Shell attaches an interactive terminal to the running relay container.
// Usage: mage shell
func Shell() error {
	const containerName = "p2p-relay-container"

	// Determine if we should use podman or docker based on what is installed/running
	runtime := "docker"
	if _, err := sh.Output("podman", "--version"); err == nil {
		runtime = "podman"
	}

	fmt.Printf("🐚 Attaching interactive shell into %s (%s)...\n", containerName, runtime)
	fmt.Println("👉 Type 'exit' to disconnect without stopping the relay.")
	fmt.Println("-------------------------------------------------------")

	// sh.RunV automatically handles tying os.Stdin, os.Stdout, and os.Stderr
	// to your terminal so interactive CLI commands work perfectly.
	return sh.RunV(runtime, "exec", "-it", containerName, "/bin/sh")
}

// AddNode spawns a new node process and bootstraps it as the cluster's sole
// leader. mage requires every target to take a fixed number of arguments
// (no optional/variadic CLI args), so the three addnode shapes described in
// the project's design (0, 1, or 2 peer-id arguments) are exposed as three
// targets -- AddNode, AddFollower, RejoinNode -- that all delegate to the
// same underlying kvctl.AddNode(repoRoot, peerIDs...).
//
// Usage: mage addnode
func AddNode() error {
	return runAddNode()
}

// AddFollower spawns a new node process and joins it to the cluster led by
// leaderPeerID.
//
// Usage: mage addfollower <leaderPeerID>
func AddFollower(leaderPeerID string) error {
	return runAddNode(leaderPeerID)
}

// RejoinNode restarts the existing node ownPeerID (reusing its data
// directory and identity) and (re)joins it to leaderPeerID. Use this when
// the node's address changed since it went down, or a different/new
// leader needs to be told about it; if neither is true, ResumeNode is
// simpler (no leader coordination at all).
//
// Usage: mage rejoinnode <leaderPeerID> <ownPeerID>
func RejoinNode(leaderPeerID, ownPeerID string) error {
	return runAddNode(leaderPeerID, ownPeerID)
}

// ResumeNode restarts the existing node ownPeerID in place -- reusing its
// data directory and identity -- with no leader coordination at all. The
// daemon recognizes it already has persisted raft state and resumes
// operating on it directly. Use this when the node's address hasn't
// changed since it went down (a pinned -listen-port makes that reliable);
// otherwise use RejoinNode.
//
// Usage: mage resumenode <ownPeerID>
func ResumeNode(ownPeerID string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	peerID, err := kvctl.ResumeNode(root, ownPeerID)
	if err != nil {
		return err
	}
	fmt.Printf("✅ node %s resumed and selected as current\n", peerID)
	return nil
}

func runAddNode(peerIDs ...string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	peerID, err := kvctl.AddNode(root, peerIDs...)
	if err != nil {
		return err
	}
	fmt.Printf("✅ node %s is up and selected as current\n", peerID)
	return nil
}

// Use selects which node Set/Get target, by peer id.
// Usage: mage use <peerID>
func Use(peerID string) error {
	if err := kvctl.Use(peerID); err != nil {
		return err
	}
	fmt.Printf("current node set to %s\n", peerID)
	return nil
}

// Set stores key=value through raft on the current node.
// Usage: mage set <key> <value>
func Set(key, value string) error {
	return kvctl.Set(key, value)
}

// Get reads key from the current node's local state.
// Usage: mage get <key>
func Get(key string) error {
	value, err := kvctl.Get(key)
	if err != nil {
		return err
	}
	fmt.Println(value)
	return nil
}

// repoRoot returns the directory this magefile lives in, so AddNode can
// `go build ./cmd/kvnode` regardless of the directory mage was invoked from.
func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("addnode: cannot determine repo root")
	}
	return filepath.Dir(file), nil
}

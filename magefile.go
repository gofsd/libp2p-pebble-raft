//go:build mage

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2erun"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
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

// E2E groups the end-to-end test/deploy pipeline behind `mage e2e:<method>`
// -- see pkg/e2edata for the single testdata file format (versions, node
// identities, test rows) and pkg/e2erun for what actually deploying and
// running a row means per platform.
//
// This replaced a stub `E2e()` target that ran `go test -tags=e2e ./...`
// against a build tag no file in this repo ever used -- i.e. it always
// silently did nothing. mg.Namespace also can't coexist with a same-named
// bare function target (mage's CLI target discovery would collide "e2e"
// the function against "e2e:" the namespace prefix), so replacing it was
// required, not optional, to add these targets at all.
type E2E mg.Namespace

func testdataPath() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, e2edata.DefaultPath), nil
}

// NewVersion records a new e2e test version stamped with this repo's
// current semver (from git tags -- the same version `mage patch`/`minor`/
// `major` manage), shared across every platform's implementation since
// this is one monorepo with one release version, not per-platform version
// numbers (see e2edata.File.Versions' doc comment). Subsequent
// `e2e:addtest` calls target it instead of whatever version was current
// before.
//
// Usage: mage e2e:newversion
func (E2E) NewVersion() error {
	v, err := getCurrentVersion()
	if err != nil {
		return err
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	id := f.NewVersion(v.String())
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ version %d: %s\n", id, v.String())
	return nil
}

// AddNode generates a fresh deterministic Ed25519 identity for platform
// ("desktop", "android", "web", or "remote" -- the SSH-deployed bootstrap
// leader, though e2e:bootstrap provisions that one automatically if it
// doesn't exist yet, so this is rarely needed for "remote" directly) and
// records it, printing the node id later e2e:addtest calls reference.
//
// Usage: mage e2e:addnode <platform>
func (E2E) AddNode(platform string) error {
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	id, node, err := f.AddNode(e2edata.Platform(platform))
	if err != nil {
		return err
	}
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ node %d (%s): %s\n", id, node.Platform, node.PeerID)
	return nil
}

// AddTest appends a test row against the current (not yet published)
// version -- creating version 1 automatically if none exists yet -- that
// sends one raw pkg/shmevent to nodeID: eventName is the event's name (see
// pkg/shmevent.EventName -- "set_key", "set_field", "get_key", "get_field",
// "get_public_key", "get_private_key", "add"), id is this message's own
// correlation id (pick a nonzero value and reuse it as a later row's
// sourceID/destID to link them -- e.g. a set_key row with id=100 followed
// by a set_field row with sourceID=100 -- since pkg/e2erun dispatches rows
// through kvctl-cli sendevent, which only randomizes an id left at its
// zero value, an explicit id here is preserved exactly through to the
// wire), sourceID/destID are the relational reference fields (0 for
// unused), and value is plain text (see pkg/e2edata.Event's doc comment on
// how binary values are represented). An "add" row's value may be the
// literal string "BOOTSTRAP" to mean "the live bootstrap leader's address,
// whatever it is at run time" (see pkg/e2erun.BootstrapToken) instead of a
// frozen address.
//
// Usage: mage e2e:addtest <nodeID> <eventName> <id> <sourceID> <destID> <value>
func (E2E) AddTest(nodeID int, eventName string, id, sourceID, destID int, value string) error {
	eventType, ok := shmevent.EventFromName(eventName)
	if !ok {
		return fmt.Errorf("e2e:addtest: unknown event name %q (want one of: set_key, set_field, get_key, get_field, get_public_key, get_private_key, add)", eventName)
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	ev := e2edata.NewEvent(eventType, uint16(sourceID), uint16(destID), []byte(value), uint16(id))
	row, err := f.AddTest(nodeID, ev)
	if err != nil {
		return err
	}
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ added row: version %d, node %d, event %s\n", row.Version, row.Node, eventName)
	return nil
}

// DeleteNode tears down whatever real process/data nodeID's platform has
// running -- a local kvnode process for a desktop node, or the SSH
// bootstrap daemon and its entire remote directory for the remote node
// (see pkg/e2erun.DeleteNode) -- then removes it from the testdata file.
// Nodes are never deleted automatically by e2e:current/e2e:all, precisely
// so a deployed node stays around for a human to poke at; this is the
// explicit, deliberate teardown command for when that's no longer wanted.
//
// Usage: mage e2e:deletenode <nodeID>
func (E2E) DeleteNode(nodeID int) error {
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	if err := e2erun.DeleteNode(f, nodeID); err != nil {
		return err
	}
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ node %d deleted\n", nodeID)
	return nil
}

// DestroyAll tears down every node currently in the testdata file at once --
// the same real teardown DeleteNode does (kill the local desktop process,
// kill the SSH bootstrap daemon and wipe its whole remote directory), just
// for every node id instead of naming one. Android/web nodes have no
// persistent process this pipeline manages, so "destroying" them is just
// removing their testdata.json entry (see pkg/e2erun.DeleteNode's doc
// comment).
//
// Saves the file even if some node's teardown failed, so whichever nodes
// *did* get torn down aren't left looking like they're still around --
// e2erun.DeleteAllNodes's own doc comment covers why one failure doesn't
// stop the rest.
//
// Usage: mage e2e:destroyall
func (E2E) DestroyAll() error {
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	destroyErr := e2erun.DeleteAllNodes(f)
	if err := f.Save(path); err != nil {
		return err
	}
	if destroyErr != nil {
		return destroyErr
	}
	fmt.Println("✅ all nodes destroyed")
	return nil
}

// Bootstrap deploys (or confirms already running, idempotently) the shared
// e2e bootstrap/leader node on the SSH server -- see
// pkg/e2erun.EnsureBootstrap for exactly what that involves and how it
// avoids disturbing any other node already running there.
//
// Usage: mage e2e:bootstrap
func (E2E) Bootstrap() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	multiaddr, webTransportAddr, peerID, err := e2erun.EnsureBootstrap(root, path, f)
	if err != nil {
		return err
	}
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ bootstrap %s ready at %s (webtransport: %s)\n", peerID, multiaddr, webTransportAddr)
	return nil
}

// BootstrapAll ensures every node currently recorded in the testdata file
// has its real process up and running, without running any test rows: the
// SSH-deployed remote leader (same as e2e:bootstrap -- and, same as that
// command, this provisions a fresh remote identity first if the file has
// none yet), plus a real local kvnode process for every desktop node.
// Android/web nodes have no persistent process to pre-start -- see
// pkg/e2erun.EnsureAllDesktopNodes's doc comment -- e2e:current/e2e:all
// drive those fresh each time regardless.
//
// Usage: mage e2e:bootstrapall
func (E2E) BootstrapAll() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	multiaddr, webTransportAddr, peerID, err := e2erun.EnsureBootstrap(root, path, f)
	if err != nil {
		return err
	}
	if err := f.Save(path); err != nil {
		return err
	}
	fmt.Printf("✅ bootstrap %s ready at %s (webtransport: %s)\n", peerID, multiaddr, webTransportAddr)

	return e2erun.EnsureAllDesktopNodes(root, f)
}

// Current runs only the rows recorded since the last published version --
// what should run before every push (see the pre-push hook in
// scripts/git-hooks/pre-push). On full success it advances
// PublishedVersion, so the next e2e:current only covers whatever's new
// again -- this is the "version increment based on new tests" behavior.
//
// Usage: mage e2e:current
func (E2E) Current() error {
	return runE2ERows(func(f *e2edata.File) []int { return f.PendingRows() }, true)
}

// All runs every recorded test row across every version, regardless of
// what's already published -- a full regression pass.
//
// Usage: mage e2e:all
func (E2E) All() error {
	return runE2ERows(func(f *e2edata.File) []int { return f.AllRowIndices() }, false)
}

func runE2ERows(selectRows func(*e2edata.File) []int, markPublishedOnSuccess bool) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	path, err := testdataPath()
	if err != nil {
		return err
	}
	f, err := e2edata.Load(path)
	if err != nil {
		return err
	}
	rows := selectRows(f)
	if len(rows) == 0 {
		fmt.Println("✅ no rows to run")
		return nil
	}
	runErr := e2erun.Run(root, path, f, rows)
	if runErr == nil && markPublishedOnSuccess {
		f.MarkPublished()
		if err := f.Save(path); err != nil {
			return err
		}
	}
	return runErr
}

// Githooks groups git hook installation behind `mage githooks:<method>`.
type Githooks mg.Namespace

// Install points this repo's core.hooksPath at scripts/git-hooks, so the
// pre-push hook there (which runs `mage e2e:current`, blocking a push if
// it fails) actually runs -- see that file's own doc comment for the
// SKIP_E2E escape hatch. Idempotent and safe to re-run.
//
// Usage: mage githooks:install
func (Githooks) Install() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	hooksDir := filepath.Join(root, "scripts", "git-hooks")
	if err := os.Chmod(filepath.Join(hooksDir, "pre-push"), 0o755); err != nil {
		return fmt.Errorf("githooks: %w", err)
	}
	cmd := exec.Command("git", "config", "core.hooksPath", "scripts/git-hooks")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("githooks: set core.hooksPath: %w", err)
	}
	fmt.Println("✅ core.hooksPath set to scripts/git-hooks -- `mage e2e:current` now runs before every push")
	return nil
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
	if err := (E2E{}).All(); err != nil {
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

// BuildAndroid cross-compiles mobile/kvmobile into android-app/app/libs/kvmobile.aar
// via `gomobile bind`, with no identity/leader baked in -- a plain
// "does this still compile for Android" smoke build, the Android
// counterpart to BuildLinux/BuildWindows. `mage e2e:current`/`e2e:all`
// build their own AAR with a real identity and leader multiaddr baked in
// for actual test runs (see pkg/e2erun.buildAndroidAAR); this target
// doesn't produce something a device can usefully run against a real
// cluster.
func BuildAndroid() error {
	fmt.Println("Building Android AAR...")
	root, err := repoRoot()
	if err != nil {
		return err
	}
	aarPath := filepath.Join(root, "android-app", "app", "libs", "kvmobile.aar")
	if err := os.MkdirAll(filepath.Dir(aarPath), 0o755); err != nil {
		return err
	}
	cmd := exec.Command("gomobile", "bind", "-target=android", "-androidapi", "26",
		"-o", aarPath, "./mobile/kvmobile")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("buildandroid: gomobile bind: %w", err)
	}
	fmt.Println("✅ android-app/app/libs/kvmobile.aar built (no identity/leader baked in)")
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

// AddNodeWithKey is the AddNode equivalent for provisioning a node under an
// existing Ed25519 identity (an identity.key file's hex-encoded format --
// e.g. one saved from a node created elsewhere) instead of generating a
// fresh one, bootstrapping it as the cluster's sole leader.
//
// Usage: mage addnodewithkey <keyFile>
func AddNodeWithKey(keyFile string) error {
	return runAddNodeWithKey(keyFile)
}

// AddFollowerWithKey is the AddFollower equivalent of AddNodeWithKey: joins
// the cluster led by leaderPeerID under an existing identity instead of
// generating a fresh one.
//
// Usage: mage addfollowerwithkey <keyFile> <leaderPeerID>
func AddFollowerWithKey(keyFile, leaderPeerID string) error {
	return runAddNodeWithKey(keyFile, leaderPeerID)
}

func runAddNodeWithKey(keyFile string, peerIDs ...string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	peerID, err := kvctl.AddNodeWithKey(root, keyFile, peerIDs...)
	if err != nil {
		return err
	}
	fmt.Printf("✅ node %s is up and selected as current\n", peerID)
	return nil
}

// Join asks the raft cluster reachable through targetPeerID to admit the
// current node's own identity (see `mage use`), switching it onto a
// composite data dir dedicated to that cluster. If the target requires
// confirmation (-require-confirm-for-join), this only lodges a pending
// request -- ask a current raft voter on that cluster to run
// `mage confirmpermit cluster-join <peerID>` to actually admit it.
//
// Usage: mage join <targetPeerID>
func Join(targetPeerID string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	peerID, err := kvctl.Join(root, targetPeerID)
	if err != nil {
		return err
	}
	fmt.Printf("✅ node %s is up and selected as current\n", peerID)
	return nil
}

// Leave asks the raft cluster the current node is joined to (see `mage
// use`) to remove it (raft.RemoveServer -- a graceful shrink, the
// remaining voters keep operating normally), then switches the node back
// onto its own default solo single-node cluster. The composite cluster
// data dir is left on disk untouched, so a later `mage join`/
// `mage rejoinnode` back to the same cluster can pick its local state
// back up -- see Rm for the variant that wipes it instead.
//
// Usage: mage leave <peerID>
func Leave(peerID string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	if err := kvctl.Leave(root, peerID); err != nil {
		return err
	}
	fmt.Printf("✅ node %s left its cluster and resumed its solo db\n", peerID)
	return nil
}

// Rm does everything Leave does, plus revokes peerID's standing with the
// cluster it's leaving (so a later `mage join` attempt against the same
// cluster starts genuinely pending again, not auto-approved by a stale
// confirmed record) and deletes the composite cluster data dir outright.
//
// Usage: mage rm <peerID>
func Rm(peerID string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	if err := kvctl.Rm(root, peerID); err != nil {
		return err
	}
	fmt.Printf("🗑️  node %s removed from its cluster and resumed its solo db\n", peerID)
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

// DeleteNode permanently deletes a node's on-disk data (identity, sqlite
// store, raft log/snapshots) and its registry entry, by peer id. Refuses
// while the node's daemon process still appears to be running -- stop it
// first (there is no automatic kill: this mirrors the e2e pipeline's
// deletenode, which likewise never tears anything down implicitly).
//
// Usage: mage deletenode <peerID>
func DeleteNode(peerID string) error {
	if err := kvctl.DeleteNode(peerID); err != nil {
		return err
	}
	fmt.Printf("🗑️  node %s deleted\n", peerID)
	return nil
}

// ListClusters shows every raft cluster known to this machine's registry
// (grouped by whichever peer id originally bootstrapped it -- see
// kvctl.ListClusters), and every locally-created node identity that
// belongs to each one. This is purely a local registry read: no daemon
// needs to be running, but for the same reason it only ever shows clusters
// this machine has itself created or joined a node into -- there is no
// network-wide cluster discovery. Pass any listed peer id (one whose
// daemon is currently running) to `mage listnodes` to see that cluster's
// full live raft membership instead.
//
// Usage: mage listclusters
func ListClusters() error {
	clusters, err := kvctl.ListClusters()
	if err != nil {
		return err
	}
	if len(clusters) == 0 {
		fmt.Println("no nodes registered on this machine")
		return nil
	}
	for _, c := range clusters {
		fmt.Printf("cluster %s\n", c.ClusterID)
		for _, m := range c.Members {
			running := "stopped"
			if m.Running {
				running = "running"
			}
			fmt.Printf("  %s  role=%s  %s\n", m.PeerID, m.Role, running)
		}
	}
	return nil
}

// ListNodes queries the already-running node localPeerID for its raft
// cluster's full live membership (every peer id currently a voter/learner/
// leader, including peers this machine never created and so has no
// registry entry for) -- read from that node's own locally-replicated
// shmevent.KindClusterMember records, see kvctl.ListClusterMembers.
// localPeerID would typically be one of the peer ids `mage listclusters`
// just printed, but it must actually be running: unlike raft's own
// AppendEntries/InstallSnapshot traffic, this query only ever reaches a
// daemon over local shmring IPC, never a remote peer directly.
//
// Usage: mage listnodes <localPeerID>
func ListNodes(localPeerID string) error {
	members, err := kvctl.ListClusterMembers(localPeerID)
	if err != nil {
		return err
	}
	if len(members) == 0 {
		fmt.Printf("no cluster-member records found via %s\n", localPeerID)
		return nil
	}
	for _, m := range members {
		fmt.Printf("%s  role=%s\n", m.PeerID, m.Role)
	}
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

// RangeScan lists every key/value pair between start and end (both
// inclusive, lexicographic byte order over the raw key bytes) on the
// current node's local state, one JSON object per line -- a generic
// counterpart to set/get for inspecting a whole range of keys at once
// instead of one at a time, covering any key in the store (ordinary data
// as well as this project's own reserved namespaces -- see
// kvctl.RangeScan's doc comment for why that's not a new privilege for a
// local caller). limit is a count or "" (unlimited).
//
// Usage: mage rangescan <start> <end> [limit|""]
func RangeScan(start, end, limit string) error {
	n := 0
	if limit != "" {
		v, err := strconv.Atoi(limit)
		if err != nil {
			return fmt.Errorf("rangescan: invalid limit %q: %w", limit, err)
		}
		n = v
	}
	results, err := kvctl.RangeScan(start, end, n)
	if err != nil {
		return err
	}
	for _, kv := range results {
		out, err := json.Marshal(kv)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// RequestPermit lodges a pending permit record for peerID on the current
// node, forwarded to the leader like any other Set. metadata may be "" (a
// dialable multiaddr for kind "bootstrap", unused for kind "peer" -- see
// shmevent.EncodePermitRequestPayload).
// Usage: mage requestpermit <kind: peer|bootstrap> <peerID> <metadata>
func RequestPermit(kind, peerID, metadata string) error {
	k, ok := shmevent.KindFromName(kind)
	if !ok {
		return fmt.Errorf("unknown permit kind %q (want \"peer\" or \"bootstrap\")", kind)
	}
	if err := kvctl.RequestPermit(k, []byte(peerID), []byte(metadata)); err != nil {
		return err
	}
	fmt.Println("✅ permit requested")
	return nil
}

// ConfirmPermit promotes a pending permit record for peerID to confirmed.
// Only takes effect if the current node is itself a raft voter.
// Usage: mage confirmpermit <kind: peer|bootstrap> <peerID>
func ConfirmPermit(kind, peerID string) error {
	k, ok := shmevent.KindFromName(kind)
	if !ok {
		return fmt.Errorf("unknown permit kind %q (want \"peer\" or \"bootstrap\")", kind)
	}
	if err := kvctl.ConfirmPermit(k, []byte(peerID)); err != nil {
		return err
	}
	fmt.Println("✅ permit confirmed")
	return nil
}

// RevokePermit deletes a confirmed permit record for peerID outright.
// Only takes effect if the current node is itself a raft voter.
// Usage: mage revokepermit <kind: peer|bootstrap> <peerID>
func RevokePermit(kind, peerID string) error {
	k, ok := shmevent.KindFromName(kind)
	if !ok {
		return fmt.Errorf("unknown permit kind %q (want \"peer\" or \"bootstrap\")", kind)
	}
	if err := kvctl.RevokePermit(k, []byte(peerID)); err != nil {
		return err
	}
	fmt.Println("✅ permit revoked")
	return nil
}

// RequestLogPermit lodges a pending permission for peerID to
// append/query pkg/logrecord records of logKind, forwarded to the leader
// like any other Set. metadata may be "".
// Usage: mage requestlogpermit <logKind> <peerID> <metadata>
func RequestLogPermit(logKind, peerID, metadata string) error {
	if err := kvctl.RequestLogPermit(logKind, []byte(peerID), []byte(metadata)); err != nil {
		return err
	}
	fmt.Println("✅ log permit requested")
	return nil
}

// ConfirmLogPermit promotes a pending log-kind permit record for peerID
// to confirmed. Only takes effect if the current node is itself a raft
// voter.
// Usage: mage confirmlogpermit <logKind> <peerID>
func ConfirmLogPermit(logKind, peerID string) error {
	if err := kvctl.ConfirmLogPermit(logKind, []byte(peerID)); err != nil {
		return err
	}
	fmt.Println("✅ log permit confirmed")
	return nil
}

// RevokeLogPermit deletes a confirmed log-kind permit record for peerID
// outright. Only takes effect if the current node is itself a raft
// voter.
// Usage: mage revokelogpermit <logKind> <peerID>
func RevokeLogPermit(logKind, peerID string) error {
	if err := kvctl.RevokeLogPermit(logKind, []byte(peerID)); err != nil {
		return err
	}
	fmt.Println("✅ log permit revoked")
	return nil
}

// Execute sends value as a direct peer-to-peer EventExecute notification
// from the current node to destPeerID -- bypassing raft and the store
// entirely, see shmevent.EventExecute's doc comment.
// Usage: mage execute <destPeerID> <value>
func Execute(destPeerID, value string) error {
	if err := kvctl.Execute(destPeerID, value); err != nil {
		return err
	}
	fmt.Println("✅ execute sent")
	return nil
}

// PollExecute drains one queued EventExecute notification delivered to the
// current node, if any, and prints its sender peer id and value.
// Usage: mage pollexecute
func PollExecute() error {
	senderPeerID, value, ok, err := kvctl.PollExecute()
	if err != nil {
		return err
	}
	if !ok {
		fmt.Println("(no execute notification pending)")
		return nil
	}
	fmt.Printf("%s: %s\n", senderPeerID, value)
	return nil
}

// LogAppend appends a pkg/logrecord.Record of the given kind/unit,
// timestamped now and attributed to the current node -- see
// pkg/logrecord's doc comment for the generic key/record scheme this
// project builds structured log/report records on top of. kind and
// unitID are entirely caller-chosen strings, not a fixed set. fieldsJSON
// may be "" (no structured fields).
// Usage: mage logappend <kind> <unitID> <fieldsJSON> <narrative>
func LogAppend(kind, unitID, fieldsJSON, narrative string) error {
	var fields map[string]string
	if fieldsJSON != "" {
		if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
			return fmt.Errorf("decode fieldsJSON: %w", err)
		}
	}
	if err := kvctl.LogAppend(kind, unitID, fields, narrative); err != nil {
		return err
	}
	fmt.Println("✅ log appended")
	return nil
}

// LogQuery lists every pkg/logrecord.Record of the given kind/unit whose
// timestamp falls in [since, until], oldest first, one JSON object per
// line. since/until are RFC3339 or "" (since "" = unbounded, until "" =
// now). limit is a count or "" (unlimited).
// Usage: mage logquery <kind> <unitID> <since|""> <until|""> <limit|"">
func LogQuery(kind, unitID, since, until, limit string) error {
	start, end, n, err := parseTimeWindow(since, until, limit)
	if err != nil {
		return err
	}

	records, err := kvctl.LogQuery(kind, unitID, start, end, n)
	if err != nil {
		return err
	}
	for _, rec := range records {
		out, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// parseTimeWindow parses the since/until/limit string triple every
// *Query-style mage target (LogQuery, QueryCommandLog) takes on the
// command line into kvctl.LogQuery's native (start, end time.Time, limit
// int) shape: since/until are RFC3339 or "" (since "" = unbounded epoch,
// until "" = now); limit is a count or "" (0, meaning unlimited).
func parseTimeWindow(since, until, limit string) (start, end time.Time, n int, err error) {
	start = time.Unix(0, 0)
	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return time.Time{}, time.Time{}, 0, fmt.Errorf("since: %w", err)
		}
		start = t
	}
	end = time.Now()
	if until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return time.Time{}, time.Time{}, 0, fmt.Errorf("until: %w", err)
		}
		end = t
	}
	if limit != "" {
		v, err := strconv.Atoi(limit)
		if err != nil {
			return time.Time{}, time.Time{}, 0, fmt.Errorf("limit: %w", err)
		}
		n = v
	}
	return start, end, n, nil
}

// CreateGroup implements `mage creategroup <id> <name> <description>`:
// defines a new command group, publicly listable by any cluster member --
// see pkg/kvctl.CreateGroup's doc comment.
// Usage: mage creategroup <id> <name> <description>
func CreateGroup(id, name, description string) error {
	if err := kvctl.CreateGroup(id, name, description); err != nil {
		return err
	}
	fmt.Println("✅ group created")
	return nil
}

// UpdateGroup implements `mage updategroup <id> <name> <description>`:
// appends a new name/description revision for an existing group. Requires
// the current node to already be a participant of id.
// Usage: mage updategroup <id> <name> <description>
func UpdateGroup(id, name, description string) error {
	if err := kvctl.UpdateGroup(id, name, description); err != nil {
		return err
	}
	fmt.Println("✅ group updated")
	return nil
}

// DeleteGroup implements `mage deletegroup <id>`: tombstones a group so
// GetGroup/ListGroups exclude it afterward (its Commands remain on disk,
// just unreachable through the catalog). Requires the current node to
// already be a participant of id.
// Usage: mage deletegroup <id>
func DeleteGroup(id string) error {
	if err := kvctl.DeleteGroup(id); err != nil {
		return err
	}
	fmt.Println("✅ group deleted")
	return nil
}

// GetGroup implements `mage getgroup <id>`: prints id's current
// definition as one JSON object.
// Usage: mage getgroup <id>
func GetGroup(id string) error {
	group, err := kvctl.GetGroup(id)
	if err != nil {
		return err
	}
	out, err := json.Marshal(group)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// ListGroups implements `mage listgroups`: prints every non-deleted
// Group, one JSON object per line.
// Usage: mage listgroups
func ListGroups() error {
	groups, err := kvctl.ListGroups()
	if err != nil {
		return err
	}
	for _, g := range groups {
		out, err := json.Marshal(g)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// IsGroupParticipant implements `mage isgroupparticipant <groupID>`:
// prints "true"/"false" for whether the current node holds a confirmed
// participation permit for groupID -- see pkg/kvctl.IsGroupParticipant's
// doc comment.
// Usage: mage isgroupparticipant <groupID>
func IsGroupParticipant(groupID string) error {
	ok, err := kvctl.IsGroupParticipant(groupID)
	if err != nil {
		return err
	}
	fmt.Println(ok)
	return nil
}

// RequestGroupParticipation implements `mage requestgroupparticipation
// <groupID> <peerID> <metadata>`: lodges a pending request for peerID to
// participate in groupID -- a thin wrapper over `mage requestlogpermit`
// under the hood (see pkg/kvctl.RequestGroupParticipation).
// Usage: mage requestgroupparticipation <groupID> <peerID> <metadata>
func RequestGroupParticipation(groupID, peerID, metadata string) error {
	if err := kvctl.RequestGroupParticipation(groupID, peerID, metadata); err != nil {
		return err
	}
	fmt.Println("✅ participation requested")
	return nil
}

// ConfirmGroupParticipation implements `mage confirmgroupparticipation
// <groupID> <peerID>`: promotes a pending participation request to
// confirmed. Only takes effect if the current node is itself a raft
// voter.
// Usage: mage confirmgroupparticipation <groupID> <peerID>
func ConfirmGroupParticipation(groupID, peerID string) error {
	if err := kvctl.ConfirmGroupParticipation(groupID, peerID); err != nil {
		return err
	}
	fmt.Println("✅ participation confirmed")
	return nil
}

// RevokeGroupParticipation implements `mage revokegroupparticipation <groupID>
// <peerID>`: deletes a confirmed participation record outright.
// Usage: mage revokegroupparticipation <groupID> <peerID>
func RevokeGroupParticipation(groupID, peerID string) error {
	if err := kvctl.RevokeGroupParticipation(groupID, peerID); err != nil {
		return err
	}
	fmt.Println("✅ participation revoked")
	return nil
}

// CreateCommand implements `mage createcommand <id> <groupID>
// <targetPeerID> <name> <description> <formSchemaJSON>`: defines a
// Command belonging to groupID, executed by targetPeerID. formSchemaJSON
// is a JSON array of pkg/kvctl.FormField, or "" for none. Requires the
// current node to already be a participant of groupID.
// Usage: mage createcommand <id> <groupID> <targetPeerID> <name> <description> <formSchemaJSON>
func CreateCommand(id, groupID, targetPeerID, name, description, formSchemaJSON string) error {
	schema, err := decodeFormSchema(formSchemaJSON)
	if err != nil {
		return err
	}
	if err := kvctl.CreateCommand(id, groupID, targetPeerID, name, description, schema); err != nil {
		return err
	}
	fmt.Println("✅ command created")
	return nil
}

// UpdateCommand implements `mage updatecommand <id> <groupID>
// <targetPeerID> <name> <description> <formSchemaJSON>`: CreateCommand's
// alias for the "this id already exists" case -- see
// pkg/kvctl.CreateCommand's doc comment.
// Usage: mage updatecommand <id> <groupID> <targetPeerID> <name> <description> <formSchemaJSON>
func UpdateCommand(id, groupID, targetPeerID, name, description, formSchemaJSON string) error {
	schema, err := decodeFormSchema(formSchemaJSON)
	if err != nil {
		return err
	}
	if err := kvctl.UpdateCommand(id, groupID, targetPeerID, name, description, schema); err != nil {
		return err
	}
	fmt.Println("✅ command updated")
	return nil
}

// decodeFormSchema decodes formSchemaJSON (a JSON array of
// pkg/kvctl.FormField, or "") for CreateCommand/UpdateCommand -- the same
// "magefile decodes the JSON, kvctl takes a native value" split LogAppend
// already uses for fieldsJSON.
func decodeFormSchema(formSchemaJSON string) ([]kvctl.FormField, error) {
	if formSchemaJSON == "" {
		return nil, nil
	}
	var schema []kvctl.FormField
	if err := json.Unmarshal([]byte(formSchemaJSON), &schema); err != nil {
		return nil, fmt.Errorf("decode formSchemaJSON: %w", err)
	}
	return schema, nil
}

// DeleteCommand implements `mage deletecommand <groupID> <id>`:
// tombstones a Command so GetCommand/ListCommands exclude it afterward.
// Requires the current node to already be a participant of groupID.
// Usage: mage deletecommand <groupID> <id>
func DeleteCommand(groupID, id string) error {
	if err := kvctl.DeleteCommand(groupID, id); err != nil {
		return err
	}
	fmt.Println("✅ command deleted")
	return nil
}

// GetCommand implements `mage getcommand <groupID> <id>`: prints id's
// current definition within groupID as one JSON object. Requires the
// current node to already be a participant of groupID.
// Usage: mage getcommand <groupID> <id>
func GetCommand(groupID, id string) error {
	cmd, err := kvctl.GetCommand(groupID, id)
	if err != nil {
		return err
	}
	out, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// ListCommands implements `mage listcommands <groupID>`: prints every
// non-deleted Command currently defined under groupID, one JSON object
// per line. Requires the current node to already be a participant of
// groupID.
// Usage: mage listcommands <groupID>
func ListCommands(groupID string) error {
	commands, err := kvctl.ListCommands(groupID)
	if err != nil {
		return err
	}
	for _, c := range commands {
		out, err := json.Marshal(c)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// SubmitCommand implements `mage submitcommand <groupID> <commandID>
// <inputsJSON>`: dispatches commandID as a durable, replicated request
// plus a low-latency Execute poke to its TargetPeerID, and prints the new
// instance id -- see pkg/kvctl.SubmitCommand's doc comment.
// Usage: mage submitcommand <groupID> <commandID> <inputsJSON>
func SubmitCommand(groupID, commandID, inputsJSON string) error {
	instanceID, err := kvctl.SubmitCommand(groupID, commandID, inputsJSON)
	if err != nil {
		return err
	}
	fmt.Println(instanceID)
	return nil
}

// GetCommandRequest implements `mage getcommandrequest <groupID>
// <instanceID>`: prints instanceID's dispatch record within groupID as
// one JSON object.
// Usage: mage getcommandrequest <groupID> <instanceID>
func GetCommandRequest(groupID, instanceID string) error {
	req, err := kvctl.GetCommandRequest(groupID, instanceID)
	if err != nil {
		return err
	}
	out, err := json.Marshal(req)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// ListCommandRequests implements `mage listcommandrequests <groupID>`:
// prints every dispatch request currently recorded for groupID, oldest
// first, one JSON object per line -- the target's catch-up path for a
// missed Execute poke.
// Usage: mage listcommandrequests <groupID>
func ListCommandRequests(groupID string) error {
	requests, err := kvctl.ListCommandRequests(groupID)
	if err != nil {
		return err
	}
	for _, r := range requests {
		out, err := json.Marshal(r)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// ListExecutions implements `mage listexecutions <peerID>`: prints up to
// the 200 most recent SubmitCommand dispatches touching peerID, as either
// requester or target, most recent first, one JSON object per line -- see
// pkg/kvctl.ListExecutionsByPeer's doc comment.
// Usage: mage listexecutions <peerID>
func ListExecutions(peerID string) error {
	executions, err := kvctl.ListExecutionsByPeer(peerID)
	if err != nil {
		return err
	}
	for _, e := range executions {
		out, err := json.Marshal(e)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// AppendCommandLog implements `mage appendcommandlog <requesterPeerID>
// <instanceID> <fieldsJSON> <narrative>`: writes one execution-log entry
// for instanceID and pokes requesterPeerID (pass "" to skip the poke) --
// see pkg/kvctl.AppendCommandLog's doc comment.
// Usage: mage appendcommandlog <requesterPeerID> <instanceID> <fieldsJSON> <narrative>
func AppendCommandLog(requesterPeerID, instanceID, fieldsJSON, narrative string) error {
	var fields map[string]string
	if fieldsJSON != "" {
		if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
			return fmt.Errorf("decode fieldsJSON: %w", err)
		}
	}
	if err := kvctl.AppendCommandLog(requesterPeerID, instanceID, fields, narrative); err != nil {
		return err
	}
	fmt.Println("✅ command log appended")
	return nil
}

// QueryCommandLog implements `mage querycommandlog <instanceID> <since>
// <until> <limit>`: lists every AppendCommandLog entry for instanceID
// whose timestamp falls in [since, until], oldest first, one JSON object
// per line -- see LogQuery's doc comment for the since/until/limit
// argument shape.
// Usage: mage querycommandlog <instanceID> <since|""> <until|""> <limit|"">
func QueryCommandLog(instanceID, since, until, limit string) error {
	start, end, n, err := parseTimeWindow(since, until, limit)
	if err != nil {
		return err
	}

	records, err := kvctl.QueryCommandLog(instanceID, start, end, n)
	if err != nil {
		return err
	}
	for _, rec := range records {
		out, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// LatestCommandLog implements `mage latestcommandlog <instanceID>`:
// prints instanceID's single most recent AppendCommandLog entry as one
// JSON object -- see pkg/kvctl.LatestCommandLog's doc comment.
// Usage: mage latestcommandlog <instanceID>
func LatestCommandLog(instanceID string) error {
	rec, err := kvctl.LatestCommandLog(instanceID)
	if err != nil {
		return err
	}
	out, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// bootstrapNodesConfigPath is configs/bootstrap-nodes.json, relative to the
// repo root -- a human-maintained record of already-deployed leader nodes
// (ssh host, install dir, port, peer id/multiaddrs). Nothing in this repo
// deploys to it automatically; it's updated by hand (or by whoever ran the
// SSH bootstrap in README's "Leader on a remote machine" section) after the
// fact, same as registry.json on the node itself.
const bootstrapNodesConfigPath = "configs/bootstrap-nodes.json"

// bootstrapNodesFile is configs/bootstrap-nodes.json's shape.
type bootstrapNodesFile struct {
	BootstrapNodes []bootstrapNodeConfig `json:"bootstrap_nodes"`
}

type bootstrapNodeConfig struct {
	Name         string   `json:"name"`
	SSHHost      string   `json:"ssh_host"`
	InstallDir   string   `json:"install_dir"`
	ListenPort   int      `json:"listen_port"`
	RelayService bool     `json:"relay_service"`
	PeerID       string   `json:"peer_id"`
	DataDir      string   `json:"data_dir"`
	KeyPath      string   `json:"key_path"`
	ListenAddrs  []string `json:"listen_addrs"`
}

// BootstrapNodes prints the leader nodes recorded in
// configs/bootstrap-nodes.json -- each one an already-deployed kvnode
// leader on a remote host, not something this target deploys itself (see
// bootstrapNodesConfigPath's doc comment).
//
// Usage: mage bootstrapnodes
func BootstrapNodes() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	path := filepath.Join(root, bootstrapNodesConfigPath)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("bootstrapnodes: read %s: %w", path, err)
	}
	var cfg bootstrapNodesFile
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("bootstrapnodes: parse %s: %w", path, err)
	}
	if len(cfg.BootstrapNodes) == 0 {
		fmt.Printf("no bootstrap nodes configured in %s\n", bootstrapNodesConfigPath)
		return nil
	}
	for _, n := range cfg.BootstrapNodes {
		fmt.Printf("%s\n", n.Name)
		fmt.Printf("  ssh:     %s\n", n.SSHHost)
		fmt.Printf("  install: %s (port %d, relay-service=%v)\n", n.InstallDir, n.ListenPort, n.RelayService)
		fmt.Printf("  peer id: %s\n", n.PeerID)
		for _, addr := range n.ListenAddrs {
			fmt.Printf("  addr:    %s\n", addr)
		}
	}
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

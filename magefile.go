//go:build mage

package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/mg"
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

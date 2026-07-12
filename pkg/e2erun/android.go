package e2erun

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// androidGoPackage is mobile/kvmobile's import path, for the -X ldflags
// gomobile bind needs to bake an identity/leader into the AAR.
const androidGoPackage = "github.com/gofsd/libp2p-kv-raft/mobile/kvmobile"

// androidAppID/androidTestRunner match android-app/app/build.gradle.kts's
// applicationId and testInstrumentationRunner -- AGP's default test APK id
// is "<applicationId>.test" (no testApplicationId override is set there).
const (
	androidAppID      = "com.gofsd.kvdemo"
	androidTestAppID  = androidAppID + ".test"
	androidTestClass  = androidAppID + ".E2ETest"
	androidTestRunner = androidTestAppID + "/androidx.test.runner.AndroidJUnitRunner"
)

// androidUnavailable checks that gomobile, adb, and a connected
// device/emulator are all present, returning a human-readable reason if
// not -- so a pipeline run on a machine without Android tooling degrades
// to a clear Skipped status instead of failing hard (see runRow's android
// case) or, worse, hanging on a tool that was never going to answer.
func androidUnavailable() string {
	if _, err := exec.LookPath("gomobile"); err != nil {
		return "gomobile not found on PATH"
	}
	if _, err := exec.LookPath("adb"); err != nil {
		return "adb not found on PATH"
	}
	out, err := exec.Command("adb", "devices").Output()
	if err != nil {
		return fmt.Sprintf("adb devices: %v", err)
	}
	if !hasConnectedDevice(string(out)) {
		return "no adb device/emulator connected (see `adb devices` / start an AVD)"
	}
	return ""
}

// hasConnectedDevice parses `adb devices` output for at least one line
// ending in "\tdevice" (as opposed to "\toffline"/"\tunauthorized", or the
// header line).
func hasConnectedDevice(adbDevicesOutput string) bool {
	for line := range strings.SplitSeq(adbDevicesOutput, "\n") {
		if strings.HasSuffix(strings.TrimRight(line, "\r"), "\tdevice") {
			return true
		}
	}
	return false
}

// runAndroidRows runs every PlatformAndroid row in rowIndices, grouped by
// node: one gomobile bind + gradle install + instrumented test run per
// distinct android node (covering every row that node has, in order),
// since rebuilding/reinstalling per row would be prohibitively slow --
// unlike desktop/remote rows, which each get their own real dispatch, or
// web rows, which share one Playwright-driven verdict per whole run (see
// web.go). Returns a result for every android row index in rowIndices.
func runAndroidRows(repoRoot string, f *e2edata.File, rowIndices []int, bootstrapMultiaddr string) map[int]rowOutcome {
	results := map[int]rowOutcome{}

	byNode := map[int][]int{}
	for _, idx := range rowIndices {
		row := f.Rows[idx]
		node, ok := f.Nodes[row.Node]
		if !ok || node.Platform != e2edata.PlatformAndroid {
			continue
		}
		byNode[row.Node] = append(byNode[row.Node], idx)
	}
	if len(byNode) == 0 {
		return results
	}

	if reason := androidUnavailable(); reason != "" {
		for _, idxs := range byNode {
			for _, idx := range idxs {
				results[idx] = rowOutcome{status: e2edata.StatusSkipped, errMsg: "android e2e skipped: " + reason}
			}
		}
		return results
	}

	for nodeID, idxs := range byNode {
		node := f.Nodes[nodeID]
		maps.Copy(results, runAndroidNode(repoRoot, node, bootstrapMultiaddr, f, idxs))
	}
	return results
}

// runAndroidNode builds an AAR baked with node's identity and
// bootstrapMultiaddr as the leader to join, installs the app + its
// instrumented test APK, runs every row in rowIdxs (in order) in one
// instrumentation invocation, and maps the per-row results back to
// rowIdxs. A failure at the build/install/instrument-invocation level
// (not an individual row's own event failing) marks every row in this
// batch failed with that same error, since none of them actually ran.
func runAndroidNode(repoRoot string, node e2edata.Node, bootstrapMultiaddr string, f *e2edata.File, rowIdxs []int) map[int]rowOutcome {
	fail := func(err error) map[int]rowOutcome {
		out := make(map[int]rowOutcome, len(rowIdxs))
		for _, idx := range rowIdxs {
			out[idx] = rowOutcome{status: e2edata.StatusFail, errMsg: err.Error()}
		}
		return out
	}

	eventJSONs := make([]string, len(rowIdxs))
	for i, idx := range rowIdxs {
		ev := f.Rows[idx].Event
		if ev.EventType == shmevent.EventAdd {
			resolved := ResolveBootstrapPlaceholder(string(ev.Value()), bootstrapMultiaddr)
			ev = e2edata.NewEvent(ev.EventType, ev.SourceID, ev.DestinationID, []byte(resolved), ev.ID)
		}
		data, err := json.Marshal(ev)
		if err != nil {
			return fail(fmt.Errorf("e2erun: encode android row event: %w", err))
		}
		eventJSONs[i] = string(data)
	}
	rowsArg, err := json.Marshal(eventJSONs)
	if err != nil {
		return fail(fmt.Errorf("e2erun: encode android rows argument: %w", err))
	}

	if err := buildAndroidAAR(repoRoot, node, bootstrapMultiaddr); err != nil {
		return fail(err)
	}
	if err := gradleInstall(repoRoot); err != nil {
		return fail(err)
	}
	resultsJSON, err := runInstrumentedTest(string(rowsArg))
	if err != nil {
		return fail(err)
	}

	out, err := parseRowResults(resultsJSON, rowIdxs, "android instrumented test")
	if err != nil {
		return fail(err)
	}
	return out
}

// buildAndroidAAR runs `gomobile bind` for mobile/kvmobile, baking node's
// identity and bootstrapMultiaddr as the leader to join, and writes the
// result to android-app/app/libs/kvmobile.aar -- the exact path
// android-app/app/build.gradle.kts's `implementation(files("libs/kvmobile.aar"))`
// expects (see README.md's "Follower on Android" section for the manual
// equivalent of this same command).
func buildAndroidAAR(repoRoot string, node e2edata.Node, bootstrapMultiaddr string) error {
	aarPath := filepath.Join(repoRoot, "android-app", "app", "libs", "kvmobile.aar")
	if err := os.MkdirAll(filepath.Dir(aarPath), 0o755); err != nil {
		return err
	}
	ldflags := fmt.Sprintf(
		"-X %[1]s.leaderMultiaddr=%[2]s -X %[1]s.relayMultiaddr=%[2]s -X %[1]s.identitySeedHex=%[3]s",
		androidGoPackage, bootstrapMultiaddr, node.PrivateKey,
	)
	cmd := exec.Command("gomobile", "bind", "-target=android", "-androidapi", "26",
		"-ldflags", ldflags, "-o", aarPath, "./mobile/kvmobile")
	cmd.Dir = repoRoot
	if err := runCaptured(cmd, "gomobile bind"); err != nil {
		return err
	}
	return nil
}

// gradleInstall builds and installs both the app and its instrumented
// test APK onto whatever adb device/emulator is connected.
func gradleInstall(repoRoot string) error {
	androidDir := filepath.Join(repoRoot, "android-app")
	gradlew := filepath.Join(androidDir, "gradlew")
	cmd := exec.Command(gradlew, "installDebug", "installDebugAndroidTest")
	cmd.Dir = androidDir
	return runCaptured(cmd, "gradlew installDebug")
}

// capturedOutputLimit bounds how much of a failing command's combined
// output gets embedded in the returned error (and so recorded into
// testdata.json) -- long enough to carry the actual failure (e.g. a real
// device's package-manager rejection reason), short enough not to dump an
// entire multi-thousand-line Gradle build log into the test row.
const capturedOutputLimit = 2000

// runCaptured runs cmd, tee-ing its combined output to this process's
// stderr (for live visibility exactly like every other exec.Command in
// this package) while also capturing it, so a failure's returned error
// carries the actual diagnostic content instead of just "exit status 1" --
// a real bug caught by this producing exactly that useless message when
// android-app/app/build.gradle.kts's installDebugAndroidTest failed with a
// real, specific, otherwise-nowhere-recorded package-manager error. See
// summarizeFailure for why this takes the *front* of Gradle's "FAILURE:"
// section rather than just the tail of the whole log.
func runCaptured(cmd *exec.Cmd, label string) error {
	var buf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stderr, &buf)
	cmd.Stderr = io.MultiWriter(os.Stderr, &buf)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("e2erun: %s: %w: %s", label, err, summarizeFailure(buf.String()))
	}
	return nil
}

// summarizeFailure extracts the useful part of a failing command's output
// for embedding in an error message. Gradle's own failures put the actual
// one-line cause (e.g. "INSTALL_FAILED_USER_RESTRICTED: Install canceled
// by user") in a short "FAILURE: Build failed..." / "* What went wrong:"
// section near the *start* of its error report, followed by a multi-
// thousand-line Java stack trace and "* Try:"/"* Get more help" boilerplate
// at the end -- so blindly taking the last N bytes of the full output (an
// earlier version of this function did exactly that) captures only stack
// trace noise and cuts off before ever reaching the real cause. Falls back
// to the last capturedOutputLimit bytes of the whole output for commands
// (e.g. gomobile bind) that don't follow this convention.
func summarizeFailure(out string) string {
	if idx := strings.Index(out, "FAILURE: "); idx >= 0 {
		out = out[idx:]
		if end := strings.Index(out, "\n* Try:"); end >= 0 {
			out = out[:end]
		}
		if len(out) > capturedOutputLimit {
			out = out[:capturedOutputLimit] + "..."
		}
		return strings.TrimSpace(out)
	}
	if len(out) > capturedOutputLimit {
		out = "..." + out[len(out)-capturedOutputLimit:]
	}
	return strings.TrimSpace(out)
}

// runInstrumentedTest triggers E2ETest via `adb shell am instrument`,
// passing rowsArgJSON as its "rows" instrumentation argument, then pulls
// and returns the results file it writes to the app's external files dir
// (see E2ETest.kt's doc comment on why external, not private, storage).
// The instrumentation run itself is allowed to report a JUnit failure
// (E2ETest.fail()s if any row failed) without that being treated as a Go
// error here -- the results file, not the instrumentation exit status, is
// authoritative for per-row pass/fail.
func runInstrumentedTest(rowsArgJSON string) ([]byte, error) {
	cmd := exec.Command("adb", "shell", "am", "instrument", "-w",
		"-e", "class", androidTestClass,
		"-e", "rows", rowsArgJSON,
		androidTestRunner,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	_ = cmd.Run()

	deviceResultsPath := fmt.Sprintf("/sdcard/Android/data/%s/files/e2e_results.json", androidAppID)
	localResultsPath := filepath.Join(os.TempDir(), "kvraft-e2e-android-results.json")
	defer os.Remove(localResultsPath)
	pull := exec.Command("adb", "pull", deviceResultsPath, localResultsPath)
	var pullOut bytes.Buffer
	pull.Stdout = &pullOut
	pull.Stderr = &pullOut
	if err := pull.Run(); err != nil {
		return nil, fmt.Errorf("e2erun: pull android results (instrument output: %s): %w: %s", out.String(), err, pullOut.String())
	}
	data, err := os.ReadFile(localResultsPath)
	if err != nil {
		return nil, fmt.Errorf("e2erun: read pulled android results: %w", err)
	}
	return data, nil
}

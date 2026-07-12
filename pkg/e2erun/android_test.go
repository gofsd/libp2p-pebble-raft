package e2erun

import (
	"strings"
	"testing"
)

// TestSummarizeFailureExtractsRealCause uses a trimmed but real excerpt of
// a Gradle installDebugAndroidTest failure (from an actual device
// rejecting the install with INSTALL_FAILED_USER_RESTRICTED) to prove
// summarizeFailure surfaces the actual one-line cause instead of the
// multi-thousand-line stack trace that follows it -- a real bug (an
// earlier version took the last N bytes of the whole log, landing
// entirely inside the stack trace and never reaching the cause) caught by
// running this against a live device, not by inspection.
func TestSummarizeFailureExtractsRealCause(t *testing.T) {
	out := `> Task :app:installDebugAndroidTest FAILED
com.android.ddmlib.InstallException: INSTALL_FAILED_USER_RESTRICTED: Install canceled by user
	at com.android.ddmlib.IDeviceSharedImpl.installRemotePackage(IDeviceSharedImpl.java:344)
` + strings.Repeat("\tat some.deeply.nested.StackFrame.method(File.java:123)\n", 200) + `

FAILURE: Build failed with an exception.

* What went wrong:
Execution failed for task ':app:installDebugAndroidTest'.
> java.util.concurrent.ExecutionException: com.android.builder.testing.api.DeviceException: com.android.ddmlib.InstallException: INSTALL_FAILED_USER_RESTRICTED: Install canceled by user

* Try:
> Run with --stacktrace option to get the stack trace.
> Get more help at https://help.gradle.org.

BUILD FAILED in 27s`

	got := summarizeFailure(out)
	if !strings.Contains(got, "INSTALL_FAILED_USER_RESTRICTED") {
		t.Fatalf("summarizeFailure did not surface the real cause; got: %s", got)
	}
	if !strings.Contains(got, "What went wrong") {
		t.Fatalf("summarizeFailure should keep Gradle's own summary heading; got: %s", got)
	}
	if strings.Contains(got, "Run with --stacktrace") {
		t.Fatalf("summarizeFailure should stop before the '* Try:' boilerplate; got: %s", got)
	}
}

func TestSummarizeFailureFallsBackToTailWithoutGradleMarker(t *testing.T) {
	out := strings.Repeat("noise ", 1000) + "the real cause is here"
	got := summarizeFailure(out)
	if !strings.Contains(got, "the real cause is here") {
		t.Fatalf("summarizeFailure fallback should keep the tail; got: %s", got)
	}
}

func TestHasConnectedDevice(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want bool
	}{
		{"no devices, just header", "List of devices attached\n\n", false},
		{"one real device", "List of devices attached\nO7AQS4UO9LWWJZI7\tdevice\n\n", true},
		{"offline device only", "List of devices attached\nemulator-5554\toffline\n\n", false},
		{"unauthorized device only", "List of devices attached\nO7AQS4UO9LWWJZI7\tunauthorized\n\n", false},
		{"mixed: offline then device", "List of devices attached\nemulator-5554\toffline\nO7AQS4UO9LWWJZI7\tdevice\n\n", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := hasConnectedDevice(c.out); got != c.want {
				t.Fatalf("hasConnectedDevice(%q) = %v, want %v", c.out, got, c.want)
			}
		})
	}
}

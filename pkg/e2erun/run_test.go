package e2erun

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Most of this package is SSH/process orchestration that needs a live
// environment to exercise meaningfully (verified manually against a real
// spawned kvnode -- see this package's use from magefile.go's E2E
// namespace). These tests cover the pure logic pieces that don't.

func TestResolveBootstrapPlaceholder(t *testing.T) {
	if got := ResolveBootstrapPlaceholder(BootstrapToken, "/ip4/1.2.3.4/tcp/4101/p2p/12D3Koo..."); got != "/ip4/1.2.3.4/tcp/4101/p2p/12D3Koo..." {
		t.Fatalf("ResolveBootstrapPlaceholder(token) = %q, want the resolved multiaddr", got)
	}
	if got := ResolveBootstrapPlaceholder("some literal value", "/ip4/1.2.3.4/tcp/4101/p2p/12D3Koo..."); got != "some literal value" {
		t.Fatalf("ResolveBootstrapPlaceholder(non-token) = %q, want unchanged", got)
	}
}

func TestInterpretSendEventResult(t *testing.T) {
	okEv := e2edata.EventFromMsg(shmevent.Msg{EventType: shmevent.EventGetPublicKey, Value: []byte("key-bytes")})
	okJSON := mustJSON(t, okEv)
	status, errMsg := interpretSendEventResult(okJSON, "", nil)
	if status != e2edata.StatusPass || errMsg != "" {
		t.Fatalf("interpretSendEventResult(pass) = (%d, %q), want (StatusPass, \"\")", status, errMsg)
	}

	// kvctl-cli sendevent prints the response JSON to stdout *then* exits
	// 1 for an EventError response -- interpretSendEventResult must still
	// read that JSON, not just report the nonzero exit opaquely (a real
	// bug caught running this against a live deployed node).
	errEv := e2edata.EventFromMsg(shmevent.Msg{EventType: shmevent.EventError, Value: []byte("store: key not found")})
	errJSON := mustJSON(t, errEv)
	status, errMsg = interpretSendEventResult(errJSON, "", errExitStatus1)
	if status != e2edata.StatusFail || errMsg != "store: key not found" {
		t.Fatalf("interpretSendEventResult(error, exit 1) = (%d, %q), want (StatusFail, \"store: key not found\")", status, errMsg)
	}

	status, errMsg = interpretSendEventResult("", "connection refused", errExitStatus1)
	if status != e2edata.StatusFail || errMsg == "" {
		t.Fatalf("interpretSendEventResult(empty stdout, real failure) = (%d, %q), want (StatusFail, non-empty)", status, errMsg)
	}

	status, errMsg = interpretSendEventResult("not json", "", nil)
	if status != e2edata.StatusFail || errMsg == "" {
		t.Fatalf("interpretSendEventResult(garbage) = (%d, %q), want (StatusFail, non-empty)", status, errMsg)
	}
}

var errExitStatus1 = errors.New("exit status 1")

func TestStatusName(t *testing.T) {
	cases := map[int]string{
		e2edata.StatusPass:    "PASS",
		e2edata.StatusSkipped: "SKIP",
		e2edata.StatusFail:    "FAIL",
		99:                    "FAIL",
	}
	for status, want := range cases {
		if got := statusName(status); got != want {
			t.Fatalf("statusName(%d) = %q, want %q", status, got, want)
		}
	}
}

func TestParseRowResults(t *testing.T) {
	rowIdxs := []int{10, 20, 30}

	out, err := parseRowResults([]byte(`[
		{"index":0,"pass":true},
		{"index":1,"pass":false,"error":"store: key not found"},
		{"index":2,"pass":true}
	]`), rowIdxs, "test driver")
	if err != nil {
		t.Fatalf("parseRowResults: %v", err)
	}
	if out[10].status != e2edata.StatusPass {
		t.Fatalf("row 10 (index 0) = %+v, want pass", out[10])
	}
	if out[20].status != e2edata.StatusFail || out[20].errMsg != "store: key not found" {
		t.Fatalf("row 20 (index 1) = %+v, want fail with the recorded error", out[20])
	}
	if out[30].status != e2edata.StatusPass {
		t.Fatalf("row 30 (index 2) = %+v, want pass", out[30])
	}
}

func TestParseRowResultsMissingRowFailsClosed(t *testing.T) {
	rowIdxs := []int{10, 20}

	// Only one of the two expected results came back -- e.g. the driver
	// process crashed partway through.
	out, err := parseRowResults([]byte(`[{"index":0,"pass":true}]`), rowIdxs, "test driver")
	if err != nil {
		t.Fatalf("parseRowResults: %v", err)
	}
	if out[10].status != e2edata.StatusPass {
		t.Fatalf("row 10 = %+v, want pass", out[10])
	}
	if out[20].status != e2edata.StatusFail {
		t.Fatalf("row 20 (never reported) = %+v, want fail-closed, not a silent pass", out[20])
	}
}

func TestParseRowResultsInvalidJSON(t *testing.T) {
	if _, err := parseRowResults([]byte("not json"), []int{1}, "test driver"); err == nil {
		t.Fatal("parseRowResults with invalid JSON: want error, got nil")
	}
}

func mustJSON(t *testing.T, ev e2edata.Event) string {
	t.Helper()
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}

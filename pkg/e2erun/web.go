package e2erun

import (
	"os"
	"os/exec"
	"path/filepath"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// webRunResult caches the single web-platform verdict for a whole Run
// invocation -- see runWebSuite's doc comment for why every web row shares
// one verdict rather than each getting its own real dispatch.
type webRunResult struct {
	computed bool
	status   int
	errMsg   string
}

func (c *webRunResult) resultFor(repoRoot, bootstrapMultiaddr string) (int, string) {
	if !c.computed {
		c.status, c.errMsg = runWebSuite(repoRoot, bootstrapMultiaddr)
		c.computed = true
	}
	return c.status, c.errMsg
}

// runWebSuite runs the web-app crate's existing Playwright smoke test
// (tests/set_get.spec.js) once, against bootstrapMultiaddr, and reports a
// single pass/fail/skip verdict shared by every web-platform row in this
// run.
//
// This is coarser than the desktop path's real per-row raw shmevent
// dispatch: web-app's MainHandle only exposes connect/set/get (see
// web-app/src/app.rs), not a generic "send this exact shmevent" primitive,
// and building one purely for e2e fidelity was out of scope here -- see
// web-app/README.md's Known gaps. A failing/skipped verdict is recorded on
// every web row so that gap stays visible in testdata.json rather than
// silently reporting a pass no real browser ever produced.
func runWebSuite(repoRoot, bootstrapMultiaddr string) (status int, errMsg string) {
	webDir := filepath.Join(repoRoot, "web-app")

	if _, err := exec.LookPath("npx"); err != nil {
		return e2edata.StatusSkipped, "npx not found on PATH; skipping web e2e (see web-app/README.md's Building section)"
	}
	if _, err := os.Stat(filepath.Join(webDir, "node_modules")); err != nil {
		return e2edata.StatusSkipped, "web-app/node_modules missing (run `npm install` in web-app/); skipping web e2e"
	}
	if _, err := os.Stat(filepath.Join(webDir, "pkg", "kv_raft_web.js")); err != nil {
		return e2edata.StatusSkipped, "web-app/pkg wasm bundle missing (run `npm run build:wasm` in web-app/); skipping web e2e"
	}

	cmd := exec.Command("npx", "playwright", "test", "tests/set_get.spec.js")
	cmd.Dir = webDir
	cmd.Env = append(os.Environ(), "KVNODE_MULTIADDR="+bootstrapMultiaddr)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return e2edata.StatusFail, string(out)
	}
	return e2edata.StatusPass, ""
}

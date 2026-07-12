package com.gofsd.kvdemo

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kvmobile.Kvmobile
import org.json.JSONArray
import org.json.JSONObject
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File

/**
 * Instrumented E2E driver, called via `adb shell am instrument` by
 * pkg/e2erun's Android execution path (see that package's android.go) --
 * not run as part of a normal `./gradlew test`/`connectedCheck`.
 *
 * Reads a JSON array of event JSON strings (the same human-readable shape
 * pkg/e2edata.Event / kvctl-cli sendevent use, e.g.
 * `["{\"event\":\"get_public_key\"}", ...]`, with any `add` row's leader
 * address already resolved host-side -- see
 * pkg/e2erun.ResolveBootstrapPlaceholder) from the "rows" instrumentation
 * argument, calls Kvmobile.start() once and Kvmobile.sendEvent() per row in
 * order, and writes a JSON array of
 * `{"index":N,"pass":bool,"error":"..."}` results to this app's external
 * files dir (not the private filesDir Kvmobile.start uses for its own
 * daemon data) so the host side can `adb pull` it without needing
 * `run-as` -- deliberately decoupled from JUnit's own per-test-method
 * granularity, since "rows" is an arbitrary runtime-provided list, not a
 * fixed set of `@Test` methods.
 */
@RunWith(AndroidJUnit4::class)
class E2ETest {
    @Test
    fun runRows() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val args = InstrumentationRegistry.getArguments()
        val rows = JSONArray(args.getString("rows") ?: "[]")

        Kvmobile.start(context.filesDir.absolutePath)

        val results = JSONArray()
        var failures = 0
        for (i in 0 until rows.length()) {
            val result = JSONObject().put("index", i)
            val (pass, error) = sendWithRetry(rows.getString(i))
            result.put("pass", pass)
            if (!pass) {
                result.put("error", error)
                failures++
            }
            results.put(result)
        }

        File(context.getExternalFilesDir(null), "e2e_results.json").writeText(results.toString())

        if (failures > 0) {
            Assert.fail("$failures of ${rows.length()} row(s) failed -- see e2e_results.json")
        }
    }

    /**
     * Sends eventJson via Kvmobile.sendEvent, retrying for up to
     * READ_RETRY_BUDGET_MS (in READ_RETRY_DELAY_MS steps) if it's a
     * get_field/get_key event that comes back failed -- a raft follower's
     * local read can briefly lag just behind a set_field that only just
     * committed on the leader (the same documented caveat
     * pkg/e2erun.retryReadsIfNeeded works around for desktop/remote rows,
     * mirrored here since this retry has to happen on-device against the
     * real Kvmobile.sendEvent call, not something the host side can inject
     * after the fact). Every other event type is sent exactly once,
     * unretried -- a real failure there shouldn't be masked by blindly
     * retrying. Caught by a real get_field row immediately following its
     * set_field failing intermittently against a live deployed cluster,
     * the same issue already fixed for the desktop/remote path.
     */
    private fun sendWithRetry(eventJson: String): Pair<Boolean, String?> {
        val eventName = runCatching { JSONObject(eventJson).optString("event") }.getOrDefault("")
        val retryable = eventName == "get_field" || eventName == "get_key"
        val deadline = System.currentTimeMillis() + if (retryable) READ_RETRY_BUDGET_MS else 0

        while (true) {
            val outcome = runCatching { JSONObject(Kvmobile.sendEvent(eventJson)) }
            val (pass, error) = when {
                outcome.isFailure -> false to (outcome.exceptionOrNull()?.message ?: outcome.exceptionOrNull().toString())
                outcome.getOrNull()?.optString("event") == "error" -> false to outcome.getOrNull()?.optString("value")
                else -> true to null
            }
            if (pass || !retryable || System.currentTimeMillis() >= deadline) {
                return pass to error
            }
            Thread.sleep(READ_RETRY_DELAY_MS)
        }
    }

    companion object {
        // Slightly more generous than pkg/e2erun.retryReadsIfNeeded's 3s
        // desktop/remote budget, since this device joins over whatever
        // real network path it actually has to the bootstrap leader
        // (mobile Wi-Fi/cellular, not the loopback a desktop test node
        // uses) -- but this is *not* a fix for a follower that can never
        // catch up at all: tested directly against a real device with a
        // 20s budget and it still never did, while `kvctl-cli sendevent`
        // against the leader directly confirmed the write really was
        // committed there the whole time. That isolates the problem to
        // leader-to-follower AppendEntries delivery for this specific
        // follower never completing -- most likely this device's network
        // not actually being reachable back through the relay reservation
        // `relayMultiaddr` requests (see pkg/daemon.Config.RelayPeer's doc
        // comment) -- not a timing issue more retrying fixes. If a
        // get_field row still fails after this budget, look at
        // connectivity/relay, not this constant.
        private const val READ_RETRY_BUDGET_MS = 5000L
        private const val READ_RETRY_DELAY_MS = 500L
    }
}

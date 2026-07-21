package com.gofsd.kvdemo

import android.app.Activity
import android.os.Bundle
import android.view.View
import android.widget.AdapterView
import android.widget.ArrayAdapter
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.Spinner
import android.widget.TextView
import kvmobile.Kvmobile

// A generic runner over every kvmobile function (see CommandCatalog.kt):
// pick one from the spinner, fill in its parameters, tap Run, and see the
// result (or error) appended to the output log below. Deliberately not one
// hand-built screen per command -- kvmobile's surface mirrors the desktop
// mage/kvctl-cli catalog closely enough (see README.md's "Follower on
// Android" section) that a single dynamic form driven by that catalog
// covers all of it without ~50 near-identical bespoke layouts. This device
// runs a raft follower in-process (see kvmobile's package doc) and every
// Submit is forwarded from that follower to the current leader over
// pkg/daemon.ForwardProtocolID, wherever the leader happens to be running.
class MainActivity : Activity() {
    private lateinit var statusText: TextView
    private lateinit var commandSpinner: Spinner
    private lateinit var paramsContainer: LinearLayout
    private lateinit var runButton: Button
    private lateinit var outputText: TextView
    private lateinit var outputScroll: ScrollView

    private lateinit var commands: List<CommandSpec>
    private var paramFields: List<EditText> = emptyList()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        statusText = findViewById(R.id.statusText)
        commandSpinner = findViewById(R.id.commandSpinner)
        paramsContainer = findViewById(R.id.paramsContainer)
        runButton = findViewById(R.id.runButton)
        outputText = findViewById(R.id.outputText)
        outputScroll = findViewById(R.id.outputScroll)

        commands = buildCommands(filesDir.absolutePath) { line -> runOnUiThread { appendOutput(line) } }

        val adapter = ArrayAdapter(this, android.R.layout.simple_spinner_item, commands.map { it.label })
        adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item)
        commandSpinner.adapter = adapter
        commandSpinner.onItemSelectedListener = object : AdapterView.OnItemSelectedListener {
            override fun onItemSelected(parent: AdapterView<*>?, view: View?, position: Int, id: Long) {
                renderParamFields(commands[position])
            }
            override fun onNothingSelected(parent: AdapterView<*>?) {}
        }
        renderParamFields(commands[0])

        runButton.setOnClickListener { onRun() }

        Thread {
            try {
                val peerID = Kvmobile.start(filesDir.absolutePath)
                runOnUiThread { statusText.text = "Connected as $peerID" }
            } catch (e: Exception) {
                runOnUiThread { statusText.text = "Failed to start: ${e.message}" }
            }
        }.start()
    }

    // renderParamFields swaps paramsContainer's EditTexts for the selected
    // command's own parameter list -- called once at startup and again on
    // every spinner selection change.
    private fun renderParamFields(spec: CommandSpec) {
        paramsContainer.removeAllViews()
        paramFields = spec.params.map { hint ->
            EditText(this).apply {
                this.hint = hint
                layoutParams = LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.MATCH_PARENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT,
                )
            }
        }
        paramFields.forEach { paramsContainer.addView(it) }
    }

    // onRun reads the current parameter field values and calls the
    // selected command off the UI thread (every kvmobile call may block on
    // a shmring round trip), appending its result or error to the output
    // log either way.
    private fun onRun() {
        val spec = commands[commandSpinner.selectedItemPosition]
        val args = paramFields.map { it.text.toString() }
        runButton.isEnabled = false
        Thread {
            val line = try {
                "${spec.label}(${args.joinToString(", ")}) ->\n${spec.run(args)}"
            } catch (e: Exception) {
                "${spec.label}(${args.joinToString(", ")}) FAILED: ${e.message}"
            }
            runOnUiThread {
                appendOutput(line)
                runButton.isEnabled = true
            }
        }.start()
    }

    // appendOutput adds one entry to the running log and scrolls it into
    // view -- also how WatchExecute/WatchCommandLog's callbacks (see
    // CommandCatalog.kt) report notifications that arrive after their own
    // Run call already returned.
    private fun appendOutput(line: String) {
        outputText.append(if (outputText.text.isEmpty()) line else "\n\n$line")
        outputScroll.post { outputScroll.fullScroll(View.FOCUS_DOWN) }
    }
}

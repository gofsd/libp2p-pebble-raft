package com.gofsd.kvdemo

import android.app.Activity
import android.os.Bundle
import android.widget.Button
import android.widget.EditText
import android.widget.TextView
import kvmobile.Kvmobile

// Thin UI over mobile/kvmobile.Start/Submit/Get: this device runs a raft
// follower in-process (see kvmobile's package doc) and every Submit is
// forwarded from that follower to the current leader over
// pkg/daemon.ForwardProtocolID, wherever the leader happens to be running.
class MainActivity : Activity() {
    private lateinit var statusText: TextView
    private lateinit var keyInput: EditText
    private lateinit var valueInput: EditText
    private lateinit var resultText: TextView
    private lateinit var submitButton: Button
    private lateinit var getButton: Button

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        statusText = findViewById(R.id.statusText)
        keyInput = findViewById(R.id.keyInput)
        valueInput = findViewById(R.id.valueInput)
        resultText = findViewById(R.id.resultText)
        submitButton = findViewById(R.id.submitButton)
        getButton = findViewById(R.id.getButton)

        submitButton.isEnabled = false
        getButton.isEnabled = false
        submitButton.setOnClickListener { onSubmit() }
        getButton.setOnClickListener { onGet() }

        Thread {
            try {
                val peerID = Kvmobile.start(filesDir.absolutePath)
                runOnUiThread {
                    statusText.text = "Connected as $peerID"
                    submitButton.isEnabled = true
                    getButton.isEnabled = true
                }
            } catch (e: Exception) {
                runOnUiThread { statusText.text = "Failed to start: ${e.message}" }
            }
        }.start()
    }

    private fun onSubmit() {
        val key = keyInput.text.toString()
        val value = valueInput.text.toString()
        submitButton.isEnabled = false
        Thread {
            try {
                Kvmobile.submit(key, value)
                runOnUiThread { resultText.text = "Set $key = $value" }
            } catch (e: Exception) {
                runOnUiThread { resultText.text = "Submit failed: ${e.message}" }
            } finally {
                runOnUiThread { submitButton.isEnabled = true }
            }
        }.start()
    }

    private fun onGet() {
        val key = keyInput.text.toString()
        getButton.isEnabled = false
        Thread {
            try {
                val value = Kvmobile.get(key)
                runOnUiThread { resultText.text = "$key = $value" }
            } catch (e: Exception) {
                runOnUiThread { resultText.text = "Get failed: ${e.message}" }
            } finally {
                runOnUiThread { getButton.isEnabled = true }
            }
        }.start()
    }
}

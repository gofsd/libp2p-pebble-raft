package com.gofsd.kvdemo

import kvmobile.ExecuteCallback
import kvmobile.Kvmobile
import kvmobile.LogCallback

/**
 * One kvmobile call exposed in the command-runner UI (see MainActivity).
 * [params] names the ordered text fields to render for it; [run] performs
 * the call against those raw string values (off the UI thread) and
 * returns the text to append to the output log, or throws to report a
 * failure -- MainActivity renders either outcome the same way.
 */
class CommandSpec(
    val category: String,
    val name: String,
    val params: List<String>,
    val run: (List<String>) -> String,
) {
    val label: String get() = "$category: $name"
}

private fun ok() = "OK"

private fun String.toLongOrThrow(field: String): Long =
    toLongOrNull() ?: throw IllegalArgumentException("$field must be a whole number")

/**
 * The full kvmobile API surface, one CommandSpec per exported function --
 * mirrors README.md's "Follower on Android" section (and, through it,
 * every `mage` target it documents an Android equivalent for) closely
 * enough that this list and that section should stay in sync. [dataDir]
 * is bound automatically to the app's own private storage, the same
 * directory Start has always used, rather than exposed as a field -- it's
 * an internal Android path, not something a user meaningfully types.
 * [appendLog] lets WatchExecute/WatchCommandLog's callbacks post
 * additional output lines after their initial call returns, since both
 * register a standing subscription rather than answering once.
 */
fun buildCommands(dataDir: String, appendLog: (String) -> Unit): List<CommandSpec> {
    val commands = mutableListOf<CommandSpec>()
    fun add(category: String, name: String, params: List<String>, run: (List<String>) -> String) {
        commands += CommandSpec(category, name, params, run)
    }

    // Cluster lifecycle -- see README's "Follower on Android" section for
    // why Start/StartWithKey/Join/JoinWithKey/Stop/Delete/Leave/Rm map
    // onto desktop's addfollower/addfollowerwithkey/join/joinwithkey/
    // use/deletenode/leave/rm the way they do (kvmobile runs exactly one
    // daemon per process and can never bootstrap as a fresh leader).
    add("Cluster", "Start", emptyList()) { Kvmobile.start(dataDir) }
    add("Cluster", "StartWithKey", listOf("keyHex")) { a -> Kvmobile.startWithKey(dataDir, a[0]) }
    add("Cluster", "Join", listOf("leaderAddr")) { a -> Kvmobile.join(dataDir, a[0]) }
    add("Cluster", "JoinWithKey", listOf("keyHex", "leaderAddr")) { a -> Kvmobile.joinWithKey(dataDir, a[0], a[1]) }
    add("Cluster", "Stop", emptyList()) { Kvmobile.stop(); ok() }
    add("Cluster", "Delete", emptyList()) { Kvmobile.delete(dataDir); ok() }
    add("Cluster", "Leave", emptyList()) { Kvmobile.leave(); ok() }
    add("Cluster", "Rm", emptyList()) { Kvmobile.rm(); ok() }
    add("Cluster", "ListClusters", emptyList()) { Kvmobile.listClusters() }
    add("Cluster", "ListClusterMembers", emptyList()) { Kvmobile.listClusterMembers() }
    add("Cluster", "PeerID", emptyList()) { Kvmobile.peerID() }

    // KV
    add("KV", "Submit", listOf("key", "value")) { a -> Kvmobile.submit(a[0], a[1]); ok() }
    add("KV", "Get", listOf("key")) { a -> Kvmobile.get(a[0]) }
    add("KV", "RangeScan", listOf("start", "end", "limit (0=unlimited)")) { a ->
        Kvmobile.rangeScan(a[0], a[1], a[2].toLongOrThrow("limit"))
    }

    // Permits
    add("Permits", "RequestPermit", listOf("kind (peer|bootstrap)", "targetPeerID", "metadata")) { a ->
        Kvmobile.requestPermit(a[0], a[1], a[2]); ok()
    }
    add("Permits", "ConfirmPermit", listOf("kind (peer|bootstrap)", "targetPeerID")) { a ->
        Kvmobile.confirmPermit(a[0], a[1]); ok()
    }
    add("Permits", "RevokePermit", listOf("kind (peer|bootstrap)", "targetPeerID")) { a ->
        Kvmobile.revokePermit(a[0], a[1]); ok()
    }
    add("Permits", "RequestLogPermit", listOf("logKind", "targetPeerID", "metadata")) { a ->
        Kvmobile.requestLogPermit(a[0], a[1], a[2]); ok()
    }
    add("Permits", "ConfirmLogPermit", listOf("logKind", "targetPeerID")) { a ->
        Kvmobile.confirmLogPermit(a[0], a[1]); ok()
    }
    add("Permits", "RevokeLogPermit", listOf("logKind", "targetPeerID")) { a ->
        Kvmobile.revokeLogPermit(a[0], a[1]); ok()
    }

    // Execute -- the raft-bypassing peer-to-peer notification.
    add("Execute", "Execute", listOf("destPeerID", "value")) { a -> Kvmobile.execute(a[0], a[1]); ok() }
    add("Execute", "PollExecute", emptyList()) { Kvmobile.pollExecute() }
    add("Execute", "WatchExecute", emptyList()) {
        Kvmobile.watchExecute(object : ExecuteCallback {
            override fun onNotification(senderPeerID: String, value: String) {
                appendLog("Execute from $senderPeerID: $value")
            }
        })
        "Watching -- notifications appear below as they arrive"
    }
    add("Execute", "StopWatchExecute", emptyList()) { Kvmobile.stopWatchExecute(); ok() }

    // pkg/logrecord read/write.
    add("Log records", "LogAppend", listOf("kind", "unitID", "fieldsJSON", "narrative")) { a ->
        Kvmobile.logAppend(a[0], a[1], a[2], a[3]); ok()
    }
    add(
        "Log records", "LogQuery",
        listOf("kind", "unitID", "since (RFC3339 or blank)", "until (RFC3339 or blank)", "limit (blank=unlimited)"),
    ) { a -> Kvmobile.logQuery(a[0], a[1], a[2], a[3], a[4]) }

    // Group/Command ACL catalog -- daemon-enforced records, see README's
    // "Group/command ACL" section for the model this mirrors exactly.
    add("Group", "CreateGroup", listOf("id", "name")) { a -> Kvmobile.createGroup(a[0], a[1]); ok() }
    add("Group", "UpdateGroup", listOf("id", "name")) { a -> Kvmobile.updateGroup(a[0], a[1]); ok() }
    add("Group", "DeleteGroup", listOf("id")) { a -> Kvmobile.deleteGroup(a[0]); ok() }
    add("Group", "GetGroup", listOf("id")) { a -> Kvmobile.getGroup(a[0]) }
    add("Group", "ListGroups", emptyList()) { Kvmobile.listGroups() }

    add("Command", "CreateCommand", listOf("id", "name", "targetPeerID")) { a ->
        Kvmobile.createCommand(a[0], a[1], a[2]); ok()
    }
    add("Command", "UpdateCommand", listOf("id", "name", "targetPeerID")) { a ->
        Kvmobile.updateCommand(a[0], a[1], a[2]); ok()
    }
    add("Command", "DeleteCommand", listOf("id")) { a -> Kvmobile.deleteCommand(a[0]); ok() }
    add("Command", "GetCommand", listOf("id")) { a -> Kvmobile.getCommand(a[0]) }
    add("Command", "ListCommands", emptyList()) { Kvmobile.listCommands() }

    add("Links", "AddCommandToGroup", listOf("commandID", "groupID")) { a ->
        Kvmobile.addCommandToGroup(a[0], a[1]); ok()
    }
    add("Links", "RemoveCommandFromGroup", listOf("commandID", "groupID")) { a ->
        Kvmobile.removeCommandFromGroup(a[0], a[1]); ok()
    }
    add("Links", "ListGroupsForCommand", listOf("commandID")) { a -> Kvmobile.listGroupsForCommand(a[0]) }
    add("Links", "AddPeerToGroup", listOf("peerID", "groupID")) { a -> Kvmobile.addPeerToGroup(a[0], a[1]); ok() }
    add("Links", "RemovePeerFromGroup", listOf("peerID", "groupID")) { a ->
        Kvmobile.removePeerFromGroup(a[0], a[1]); ok()
    }
    add("Links", "ListGroupsForPeer", listOf("peerID")) { a -> Kvmobile.listGroupsForPeer(a[0]) }

    // Dispatch -- turns a Command from the catalog into a request/response
    // flow (see README's "Follower on Android" section, dispatch layer).
    add("Dispatch", "SubmitCommand", listOf("commandID", "inputsJSON")) { a -> Kvmobile.submitCommand(a[0], a[1]) }
    add("Dispatch", "GetCommandRequest", listOf("commandID", "instanceID")) { a ->
        Kvmobile.getCommandRequest(a[0], a[1])
    }
    add("Dispatch", "ListCommandRequests", listOf("commandID")) { a -> Kvmobile.listCommandRequests(a[0]) }
    add("Dispatch", "ListExecutionsByPeer", listOf("peerID")) { a -> Kvmobile.listExecutionsByPeer(a[0]) }
    add(
        "Dispatch", "AppendCommandLog",
        listOf("requesterPeerID (blank=no poke)", "instanceID", "fieldsJSON", "narrative"),
    ) { a -> Kvmobile.appendCommandLog(a[0], a[1], a[2], a[3]); ok() }
    add(
        "Dispatch", "QueryCommandLog",
        listOf("instanceID", "since (RFC3339 or blank)", "until (RFC3339 or blank)", "limit (blank=unlimited)"),
    ) { a -> Kvmobile.queryCommandLog(a[0], a[1], a[2], a[3]) }
    add("Dispatch", "LatestCommandLog", listOf("instanceID")) { a -> Kvmobile.latestCommandLog(a[0]) }
    add("Dispatch", "WatchCommandLog", listOf("instanceID")) { a ->
        val instanceID = a[0]
        Kvmobile.watchCommandLog(instanceID, object : LogCallback {
            override fun onRecords(recordsJSON: String) {
                appendLog("CommandLog[$instanceID]: $recordsJSON")
            }
        })
        "Watching -- new records appear below as they arrive"
    }
    add("Dispatch", "StopWatchCommandLog", listOf("instanceID")) { a -> Kvmobile.stopWatchCommandLog(a[0]); ok() }

    // Raw escape hatch -- the same one E2ETest uses, see its own doc
    // comment and README's "Follower on Android" section.
    add("Raw", "SendEvent", listOf("eventJSON")) { a -> Kvmobile.sendEvent(a[0]) }

    return commands
}

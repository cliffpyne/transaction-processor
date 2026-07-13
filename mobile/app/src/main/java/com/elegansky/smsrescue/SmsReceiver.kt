package com.elegansky.smsrescue

import android.app.role.RoleManager
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.os.Build
import android.provider.Telephony
import com.elegansky.smsrescue.db.QueueDb
import com.elegansky.smsrescue.db.QueueEntry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

/**
 * Fires for every incoming SMS. POSTs the message straight to
 * /api/sms-rescue from the receiver's async block — no Room round-trip,
 * no WorkManager delay. Only if the POST returns a network failure
 * (server unreachable, timeout, 5xx) does the row get queued for the
 * SmsWorker drainer to retry later.
 *
 * Android fires SMS_RECEIVED to every app with RECEIVE_SMS permission,
 * AND SMS_DELIVER exclusively to the default SMS app. When we're
 * the default, both broadcasts arrive here → we'd double-post.
 * SmsReceiver bails on SMS_RECEIVED when we're default; only
 * SmsDeliverReceiver drives the pipeline in that case.
 */
class SmsReceiver : BroadcastReceiver() {

    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action == Telephony.Sms.Intents.SMS_RECEIVED_ACTION
            && isDefaultSmsApp(context)
        ) return
        handle(context, intent)
    }

    fun handle(context: Context, intent: Intent) {
        val messages = Telephony.Sms.Intents.getMessagesFromIntent(intent) ?: return
        if (messages.isEmpty()) return
        val settings = SettingsRepo(context)
        val sender = messages.first().originatingAddress ?: return
        if (!settings.senderAllowed(sender)) return
        val body = messages.joinToString(separator = "") { it.messageBody ?: "" }
        if (body.isBlank()) return

        val entry = QueueEntry(
            sender = sender,
            body = body,
            receivedAt = System.currentTimeMillis(),
        )

        val pending = goAsync()
        CoroutineScope(Dispatchers.IO).launch {
            try {
                val api = SmsPusher.api(context)
                if (api == null) {
                    QueueDb.get(context).dao().insert(entry)
                    return@launch
                }
                val outcome = SmsPusher.postOne(api, entry)
                if (outcome.networkFailure) {
                    QueueDb.get(context).dao().insert(entry)
                    SmsWorker.enqueueDrain(context)
                    return@launch
                }
                val id = QueueDb.get(context).dao().insert(entry)
                QueueDb.get(context).dao().recordAttempt(
                    id, outcome.status, outcome.error, outcome.terminal,
                )
                if (outcome.deleteSms) {
                    SmsPusher.deleteFromInbox(context, entry.sender, entry.body)
                }
            } finally {
                pending.finish()
            }
        }
    }

    private fun isDefaultSmsApp(ctx: Context): Boolean {
        return if (Build.VERSION.SDK_INT >= 29) {
            ctx.getSystemService(RoleManager::class.java)
                ?.isRoleHeld(RoleManager.ROLE_SMS) == true
        } else {
            Telephony.Sms.getDefaultSmsPackage(ctx) == ctx.packageName
        }
    }
}

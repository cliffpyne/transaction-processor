package com.elegansky.smsrescue

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.provider.Telephony
import com.elegansky.smsrescue.db.QueueDb
import com.elegansky.smsrescue.db.QueueEntry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

/**
 * Fires for every incoming SMS. Aggregates multi-part messages by sender,
 * checks the sender whitelist, appends to Room, then kicks the drain worker.
 */
class SmsReceiver : BroadcastReceiver() {

    override fun onReceive(context: Context, intent: Intent) {
        val messages = Telephony.Sms.Intents.getMessagesFromIntent(intent) ?: return
        if (messages.isEmpty()) return
        val settings = SettingsRepo(context)
        // Multi-part SMS: same sender, concat body
        val sender = messages.first().originatingAddress ?: return
        if (!settings.senderAllowed(sender)) return
        val body = messages.joinToString(separator = "") { it.messageBody ?: "" }
        if (body.isBlank()) return

        val pending = goAsync()
        CoroutineScope(Dispatchers.IO).launch {
            try {
                QueueDb.get(context).dao().insert(
                    QueueEntry(
                        sender = sender,
                        body = body,
                        receivedAt = System.currentTimeMillis(),
                    )
                )
                SmsWorker.enqueueDrain(context)
            } finally {
                pending.finish()
            }
        }
    }
}

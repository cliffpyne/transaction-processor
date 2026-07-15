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
 * does the row get queued for the SmsWorker drainer to retry later.
 *
 * When we're the default SMS handler, Android fires both SMS_DELIVER
 * (private, to us) AND SMS_RECEIVED (public). We'd double-post. This
 * class handles SMS_RECEIVED but bails when we're default; then
 * SmsDeliverReceiver becomes the sole entry point.
 */
class SmsReceiver : BroadcastReceiver() {

    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action == Telephony.Sms.Intents.SMS_RECEIVED_ACTION
            && isDefaultSmsApp(context)
        ) return
        val pending = goAsync()
        process(context, intent, pending)
    }

    companion object {

        /**
         * Shared processor called from both SmsReceiver.onReceive() and
         * SmsDeliverReceiver.onReceive(). Each caller owns its own
         * PendingResult from its OWN goAsync() call — this method just
         * uses it, then finishes it in a finally block. Calling
         * goAsync() from a non-active broadcast (e.g. through a fresh
         * SmsReceiver() instance) returns null and later crashes on
         * pending.finish() — that's the NPE we were seeing.
         */
        fun process(
            context: Context,
            intent: Intent,
            pending: BroadcastReceiver.PendingResult?,
        ) {
            val messages = Telephony.Sms.Intents.getMessagesFromIntent(intent)
            if (messages == null || messages.isEmpty()) {
                pending?.finish()
                return
            }
            val settings = SettingsRepo(context)
            val sender = messages.first().originatingAddress
            if (sender == null || !settings.senderAllowed(sender)) {
                pending?.finish()
                return
            }
            val body = messages.joinToString(separator = "") { it.messageBody ?: "" }
            if (body.isBlank()) {
                pending?.finish()
                return
            }

            val entry = QueueEntry(
                sender = sender,
                body = body,
                receivedAt = System.currentTimeMillis(),
            )
            CoroutineScope(Dispatchers.IO).launch {
                try {
                    // Receiver path uses the FAST client (3 s connect / 5 s read)
                    // so the total time in this coroutine stays under Android's
                    // 10 s broadcast-ANR limit. Slow network → we bail early,
                    // queue the row, and the drainer picks it up.
                    val api = SmsPusher.api(context, fastMode = true)
                    if (api == null) {
                        QueueDb.get(context).dao().insert(entry)
                        return@launch
                    }
                    val outcome = SmsPusher.postOne(api, entry)
                    if (outcome.networkFailure) {
                        // Fast POST failed — hand off to the drainer, which
                        // uses the slow client and has ~10 min of budget.
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
                    try { pending?.finish() } catch (_: Throwable) { /* already finished */ }
                }
            }
        }

        fun isDefaultSmsApp(ctx: Context): Boolean {
            return if (Build.VERSION.SDK_INT >= 29) {
                ctx.getSystemService(RoleManager::class.java)
                    ?.isRoleHeld(RoleManager.ROLE_SMS) == true
            } else {
                Telephony.Sms.getDefaultSmsPackage(ctx) == ctx.packageName
            }
        }
    }
}

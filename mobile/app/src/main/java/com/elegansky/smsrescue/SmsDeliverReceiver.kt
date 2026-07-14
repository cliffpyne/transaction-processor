package com.elegansky.smsrescue

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * Only invoked when this app is the DEFAULT SMS handler. Owns its own
 * PendingResult from goAsync() (calling it here — not inside the
 * shared processor — because goAsync() only works on the receiver
 * that's actively dispatching), then delegates to SmsReceiver.process
 * for the actual queue + POST + delete logic.
 */
class SmsDeliverReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        val pending = goAsync()
        SmsReceiver.process(context, intent, pending)
    }
}

package com.elegansky.smsrescue

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * Only invoked when this app is the DEFAULT SMS handler. Delegate to
 * SmsReceiver so the same enqueue+drain path runs.
 */
class SmsDeliverReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        SmsReceiver().onReceive(context, intent)
    }
}

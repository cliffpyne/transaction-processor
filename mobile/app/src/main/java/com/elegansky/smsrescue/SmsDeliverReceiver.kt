package com.elegansky.smsrescue

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * Only invoked when this app is the DEFAULT SMS handler. Skip the
 * default-check guard in SmsReceiver.onReceive() and go straight to
 * handle() so this receiver becomes the sole pipeline entry when
 * we're default (avoids double-processing with SMS_RECEIVED).
 */
class SmsDeliverReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        SmsReceiver().handle(context, intent)
    }
}

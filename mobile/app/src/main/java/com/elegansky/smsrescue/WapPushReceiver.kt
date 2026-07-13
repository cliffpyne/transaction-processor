package com.elegansky.smsrescue

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * MMS entry point. Required by the platform to be eligible as the
 * default SMS handler — we ignore MMS (bank SMS are plain text).
 */
class WapPushReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) { /* no-op */ }
}

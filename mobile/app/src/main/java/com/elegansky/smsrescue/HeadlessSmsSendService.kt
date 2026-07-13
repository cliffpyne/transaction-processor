package com.elegansky.smsrescue

import android.app.Service
import android.content.Intent
import android.os.IBinder

/**
 * Required stub so the platform lists our app in the
 * "Choose default SMS app" prompt. We don't actually send SMS —
 * we only need to be eligible for delete-inbox privilege.
 */
class HeadlessSmsSendService : Service() {
    override fun onBind(intent: Intent?): IBinder? = null
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int) = START_NOT_STICKY
}

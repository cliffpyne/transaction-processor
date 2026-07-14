package com.elegansky.smsrescue

import android.app.Notification
import android.app.PendingIntent
import android.app.Service
import android.content.Intent
import android.os.IBinder
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat

/**
 * Persistent foreground service whose ONLY job is to keep the app process
 * alive so SmsReceiver / SmsDeliverReceiver actually fire when SMS arrive.
 *
 * Samsung's aggressive app-standby will otherwise hibernate a
 * default-SMS-role app that hasn't shown UI recently — SMS_DELIVER
 * broadcasts get dropped and legitimate payment forwards are lost.
 * The persistent notification is the price the platform charges for
 * being unkillable.
 */
class KeepAliveService : Service() {

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val tapIntent = Intent(this, MainActivity::class.java)
            .setFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP)
        val pending = PendingIntent.getActivity(
            this, 0, tapIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
        val notif: Notification = NotificationCompat.Builder(this, App.NOTIF_CHANNEL_STATUS)
            .setContentTitle("SMS Rescue")
            .setContentText("Watching for payment SMS")
            .setSmallIcon(android.R.drawable.ic_dialog_email)
            .setContentIntent(pending)
            .setOngoing(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
        startForeground(NOTIF_ID, notif)
        return START_STICKY
    }

    companion object {
        private const val NOTIF_ID = 42

        fun start(ctx: android.content.Context) {
            val intent = Intent(ctx, KeepAliveService::class.java)
            ContextCompat.startForegroundService(ctx, intent)
        }
    }
}

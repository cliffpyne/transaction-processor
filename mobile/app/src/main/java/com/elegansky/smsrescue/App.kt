package com.elegansky.smsrescue

import android.app.Application
import android.app.NotificationChannel
import android.app.NotificationManager
import android.os.Build
import androidx.work.Configuration
import androidx.work.WorkManager

class App : Application(), Configuration.Provider {

    override fun onCreate() {
        super.onCreate()
        instance = this
        SettingsRepo(this).migrateLegacyPortalUrl()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val nm = getSystemService(NotificationManager::class.java)
            nm?.createNotificationChannel(
                NotificationChannel(
                    NOTIF_CHANNEL_STATUS,
                    "SMS Rescue status",
                    NotificationManager.IMPORTANCE_LOW,
                )
            )
        }
        // Kick a drain on cold-start in case any queued rows were left behind.
        SmsWorker.enqueueDrain(this)
        // Start the foreground service so Samsung can't hibernate us mid-day
        // and drop incoming SMS_DELIVER broadcasts.
        KeepAliveService.start(this)
    }

    override val workManagerConfiguration: Configuration =
        Configuration.Builder().build()

    companion object {
        const val NOTIF_CHANNEL_STATUS = "sms_rescue_status"
        lateinit var instance: App
            private set
    }
}

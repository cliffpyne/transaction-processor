package com.elegansky.smsrescue

import android.content.Context
import android.util.Log
import androidx.work.*
import com.elegansky.smsrescue.db.QueueDb

/**
 * Retry-drainer for rows the direct-post path couldn't complete
 * (server unreachable, 5xx, or persistent 404 ref_not_found). Also
 * used for the "Import inbox from last Saturday" bulk backfill.
 *
 * Always returns Result.success() so WorkManager cannot get stuck in
 * exponential backoff. Pending rows re-drain on the next SMS arrival,
 * boot, or manual tap.
 */
class SmsWorker(ctx: Context, params: WorkerParameters) : CoroutineWorker(ctx, params) {

    override suspend fun doWork(): Result {
        val ctx = applicationContext
        val api = SmsPusher.api(ctx)
        if (api == null) {
            Log.w(TAG, "settings not ready — nothing to drain")
            return Result.success()
        }
        val dao = QueueDb.get(ctx).dao()

        while (true) {
            val batch = dao.nextBatch()
            if (batch.isEmpty()) break
            for (entry in batch) {
                val outcome = SmsPusher.postOne(api, entry)
                dao.recordAttempt(entry.id, outcome.status, outcome.error, outcome.terminal)
                if (outcome.deleteSms) {
                    SmsPusher.deleteFromInbox(ctx, entry.sender, entry.body)
                }
            }
        }
        return Result.success()
    }

    companion object {
        private const val TAG = "SmsWorker"
        private const val UNIQUE_NAME = "sms-drain"

        /** Fires the drainer immediately. REPLACE cancels any pending or
         *  backed-off worker so the user's "Drain now" tap never waits. */
        fun enqueueDrain(ctx: Context) {
            val req = OneTimeWorkRequestBuilder<SmsWorker>()
                .setConstraints(
                    Constraints.Builder()
                        .setRequiredNetworkType(NetworkType.CONNECTED)
                        .build()
                )
                .build()
            WorkManager.getInstance(ctx).enqueueUniqueWork(
                UNIQUE_NAME, ExistingWorkPolicy.REPLACE, req,
            )
        }
    }
}

package com.elegansky.smsrescue

import android.content.ContentUris
import android.content.Context
import android.net.Uri
import android.provider.Telephony
import android.util.Log
import androidx.work.*
import com.elegansky.smsrescue.db.QueueDb
import com.elegansky.smsrescue.db.QueueEntry
import com.elegansky.smsrescue.net.ApiFactory
import com.elegansky.smsrescue.net.SmsRescueApi
import com.elegansky.smsrescue.net.SmsRescueRequest
import java.util.concurrent.TimeUnit

/**
 * Drains the queue: POSTs each pending row to /api/sms-rescue and reacts
 * to the response. Enqueued opportunistically on every SMS arrival + on
 * cold start + on boot; WorkManager coalesces if we ask twice.
 */
class SmsWorker(ctx: Context, params: WorkerParameters) : CoroutineWorker(ctx, params) {

    override suspend fun doWork(): Result {
        val ctx = applicationContext
        val settings = SettingsRepo(ctx)
        if (!settings.ready()) {
            Log.w(TAG, "settings not ready (no url/token) — waiting")
            return Result.retry()
        }
        val api = ApiFactory.build(settings.serverUrl, settings.token)
        val dao = QueueDb.get(ctx).dao()

        while (true) {
            val batch = dao.nextBatch()
            if (batch.isEmpty()) break
            for (entry in batch) {
                val outcome = try {
                    postOne(api, entry)
                } catch (t: Throwable) {
                    Log.w(TAG, "network error on id=${entry.id}: ${t.message}")
                    continue  // leave the row pending; next drain will retry
                }
                dao.recordAttempt(entry.id, outcome.status, outcome.error, outcome.terminal)
                if (outcome.deleteSms) {
                    // Only the DEFAULT SMS app can delete; if we aren't
                    // default the call silently returns 0 rows deleted —
                    // which is fine, just leaves the SMS in the inbox.
                    deleteSms(ctx, entry.body, entry.sender)
                }
            }
        }
        // Always success — never trigger WorkManager exponential backoff.
        // Rows that didn't reach a terminal state stay in Room with
        // terminal='' and get re-tried whenever the next drain fires
        // (new SMS arrival, boot, or the user tapping "Drain queue now").
        return Result.success()
    }

    private suspend fun postOne(api: SmsRescueApi, entry: QueueEntry): PostOutcome {
        val receivedIso = java.time.Instant.ofEpochMilli(entry.receivedAt).toString()
        val resp = api.rescue(
            SmsRescueRequest(
                message = entry.body,
                sender = entry.sender,
                received_at = receivedIso,
            )
        )
        val body = try { resp.body() } catch (_: Throwable) { null }
        val err = body?.error
        return when (resp.code()) {
            200 -> PostOutcome(200, null, "RESCUED", deleteSms = true)
            // 409 = safe-to-delete (already rescued or already PASSED)
            409 -> {
                val t = if (err == "already_rescued") "ALREADY" else "NOT_FAILED"
                PostOutcome(409, err, t, deleteSms = true)
            }
            400 -> PostOutcome(400, err ?: "extract_failed", "EXTRACT_FAILED", deleteSms = false)
            404 -> {
                val t = if (err == "plate_not_in_records") "PLATE_UNKNOWN" else "REF_NOT_FOUND"
                // ref_not_found may become findable later once the bank writes
                // the row — retry a few times before giving up.
                val terminal = if (t == "REF_NOT_FOUND" && entry.attempts < 5) "" else t
                PostOutcome(404, err, terminal, retryable = terminal.isEmpty())
            }
            else -> PostOutcome(resp.code(), err ?: "http_${resp.code()}", "", retryable = true)
        }
    }

    private fun deleteSms(ctx: Context, body: String, sender: String) {
        // Match by (address, body) — reliable within an inbox we control.
        try {
            val uri = Telephony.Sms.CONTENT_URI
            ctx.contentResolver.delete(
                uri,
                "${Telephony.Sms.ADDRESS}=? AND ${Telephony.Sms.BODY}=?",
                arrayOf(sender, body),
            )
        } catch (t: Throwable) {
            Log.w(TAG, "sms delete failed: ${t.message}")
        }
    }

    private data class PostOutcome(
        val status: Int,
        val error: String?,
        val terminal: String,
        val deleteSms: Boolean = false,
        val retryable: Boolean = false,
    )

    companion object {
        private const val TAG = "SmsWorker"
        private const val UNIQUE_NAME = "sms-drain"

        /** One-shot drain. ExistingWorkPolicy.REPLACE cancels any pending
         *  or backed-off worker and starts a fresh one immediately —
         *  a user tapping "Drain queue now" always fires this instant,
         *  never waits for an exponential-backoff cooldown. */
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

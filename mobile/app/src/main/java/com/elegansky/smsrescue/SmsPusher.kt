package com.elegansky.smsrescue

import android.content.Context
import android.provider.Telephony
import android.util.Log
import com.elegansky.smsrescue.db.QueueEntry
import com.elegansky.smsrescue.net.ApiFactory
import com.elegansky.smsrescue.net.SmsRescueApi
import com.elegansky.smsrescue.net.SmsRescueRequest

/**
 * Single-row POST used by both:
 *   - SmsReceiver on a fresh SMS (fires within milliseconds of arrival)
 *   - SmsWorker on queued rows (drain path)
 *
 * Returns a PostOutcome. The caller decides how to persist (Room row
 * for the drainer, direct delete for the receiver).
 */
data class PostOutcome(
    val status: Int,
    val error: String?,
    val terminal: String,
    val deleteSms: Boolean = false,
    val networkFailure: Boolean = false,
)

object SmsPusher {

    private const val TAG = "SmsPusher"

    fun api(ctx: Context): SmsRescueApi? {
        val settings = SettingsRepo(ctx)
        if (!settings.ready()) return null
        return ApiFactory.build(settings.serverUrl, settings.token)
    }

    suspend fun postOne(api: SmsRescueApi, entry: QueueEntry): PostOutcome {
        val receivedIso = java.time.Instant.ofEpochMilli(entry.receivedAt).toString()
        val resp = try {
            api.rescue(
                SmsRescueRequest(
                    message = entry.body,
                    sender  = entry.sender,
                    received_at = receivedIso,
                )
            )
        } catch (t: Throwable) {
            Log.w(TAG, "network error on ${entry.sender}: ${t.message}")
            return PostOutcome(0, t.message, "", networkFailure = true)
        }
        val body = try { resp.body() } catch (_: Throwable) { null }
        val err = body?.error
        return when (resp.code()) {
            200 -> PostOutcome(200, null, "RESCUED", deleteSms = true)
            409 -> {
                val t = if (err == "already_rescued") "ALREADY" else "REF_IN_PASSED"
                PostOutcome(409, err, t, deleteSms = true)
            }
            400 -> PostOutcome(400, err ?: "extract_failed", "EXTRACT_FAILED")
            404 -> {
                val t = if (err == "plate_not_in_records") "PLATE_UNKNOWN" else "REF_NOT_FOUND"
                // ref_not_found may become findable later once the bank
                // writes the row — keep pending for up to 5 attempts.
                val terminal = if (t == "REF_NOT_FOUND" && entry.attempts < 5) "" else t
                PostOutcome(404, err, terminal, networkFailure = terminal.isEmpty())
            }
            else -> PostOutcome(resp.code(), err ?: "http_${resp.code()}", "", networkFailure = true)
        }
    }

    /** Delete the SMS row matching (sender, body) from the phone's inbox.
     *  Only works when this app is the default SMS handler — otherwise
     *  the ContentResolver call silently affects 0 rows. */
    fun deleteFromInbox(ctx: Context, sender: String, body: String) {
        try {
            ctx.contentResolver.delete(
                Telephony.Sms.CONTENT_URI,
                "${Telephony.Sms.ADDRESS}=? AND ${Telephony.Sms.BODY}=?",
                arrayOf(sender, body),
            )
        } catch (t: Throwable) {
            Log.w(TAG, "sms delete failed: ${t.message}")
        }
    }
}

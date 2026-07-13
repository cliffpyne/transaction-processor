package com.elegansky.smsrescue

import android.content.Context
import android.provider.Telephony
import android.util.Log
import com.elegansky.smsrescue.db.QueueDb
import com.elegansky.smsrescue.db.QueueEntry
import java.time.DayOfWeek
import java.time.LocalDate
import java.time.ZoneId
import java.time.temporal.TemporalAdjusters

/**
 * Scans the phone's SMS inbox from the most recent Saturday onward and
 * appends every matching row to the queue so the drainer can push them
 * to /api/sms-rescue. Idempotent: the server returns 409 already_rescued
 * on replays, so a row imported twice doesn't double-move anything.
 */
object InboxImporter {

    /** Reads every SMS in content://sms/inbox with date >= last Saturday
     *  (00:00 phone-local). Returns the count of rows enqueued. */
    suspend fun importSinceLastSaturday(ctx: Context): Int {
        val settings = SettingsRepo(ctx)
        val dao = QueueDb.get(ctx).dao()
        val since = mostRecentSaturdayEpochMs()
        val cur = ctx.contentResolver.query(
            Telephony.Sms.Inbox.CONTENT_URI,
            arrayOf(
                Telephony.Sms._ID,
                Telephony.Sms.ADDRESS,
                Telephony.Sms.BODY,
                Telephony.Sms.DATE,
            ),
            "${Telephony.Sms.DATE} >= ?",
            arrayOf(since.toString()),
            "${Telephony.Sms.DATE} ASC",
        ) ?: return 0
        var enqueued = 0
        cur.use { c ->
            val iId   = c.getColumnIndexOrThrow(Telephony.Sms._ID)
            val iAddr = c.getColumnIndexOrThrow(Telephony.Sms.ADDRESS)
            val iBody = c.getColumnIndexOrThrow(Telephony.Sms.BODY)
            val iDate = c.getColumnIndexOrThrow(Telephony.Sms.DATE)
            while (c.moveToNext()) {
                val sender = c.getString(iAddr) ?: continue
                if (!settings.senderAllowed(sender)) continue
                val body = c.getString(iBody) ?: continue
                if (body.isBlank()) continue
                dao.insert(
                    QueueEntry(
                        sender = sender,
                        body = body,
                        receivedAt = c.getLong(iDate),
                        telephonyId = c.getLong(iId),
                    )
                )
                enqueued++
            }
        }
        if (enqueued > 0) SmsWorker.enqueueDrain(ctx)
        Log.i("InboxImporter", "enqueued $enqueued rows since ${since}")
        return enqueued
    }

    private fun mostRecentSaturdayEpochMs(): Long {
        val today = LocalDate.now(ZoneId.systemDefault())
        val sat = if (today.dayOfWeek == DayOfWeek.SATURDAY) today
                  else today.with(TemporalAdjusters.previous(DayOfWeek.SATURDAY))
        return sat.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
    }
}

package com.elegansky.smsrescue.db

import androidx.room.Entity
import androidx.room.PrimaryKey

@Entity(tableName = "queue")
data class QueueEntry(
    @PrimaryKey(autoGenerate = true) val id: Long = 0,
    val sender: String,
    val body: String,
    val receivedAt: Long,
    /** Row _id in Android's content://sms/inbox, so we can delete it later. */
    val telephonyId: Long? = null,
    /** Server's last observed HTTP status for this row (0 = never tried). */
    val lastStatus: Int = 0,
    /** Server's last error tag (`ref_not_found`, etc.) if any. */
    val lastError: String? = null,
    /** How many POSTs we've attempted for this row. */
    val attempts: Int = 0,
    /** Terminal states: RESCUED, ALREADY, NOT_FAILED, EXTRACT_FAILED,
     *  REF_NOT_FOUND, PLATE_UNKNOWN. Empty = still pending. */
    val terminal: String = "",
)

package com.elegansky.smsrescue.db

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.Query

@Dao
interface QueueDao {

    @Insert
    suspend fun insert(entry: QueueEntry): Long

    @Query("SELECT * FROM queue WHERE terminal = '' ORDER BY receivedAt ASC LIMIT 20")
    suspend fun nextBatch(): List<QueueEntry>

    @Query("UPDATE queue SET lastStatus = :status, lastError = :error, " +
           "attempts = attempts + 1, terminal = :terminal WHERE id = :id")
    suspend fun recordAttempt(id: Long, status: Int, error: String?, terminal: String)

    @Query("SELECT COUNT(*) FROM queue WHERE terminal = ''")
    suspend fun pendingCount(): Int

    @Query("SELECT COUNT(*) FROM queue WHERE terminal = 'RESCUED'")
    suspend fun rescuedCount(): Int

    @Query("SELECT COUNT(*) FROM queue WHERE terminal IN ('EXTRACT_FAILED','REF_NOT_FOUND','PLATE_UNKNOWN')")
    suspend fun flaggedCount(): Int

    @Query("SELECT * FROM queue WHERE terminal IN ('EXTRACT_FAILED','REF_NOT_FOUND','PLATE_UNKNOWN') " +
           "ORDER BY receivedAt DESC LIMIT 50")
    suspend fun flagged(): List<QueueEntry>
}

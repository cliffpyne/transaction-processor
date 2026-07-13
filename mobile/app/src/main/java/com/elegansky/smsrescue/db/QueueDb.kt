package com.elegansky.smsrescue.db

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase

@Database(entities = [QueueEntry::class], version = 1, exportSchema = false)
abstract class QueueDb : RoomDatabase() {
    abstract fun dao(): QueueDao

    companion object {
        @Volatile private var INSTANCE: QueueDb? = null

        fun get(ctx: Context): QueueDb =
            INSTANCE ?: synchronized(this) {
                INSTANCE ?: Room.databaseBuilder(
                    ctx.applicationContext, QueueDb::class.java, "sms-rescue.db"
                ).build().also { INSTANCE = it }
            }
    }
}

package com.elegansky.smsrescue

import android.content.Context
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey

/**
 * Encrypted key-value storage for the server URL + token + sender filter.
 * Encryption at rest so the shared secret can't be trivially exfiltrated
 * by an adversary with filesystem access to the device.
 */
class SettingsRepo(private val ctx: Context) {

    private val prefs by lazy {
        val key = MasterKey.Builder(ctx)
            .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
            .build()
        EncryptedSharedPreferences.create(
            ctx, "sms-rescue-prefs", key,
            EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
            EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM,
        )
    }

    var serverUrl: String
        get() = prefs.getString(KEY_URL, "https://portal.eleganskyboda.com") ?: ""
        set(value) { prefs.edit().putString(KEY_URL, value).apply() }

    var token: String
        get() = prefs.getString(KEY_TOKEN, "") ?: ""
        set(value) { prefs.edit().putString(KEY_TOKEN, value).apply() }

    /**
     * Comma-separated list of sender IDs to accept — everything else is
     * ignored. Empty = accept every SMS, useful for debug only.
     */
    var senderWhitelist: String
        get() = prefs.getString(KEY_SENDERS, "NMB,CRDB,NMBBANK,CRDBBANK") ?: ""
        set(value) { prefs.edit().putString(KEY_SENDERS, value).apply() }

    fun senderAllowed(sender: String): Boolean {
        val list = senderWhitelist.split(',').map { it.trim().uppercase() }.filter { it.isNotEmpty() }
        if (list.isEmpty()) return true
        val s = sender.uppercase()
        return list.any { s.contains(it) }
    }

    fun ready(): Boolean = serverUrl.isNotBlank() && token.isNotBlank()

    companion object {
        private const val KEY_URL = "server_url"
        private const val KEY_TOKEN = "server_token"
        private const val KEY_SENDERS = "sender_whitelist"
    }
}

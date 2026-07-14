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
        get() = prefs.getString(KEY_URL, DEFAULT_URL) ?: ""
        set(value) { prefs.edit().putString(KEY_URL, value).apply() }

    /**
     * One-shot migration: on 2026-07-14 the backend moved from Render
     * (portal.eleganskyboda.com — now suspended) to Contabo VPS
     * (processor.eleganskyboda.com). Any phone that was already using
     * the old URL would keep POSTing into a suspended service after
     * upgrading. This flips the stored URL to the new host EXACTLY ONCE
     * so users don't need to re-enter it manually.
     */
    fun migrateLegacyPortalUrl() {
        val current = prefs.getString(KEY_URL, null) ?: return
        if (current.trim().trimEnd('/').equals("https://portal.eleganskyboda.com", ignoreCase = true)) {
            prefs.edit().putString(KEY_URL, DEFAULT_URL).apply()
        }
    }

    var token: String
        get() = prefs.getString(KEY_TOKEN, "") ?: ""
        set(value) { prefs.edit().putString(KEY_TOKEN, value).apply() }

    /**
     * Comma-separated list of sender IDs to accept — everything else is
     * ignored. Empty (default) = accept every SMS, because customer-sent
     * payment confirmations come from arbitrary customer phone numbers,
     * not from a fixed bank shortcode.
     */
    var senderWhitelist: String
        get() = prefs.getString(KEY_SENDERS, "") ?: ""
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
        const val DEFAULT_URL = "https://processor.eleganskyboda.com"
    }
}

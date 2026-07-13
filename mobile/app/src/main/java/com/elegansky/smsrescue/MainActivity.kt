package com.elegansky.smsrescue

import android.Manifest
import android.app.role.RoleManager
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.provider.Telephony
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.lifecycle.lifecycleScope
import com.elegansky.smsrescue.databinding.ActivityMainBinding
import com.elegansky.smsrescue.db.QueueDb
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private lateinit var settings: SettingsRepo

    private val permissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { /* refresh happens in onResume */ }

    private val defaultSmsLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) { refreshStatus() }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)
        settings = SettingsRepo(this)

        binding.serverUrl.setText(settings.serverUrl)
        binding.token.setText(settings.token)
        binding.senders.setText(settings.senderWhitelist)

        binding.saveBtn.setOnClickListener {
            settings.serverUrl = binding.serverUrl.text.toString().trim()
            settings.token     = binding.token.text.toString().trim()
            settings.senderWhitelist = binding.senders.text.toString().trim()
            Toast.makeText(this, "saved", Toast.LENGTH_SHORT).show()
            SmsWorker.enqueueDrain(this)
            refreshStatus()
        }

        binding.grantPermsBtn.setOnClickListener { requestPerms() }
        binding.becomeDefaultBtn.setOnClickListener { becomeDefaultSmsApp() }
        binding.drainNowBtn.setOnClickListener {
            SmsWorker.enqueueDrain(this)
            Toast.makeText(this, "drain enqueued", Toast.LENGTH_SHORT).show()
        }
        binding.importInboxBtn.setOnClickListener {
            lifecycleScope.launch {
                val n = withContext(Dispatchers.IO) { InboxImporter.importSinceLastSaturday(this@MainActivity) }
                Toast.makeText(this@MainActivity,
                    "imported $n messages since last Saturday",
                    Toast.LENGTH_LONG).show()
                refreshStatus()
            }
        }
    }

    override fun onResume() {
        super.onResume()
        refreshStatus()
    }

    private fun refreshStatus() {
        binding.permsStatus.text = if (hasAllPerms()) "✓ SMS permissions granted"
                                   else "✗ SMS permissions missing"
        binding.defaultSmsStatus.text = if (isDefaultSmsApp()) "✓ default SMS app (can delete)"
                                        else "✗ not default — SMS won't be deleted"
        lifecycleScope.launch {
            val (pending, rescued, flagged) = withContext(Dispatchers.IO) {
                val dao = QueueDb.get(this@MainActivity).dao()
                Triple(dao.pendingCount(), dao.rescuedCount(), dao.flaggedCount())
            }
            binding.stats.text = "queued: $pending  ·  rescued: $rescued  ·  flagged: $flagged"
        }
    }

    private fun hasAllPerms(): Boolean {
        val need = listOf(
            Manifest.permission.RECEIVE_SMS,
            Manifest.permission.READ_SMS,
            Manifest.permission.SEND_SMS,
            Manifest.permission.READ_PHONE_STATE,
        )
        return need.all { ContextCompat.checkSelfPermission(this, it) == PackageManager.PERMISSION_GRANTED }
    }

    private fun requestPerms() {
        val need = mutableListOf(
            Manifest.permission.RECEIVE_SMS,
            Manifest.permission.READ_SMS,
            Manifest.permission.SEND_SMS,
            Manifest.permission.READ_PHONE_STATE,
        )
        if (Build.VERSION.SDK_INT >= 33) need += Manifest.permission.POST_NOTIFICATIONS
        permissionLauncher.launch(need.toTypedArray())
    }

    private fun isDefaultSmsApp(): Boolean =
        Telephony.Sms.getDefaultSmsPackage(this) == packageName

    private fun becomeDefaultSmsApp() {
        if (Build.VERSION.SDK_INT >= 29) {
            val rm = getSystemService(RoleManager::class.java)
            if (rm != null && rm.isRoleAvailable(RoleManager.ROLE_SMS) && !rm.isRoleHeld(RoleManager.ROLE_SMS)) {
                defaultSmsLauncher.launch(rm.createRequestRoleIntent(RoleManager.ROLE_SMS))
                return
            }
        }
        val intent = Intent(Telephony.Sms.Intents.ACTION_CHANGE_DEFAULT)
        intent.putExtra(Telephony.Sms.Intents.EXTRA_PACKAGE_NAME, packageName)
        defaultSmsLauncher.launch(intent)
    }
}

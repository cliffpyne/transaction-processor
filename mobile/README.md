# SMS Rescue — Android companion for `/api/sms-rescue`

Sits on a dedicated Android phone that receives the bank's SMS notifications, forwards each one to the portal's `/api/sms-rescue` endpoint verbatim, and — on a green response — deletes the SMS from the inbox so the phone never fills up.

## What the phone does

```
Incoming SMS
  → SmsReceiver filters by sender whitelist (NMB, CRDB, …)
  → adds row to Room queue
  → SmsWorker (WorkManager) POSTs {message} to /api/sms-rescue
  → response ∈ {200, 409} → delete SMS + mark row terminal
  → response ∈ {400, 404} → flag row for review, keep SMS
  → 5xx or network error → exponential backoff retry
```

The **server** does all the plate + ref extraction and the actual database work. The phone only forwards raw SMS bytes, so battery + storage stay minimal.

## First-time setup

1. Open the app.
2. Enter:
   - **Server URL**: `https://portal.eleganskyboda.com`
   - **Migration token**: same token used for `/admin/*` endpoints
   - **Sender whitelist**: comma-separated substrings, default `NMB,CRDB,NMBBANK,CRDBBANK`
3. **Save settings**.
4. **Grant SMS permissions** — the system prompt lists RECEIVE_SMS, READ_SMS, SEND_SMS, READ_PHONE_STATE.
5. **Set as default SMS app** — required by Android for the app to be allowed to *delete* SMS from the inbox. Only the default SMS handler can. This app doesn't send SMS or open a chat UI; it's purely a background forwarder.

## Building

Requires Android Studio Iguana (2023.2) or newer, or the command-line Gradle wrapper.

```bash
cd mobile
./gradlew assembleRelease
# APK lands in app/build/outputs/apk/release/app-release-unsigned.apk
```

Sign it with your own keystore before installing on the phone.

## Response codes the phone acts on

| HTTP | Terminal | Action |
|---|---|---|
| 200 | `RESCUED` | Delete SMS. Row is now in `BODAILIYOPATA` / `IPHONEILIYOPATA`. |
| 409 already_rescued | `ALREADY` | Delete SMS. Idempotent — a second forward is a no-op. |
| 409 not_a_failed_row | `NOT_FAILED` | Delete SMS. Ref was already PASSED. |
| 400 | `EXTRACT_FAILED` | Keep SMS, mark row flagged. Server couldn't find a plate or ref in the body. |
| 404 plate_not_in_records | `PLATE_UNKNOWN` | Keep SMS, mark row flagged. Plate not in pikipiki records. |
| 404 ref_not_found | (retry up to 5×, then `REF_NOT_FOUND`) | Bank may just be slow to write. Retry with backoff before giving up. |
| 5xx / network | pending | Retry with exponential backoff via WorkManager. |

Flagged rows stay in the queue with `terminal != ''` so you can eyeball them from the app's stats or query the local Room DB directly.

## Notes

- **Battery**: SMS receiver spends <1 s on-CPU per message. Everything else runs under WorkManager which coalesces + defers under Doze.
- **Storage on phone**: with `WRITE_TO_SUPABASE` and rescue-delete, the SMS inbox stays under a few hundred rows at any time — solves the 1,500/day nightmare.
- **Encryption at rest**: `EncryptedSharedPreferences` (AES-256-GCM) for the URL + token.
- **Rotating the token**: change it in the app's Settings and hit *Save*; queued rows will use the new token on their next attempt.

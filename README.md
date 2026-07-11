# Telegram Contact Automator v2

Checks a list of phone numbers against Telegram (via the same API call Telegram's
own app uses when you tap a phone-number link in a message), then sends a
templated message to the ones found. Supabase is the single source of truth -
there is no local CSV, no git-committed state, no index-based resume.

## Why v2 exists

The original version tracked progress as a row-position index into a Google
Sheet and committed a CSV back to git after every run. That broke in a few
ways: the index could drift out of sync if the sheet's row order changed,
concurrent checker/sender runs could race on `git push`, and a batch import
approach silently dropped large batches instead of actually checking them.

v2 replaces all of that with one Postgres table. "Pending" numbers are pulled
by status, not position, so there is nothing to desync. Updates are atomic
DB writes, so there's no git race. Numbers are checked one at a time via
`ResolvePhoneRequest` (the same call behind Telegram's tap-to-check UI),
not bulk-imported, so nothing gets silently dropped.

## Architecture

```
Google Sheet --(seed, once)--> Supabase.phone_records --check--> found/not_found
                                        |
                                        +--send--> is_messaged
```

## Setup

1. **Supabase table** - already created (`phone_records`), RLS enabled,
   locked to `service_role` only.
2. **GitHub Secrets** - add these under Settings -> Secrets and variables -> Actions:

   | Secret | Used by | Notes |
   |---|---|---|
   | `CHECKER_API_ID`, `CHECKER_API_HASH`, `CONTACT_CHECKER_SESSION` | check | Telegram API creds for the checking account |
   | `SENDER_API_ID`, `SENDER_API_HASH`, `MESSAGE_SENDER_SESSION` | send | Telegram API creds for the messaging account (keep separate from checker) |
   | `SUPABASE_URL` | check, send, seed | Project Settings -> Data API -> Project URL |
   | `SUPABASE_SERVICE_ROLE_KEY` | check, send, seed | Project Settings -> API Keys -> `service_role` (secret key, not anon/publishable) |
   | `SHEET_CSV_URL`, `PHONE_COLUMN_INDEX` | seed only | Same sheet as before |
   | `BATCH_SIZE`, `CHECK_DELAY_SECONDS`, `CHECKPOINT_INTERVAL`, `MAX_RUNTIME_SECONDS` | check | Optional, see `.env.example` for defaults |
   | `MESSAGE_TEMPLATE`, `MESSAGE_BATCH_SIZE`, `MIN_ACTIVITY_LEVEL` | send | Optional, see `.env.example` for defaults |

3. **Run "Seed Database From Sheet" once** (Actions tab -> workflow_dispatch).
   Safe to re-run any time - numbers already in Supabase are skipped.
4. Checker and Sender then run on their normal schedules (daily / hourly).

## Modes

- `python main.py --mode seed` - one-time sheet -> Supabase import
- `python main.py --mode check` - resolve a batch of pending numbers
- `python main.py --mode send` - message found + unmessaged users

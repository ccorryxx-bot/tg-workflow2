"""
Telegram Contact Automator v2 - Supabase-backed.

Key difference from v1: there is no local CSV, no progress.log, no git-committed
state. Supabase's `phone_records` table is the single source of truth. This kills
the whole class of bugs v1 had (index drifting out of sync with the sheet's row
order, git push races between checker/sender, "already processed" being wrong).

Modes:
  --mode seed   One-time (re-run-safe) import of numbers from the Google Sheet
                into Supabase as 'pending'. Only this mode touches the sheet.
  --mode check  Pulls 'pending' rows from Supabase, resolves them via Telegram's
                tap-to-check API, writes results back as 'found' / 'not_found'.
  --mode send   Pulls 'found' + not-yet-messaged rows, sends the template message,
                marks them messaged.
"""

import asyncio
import csv
import logging
import os
import random
import time
import argparse
from datetime import datetime, timezone
from typing import Dict, List

import requests
from supabase import create_client, Client
from telethon import TelegramClient
from telethon.errors import FloodWaitError, PhoneNotOccupiedError
from telethon.tl.functions.contacts import ResolvePhoneRequest

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class Config:
    # Contact Checker credentials
    CHECKER_API_ID = int(os.getenv('CHECKER_API_ID', '0'))
    CHECKER_API_HASH = os.getenv('CHECKER_API_HASH', '')
    CONTACT_CHECKER_SESSION = os.getenv('CONTACT_CHECKER_SESSION', '')

    # Message Sender credentials
    SENDER_API_ID = int(os.getenv('SENDER_API_ID', '0'))
    SENDER_API_HASH = os.getenv('SENDER_API_HASH', '')
    MESSAGE_SENDER_SESSION = os.getenv('MESSAGE_SENDER_SESSION', '')

    # Supabase
    SUPABASE_URL = os.getenv('SUPABASE_URL', '')
    SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')

    # Only read during --mode seed. Not needed for check/send.
    SHEET_CSV_URL = os.getenv('SHEET_CSV_URL', '')
    PHONE_COLUMN_INDEX = os.getenv('PHONE_COLUMN_INDEX', '0')

    # How many numbers one `check` run is willing to attempt. Kept generous by
    # default because MAX_RUNTIME_SECONDS is the real limiter now, not this count.
    CHECK_BATCH_SIZE = int(os.getenv('BATCH_SIZE', os.getenv('CHECK_BATCH_SIZE', '50000')))
    # Delay between individual ResolvePhoneRequest lookups (seconds) - this is what
    # keeps the account looking like normal manual tap-checking, not a scraper.
    CHECK_DELAY_SECONDS = float(os.getenv('CHECK_DELAY_SECONDS', '2'))
    # Pull + write results back to Supabase every N lookups, so a job timeout or
    # crash mid-run only loses at most this many lookups, never the whole run.
    CHECKPOINT_INTERVAL = int(os.getenv('CHECKPOINT_INTERVAL', '100'))
    # Hard wall-clock cap per run, safely under GitHub Actions' 360-minute default
    # job timeout, so a huge BATCH_SIZE can never get killed with nothing saved.
    MAX_RUNTIME_SECONDS = int(os.getenv('MAX_RUNTIME_SECONDS', str(5 * 3600)))

    MESSAGE_TEMPLATE = os.getenv('MESSAGE_TEMPLATE', 'Hello {first_name}!')
    MESSAGE_BATCH_SIZE = int(os.getenv('MESSAGE_BATCH_SIZE', '1'))
    MESSAGE_DELAY_MIN = int(os.getenv('MESSAGE_DELAY_MIN', '10'))
    MESSAGE_DELAY_MAX = int(os.getenv('MESSAGE_DELAY_MAX', '30'))

    FLOOD_SLEEP_THRESHOLD = int(os.getenv('FLOOD_SLEEP_THRESHOLD', '60'))
    MIN_ACTIVITY_LEVEL = os.getenv('MIN_ACTIVITY_LEVEL', 'UserStatusRecently')


ACTIVITY_LEVELS = {
    "UserStatusOnline": 4, "UserStatusRecently": 3, "UserStatusLastWeek": 2,
    "UserStatusLastMonth": 1, "UserStatusEmpty": 0, "UserStatusOffline": 0,
}


def get_supabase() -> Client:
    if not Config.SUPABASE_URL or not Config.SUPABASE_SERVICE_ROLE_KEY:
        raise ValueError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")
    # service_role bypasses RLS - required since the table blocks anon access.
    return create_client(Config.SUPABASE_URL, Config.SUPABASE_SERVICE_ROLE_KEY)


def read_phone_numbers_from_sheet(sheet_csv_url: str, phone_column_indices_str: str = "0") -> List[str]:
    """Read + normalize phone numbers from a public Google Sheet CSV export."""
    try:
        indices = [int(i.strip()) for i in phone_column_indices_str.split(',') if i.strip().isdigit()]
        if not indices:
            indices = [0]

        response = requests.get(sheet_csv_url, timeout=60)
        response.raise_for_status()

        phone_numbers = []
        reader = csv.reader(response.text.splitlines())
        for row_index, row in enumerate(reader):
            if not row:
                continue
            for col_idx in indices:
                if len(row) <= col_idx:
                    continue
                value = row[col_idx].strip()
                if not value:
                    continue
                if row_index == 0 and not value.startswith('+') and not value.isdigit():
                    continue  # header row
                if not value.startswith('+'):
                    value = '+' + value
                phone_numbers.append(value)

        unique_numbers = []
        seen = set()
        for num in phone_numbers:
            if num not in seen:
                unique_numbers.append(num)
                seen.add(num)
        return unique_numbers
    except Exception as e:
        logger.error(f"Error reading sheet: {e}")
        return []


def run_seed():
    """Import every number from the sheet into Supabase as 'pending'.
    Safe to re-run any time: ON CONFLICT (phone) skips numbers already present,
    whatever their current status, so it never overwrites real progress."""
    numbers = read_phone_numbers_from_sheet(Config.SHEET_CSV_URL, Config.PHONE_COLUMN_INDEX)
    if not numbers:
        logger.error("No numbers read from sheet - check SHEET_CSV_URL / PHONE_COLUMN_INDEX.")
        return

    supabase = get_supabase()
    chunk_size = 1000
    for i in range(0, len(numbers), chunk_size):
        chunk = numbers[i:i + chunk_size]
        rows = [{"phone": p, "status": "pending"} for p in chunk]
        supabase.table("phone_records").upsert(
            rows, on_conflict="phone", ignore_duplicates=True
        ).execute()
        logger.info(f"Seeded {min(i + chunk_size, len(numbers))}/{len(numbers)}")

    logger.info(f"Seed complete: {len(numbers)} numbers from sheet processed.")


class TelegramContactChecker:
    def __init__(self, mode: str):
        from telethon.sessions import StringSession

        if mode == 'check':
            session_str = Config.CONTACT_CHECKER_SESSION
            api_id = Config.CHECKER_API_ID
            api_hash = Config.CHECKER_API_HASH
        elif mode == 'send':
            session_str = Config.MESSAGE_SENDER_SESSION
            api_id = Config.SENDER_API_ID
            api_hash = Config.SENDER_API_HASH
        else:
            raise ValueError(f"Unknown mode: {mode}")

        if not session_str or not api_id or not api_hash:
            raise ValueError(f"Missing Telegram credentials for mode: {mode}")

        self.client = TelegramClient(
            StringSession(session_str), api_id, api_hash,
            flood_sleep_threshold=Config.FLOOD_SLEEP_THRESHOLD
        )
        self.supabase = get_supabase()

    async def run_checker(self):
        await self.client.connect()
        if not await self.client.is_user_authorized():
            logger.error("Unauthorized")
            return

        run_start = time.monotonic()
        total_checked = 0
        total_found = 0
        stopped_early = False

        while total_checked < Config.CHECK_BATCH_SIZE:
            take = min(Config.CHECKPOINT_INTERVAL, Config.CHECK_BATCH_SIZE - total_checked)
            resp = (
                self.supabase.table("phone_records")
                .select("id, phone")
                .eq("status", "pending")
                .limit(take)
                .execute()
            )
            rows = resp.data
            if not rows:
                logger.info("No pending numbers left - full list has been checked.")
                break

            found_updates = []
            notfound_ids = []

            for row in rows:
                phone = row["phone"]
                try:
                    # Same MTProto call Telegram's own app makes when you tap a
                    # phone-number link inside a message. Respects the "who can
                    # find me by phone number" setting per-number, same as the
                    # manual tap-test.
                    result = await self.client(ResolvePhoneRequest(phone))
                    if result.users:
                        user = result.users[0]
                        found_updates.append({
                            "id": row["id"],
                            "status": "found",
                            "user_id": user.id,
                            "username": getattr(user, "username", None),
                            "first_name": getattr(user, "first_name", None),
                            "last_name": getattr(user, "last_name", None),
                            "last_seen_status": str(type(user.status).__name__) if user.status else "UserStatusEmpty",
                            "checked_at": datetime.now(timezone.utc).isoformat(),
                        })
                        total_found += 1
                        logger.info(f"Found: {phone}")
                    else:
                        notfound_ids.append(row["id"])
                except PhoneNotOccupiedError:
                    # Confirmed not on Telegram - same result as the manual
                    # "This number is not on Telegram" tap-test.
                    notfound_ids.append(row["id"])
                except FloodWaitError as e:
                    logger.warning(f"Flood wait: {e.seconds}s on {phone} - leaving pending, will retry next run.")
                    await asyncio.sleep(e.seconds)
                    continue
                except Exception as e:
                    # Unexpected/transient error - leave status as 'pending' so it
                    # gets retried later instead of being permanently mislabeled.
                    logger.error(f"Resolve error on {phone}: {e}")
                    continue

                total_checked += 1
                await asyncio.sleep(Config.CHECK_DELAY_SECONDS)

                if (time.monotonic() - run_start) > Config.MAX_RUNTIME_SECONDS:
                    stopped_early = True
                    break

            # Checkpoint: one batched write for found rows, one for not-found rows.
            if found_updates:
                self.supabase.table("phone_records").upsert(found_updates, on_conflict="id").execute()
            if notfound_ids:
                self.supabase.table("phone_records").update({
                    "status": "not_found",
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                }).in_("id", notfound_ids).execute()

            if stopped_early:
                logger.warning(f"Runtime budget reached after checking {total_checked} numbers this run.")
                break

        logger.info(f"Run complete: checked {total_checked}, found {total_found}.")
        await self.client.disconnect()

    async def run_sender(self):
        await self.client.connect()
        if not await self.client.is_user_authorized():
            logger.error("Unauthorized")
            return

        min_val = ACTIVITY_LEVELS.get(Config.MIN_ACTIVITY_LEVEL, 0)
        eligible_statuses = [s for s, v in ACTIVITY_LEVELS.items() if v >= min_val]

        resp = (
            self.supabase.table("phone_records")
            .select("id, phone, user_id, username, first_name, last_seen_status")
            .eq("status", "found")
            .eq("is_messaged", False)
            .in_("last_seen_status", eligible_statuses)
            .limit(Config.MESSAGE_BATCH_SIZE)
            .execute()
        )
        batch = resp.data
        if not batch:
            logger.info("No eligible users to message.")
            await self.client.disconnect()
            return

        sent_count = 0
        for user in batch:
            try:
                text = Config.MESSAGE_TEMPLATE.format(
                    username=user.get("username") or "",
                    first_name=user.get("first_name") or "",
                )
                await self.client.send_message(int(user["user_id"]), text)

                # The `.eq("is_messaged", False)` guard makes this an atomic
                # check-and-set: if two sender runs ever overlapped, only one
                # of them can win this update, so nobody gets double-messaged.
                self.supabase.table("phone_records").update({
                    "is_messaged": True,
                    "messaged_at": datetime.now(timezone.utc).isoformat(),
                }).eq("id", user["id"]).eq("is_messaged", False).execute()

                sent_count += 1
                logger.info(f"Sent to {user['phone']}")
                if len(batch) > 1:
                    await asyncio.sleep(random.randint(Config.MESSAGE_DELAY_MIN, Config.MESSAGE_DELAY_MAX))
            except Exception as e:
                logger.error(f"Failed to send to {user['phone']}: {e}")

        logger.info(f"Attempted {len(batch)} messages, sent {sent_count}.")
        await self.client.disconnect()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['check', 'send', 'seed'], required=True)
    args = parser.parse_args()

    if args.mode == 'seed':
        run_seed()
    else:
        checker = TelegramContactChecker(mode=args.mode)
        if args.mode == 'check':
            asyncio.run(checker.run_checker())
        else:
            asyncio.run(checker.run_sender())

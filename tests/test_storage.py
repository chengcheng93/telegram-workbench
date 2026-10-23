import importlib
from contextlib import closing
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile
import unittest
from unittest.mock import patch


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.storage = importlib.import_module("storage")
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "private"
        self.store = self.storage.Store(self.root)
        self.addCleanup(self.store.close)

    def test_json_values_survive_close_and_reopen(self):
        # Dropping the commit or serializing values as strings breaks this test.
        values = {
            "object": {"name": "你好", "nested": [1, None, True]},
            "list": ["a", 2],
            "string": "内容",
            "integer": 42,
            "boolean": False,
            "null": None,
        }
        for key, value in values.items():
            self.store.put("values", key, value)
        self.store.close()
        reopened = self.storage.Store(self.root)
        self.addCleanup(reopened.close)
        for key, value in values.items():
            with self.subTest(key=key):
                self.assertEqual(reopened.get("values", key), value)
        self.assertEqual(reopened.get("values", "absent", "fallback"), "fallback")
        self.assertIsNone(reopened.get("values", "null", "fallback"))

    def test_existing_root_and_database_are_made_private(self):
        # Failing to tighten existing paths exposes stored session/settings data.
        self.store.close()
        self.root.chmod(0o755)
        database = self.root / "workbench.sqlite3"
        database.chmod(0o644)
        reopened = self.storage.Store(self.root)
        self.addCleanup(reopened.close)
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(database.stat().st_mode), 0o600)

    def test_new_root_and_database_ignore_permissive_umask(self):
        # Default mkdir/connect modes must not leave a new database world-readable.
        root = self.root / "new"
        prior_umask = os.umask(0)
        try:
            created = self.storage.Store(root)
        finally:
            os.umask(prior_umask)
        self.addCleanup(created.close)
        self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((root / "workbench.sqlite3").stat().st_mode), 0o600)

    def test_same_record_key_is_isolated_by_account_and_kind(self):
        # Omitting either scope column from queries/upserts leaks another account.
        self.store.put("drafts", "same", {"text": "global"})
        self.store.put("drafts", "same", {"text": "A"}, account_id="a")
        self.store.put("drafts", "same", {"text": "B"}, account_id="b")
        self.store.put("messages", "same", {"text": "message"}, account_id="a")
        self.store.put("drafts", "same", {"text": "A updated"}, account_id="a")
        self.assertEqual(self.store.get("drafts", "same"), {"text": "global"})
        self.assertEqual(self.store.get("drafts", "same", account_id="a"), {"text": "A updated"})
        self.assertEqual(self.store.get("drafts", "same", account_id="b"), {"text": "B"})
        self.assertEqual(self.store.get("messages", "same", account_id="a"), {"text": "message"})

    def test_rejected_json_write_preserves_previous_committed_value(self):
        # A serialization failure must not erase the committed row or poison writes.
        self.store.put("drafts", "one", {"text": "saved"}, account_id="a")
        with self.assertRaises((TypeError, ValueError)):
            self.store.put("drafts", "one", {"invalid": object()}, account_id="a")
        self.assertEqual(self.store.get("drafts", "one", account_id="a"), {"text": "saved"})
        with self.assertRaises(ValueError):
            self.store.put("drafts", "one", {"invalid": float("nan")}, account_id="a")
        self.store.put("drafts", "two", {"text": "next"}, account_id="a")
        reader = self.storage.Store(self.root)
        self.addCleanup(reader.close)
        self.assertEqual(reader.get("drafts", "one", account_id="a"), {"text": "saved"})
        self.assertEqual(reader.get("drafts", "two", account_id="a"), {"text": "next"})

    def test_sql_failure_does_not_leave_a_partial_transaction(self):
        # A failed write must roll back so the next committed write is independent.
        database = self.root / "workbench.sqlite3"
        with closing(sqlite3.connect(database)) as connection:
            with connection:
                connection.execute(
                    "CREATE TRIGGER reject_bad BEFORE INSERT ON records "
                    "WHEN NEW.key = 'bad' BEGIN SELECT RAISE(ABORT, 'rejected'); END"
                )
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.put("drafts", "bad", "rejected")
        self.store.put("drafts", "good", "saved")
        reader = self.storage.Store(self.root)
        self.addCleanup(reader.close)
        self.assertEqual(reader.get("drafts", "good"), "saved")
        self.assertIsNone(reader.get("drafts", "bad"))

    def test_list_filters_scope_and_sorts_by_key(self):
        # Sorting insertion order or treating empty scope as all mixes account data.
        self.store.put("drafts", "z", "a-z", account_id="a")
        self.store.put("drafts", "b", "b-b", account_id="b")
        self.store.put("drafts", "a", "a-a", account_id="a")
        self.store.put("drafts", "c", "app-c")
        self.store.put("messages", "a", "excluded", account_id="a")
        self.assertEqual(self.store.list("drafts", "a"), ["a-a", "a-z"])
        self.assertEqual(self.store.list("drafts", ""), ["app-c"])
        self.assertEqual(self.store.list("drafts"), ["a-a", "b-b", "app-c", "a-z"])
        self.assertEqual(self.store.list("absent"), [])

    def test_delete_only_removes_requested_scope_and_persists(self):
        # Deleting by kind/key alone would remove the other account's draft.
        self.store.put("drafts", "same", "global")
        self.store.put("drafts", "same", "A", account_id="a")
        self.store.put("drafts", "same", "B", account_id="b")
        self.store.delete("drafts", "same", account_id="a")
        self.store.delete("drafts", "missing", account_id="a")
        reader = self.storage.Store(self.root)
        self.addCleanup(reader.close)
        self.assertIsNone(reader.get("drafts", "same", account_id="a"))
        self.assertEqual(reader.get("drafts", "same", account_id="b"), "B")
        self.assertEqual(reader.get("drafts", "same"), "global")

    def test_settings_use_app_scope_and_preserve_false_and_null(self):
        # A truthiness default would replace saved false/null values.
        self.store.put("settings", "enabled", True, account_id="a")
        self.store.set_setting("enabled", False)
        self.store.set_setting("optional", None)
        self.assertIs(self.store.setting("enabled", True), False)
        self.assertIsNone(self.store.setting("optional", "default"))
        self.assertEqual(self.store.setting("missing", "default"), "default")
        self.assertIs(self.store.get("settings", "enabled", account_id="a"), True)

    def test_event_history_is_latest_first_and_account_filtered(self):
        # UUID key order is unrelated to event time; limits apply after filtering.
        with patch.object(self.storage, "now", side_effect=[
            "2026-01-01T00:00:03+00:00",
            "2026-01-01T00:00:01+00:00",
            "2026-01-01T00:00:02+00:00",
        ]):
            newest = self.store.event("a", "connected", "已连接", count=2)
            oldest = self.store.event("a", "paused", "已暂停")
            middle = self.store.event("b", "synced", "同步完成")
        self.assertEqual(newest["account_id"], "a")
        self.assertEqual(newest["action"], "connected")
        self.assertEqual(newest["detail"], "已连接")
        self.assertEqual(newest["count"], 2)
        self.assertEqual(self.store.events(), [newest, middle, oldest])
        self.assertEqual(self.store.events("a", limit=1), [newest])
        self.assertEqual(self.store.events("a", limit=0), [])
        self.assertEqual(self.store.events("missing"), [])
        self.assertEqual(self.store.get("events", newest["id"], account_id="a"), newest)
        reader = self.storage.Store(self.root)
        self.addCleanup(reader.close)
        self.assertEqual(reader.events("b"), [middle])

    def test_same_time_events_keep_newest_submission_first(self):
        # Equal timestamps must not make random UUID ordering reorder the activity log.
        with patch.object(self.storage, "now", return_value="2026-01-01T00:00:00+00:00"):
            first = self.store.event("a", "first", "先")
            second = self.store.event("a", "second", "后")
        self.assertEqual(self.store.events("a"), [second, first])

    def test_generated_identifiers_and_timestamp_are_safe_for_persistence(self):
        # Colliding/nonopaque IDs overwrite events; naive timestamps break comparisons.
        identifiers = [self.storage.new_id() for _ in range(100)]
        self.assertEqual(len(set(identifiers)), 100)
        self.assertTrue(all(re.fullmatch(r"[a-f0-9]{32}", key) for key in identifiers))
        before = datetime.now(timezone.utc)
        timestamp = datetime.fromisoformat(self.storage.now())
        after = datetime.now(timezone.utc)
        self.assertEqual(timestamp.utcoffset().total_seconds(), 0)
        self.assertLessEqual(before, timestamp)
        self.assertLessEqual(timestamp, after)

    def test_backup_captures_wal_records_as_a_stable_sqlite_snapshot(self):
        # Copying the main file alone loses committed records in the WAL.
        with closing(sqlite3.connect(self.root / "workbench.sqlite3")) as connection:
            self.assertEqual(connection.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
        self.store.put("drafts", "one", {"text": "快照内容"}, account_id="a")
        self.store.set_setting("private", {"key": "test-only"})
        self.assertGreater((self.root / "workbench.sqlite3-wal").stat().st_size, 0)
        destination = self.root / "snapshot" / "workbench.sqlite3"
        self.assertIsNone(self.store.backup_to(destination))
        self.store.put("drafts", "one", {"text": "后续内容"}, account_id="a")
        copied = self.storage.Store(destination.parent)
        self.addCleanup(copied.close)
        self.assertEqual(copied.get("drafts", "one", account_id="a"), {"text": "快照内容"})
        self.assertEqual(copied.setting("private"), {"key": "test-only"})
        self.assertEqual(self.store.get("drafts", "one", account_id="a"), {"text": "后续内容"})
        with closing(sqlite3.connect(destination)) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_backup_creates_private_file_and_all_new_parent_directories(self):
        # A permissive umask must not expose the backup or any new parent directory.
        destination = self.root.parent / "exports" / "nested" / "backup.sqlite3"
        previous = os.umask(0)
        try:
            self.store.backup_to(destination)
        finally:
            os.umask(previous)
        self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(destination.parent.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(destination.parent.parent.stat().st_mode), 0o700)

    def test_backup_tightens_existing_destination_without_changing_existing_parent(self):
        # Updating an old backup must tighten its permissions, not its shared parent.
        existing = self.root.parent / "existing"
        existing.mkdir(mode=0o755)
        existing.chmod(0o755)
        destination = existing / "backup.sqlite3"
        destination.touch(mode=0o644)
        destination.chmod(0o644)
        self.store.put("messages", "one", "saved", account_id="a")
        self.store.backup_to(destination)
        self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(existing.stat().st_mode), 0o755)
        with closing(sqlite3.connect(destination)) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM records").fetchone()[0], 1)

    def test_backup_refuses_source_paths_symlinks_and_hardlinks(self):
        # Aliases of the source must never be opened as writable backup destinations.
        source = self.root / "workbench.sqlite3"
        symbolic = self.root / "symbolic.sqlite3"
        symbolic.symlink_to(source)
        hardlink = self.root / "hardlink.sqlite3"
        os.link(source, hardlink)
        self.store.put("drafts", "one", "preserved", account_id="a")
        for destination in (source, symbolic, hardlink):
            with self.subTest(destination=destination.name):
                with self.assertRaisesRegex(ValueError, "[\u4e00-\u9fff]"):
                    self.store.backup_to(destination)
        self.assertEqual(self.store.get("drafts", "one", account_id="a"), "preserved")
        self.store.put("drafts", "two", "still usable", account_id="a")

    def test_failed_backup_preserves_existing_destination_and_removes_temporary_file(self):
        # A backup failure must not destroy the previous copy or leave private debris.
        destination = self.root / "snapshot" / "backup.sqlite3"
        destination.parent.mkdir(mode=0o700)
        destination.write_bytes(b"previous backup")
        self.store.close()
        with self.assertRaises(sqlite3.ProgrammingError):
            self.store.backup_to(destination)
        self.assertEqual(destination.read_bytes(), b"previous backup")
        self.assertEqual(list(destination.parent.iterdir()), [destination])

    def test_backup_refuses_live_database_journal_paths_and_preserves_wal_data(self):
        # Replacing the active WAL destroys committed source data for new readers.
        source = self.root / "workbench.sqlite3"
        with closing(sqlite3.connect(source)) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
        self.store.put("drafts", "one", "preserved", account_id="a")
        wal_alias = self.root / "wal-alias"
        wal_alias.symlink_to(self.root / "workbench.sqlite3-wal")
        for destination in (
            self.root / "workbench.sqlite3-wal",
            self.root / "workbench.sqlite3-shm",
            self.root / "workbench.sqlite3-journal",
            wal_alias,
        ):
            with self.assertRaisesRegex(ValueError, "[\u4e00-\u9fff]"):
                self.store.backup_to(destination)
        reader = self.storage.Store(self.root)
        self.addCleanup(reader.close)
        self.assertEqual(reader.get("drafts", "one", account_id="a"), "preserved")

    def test_clear_kind_removes_all_scopes_and_preserves_other_kinds(self):
        # Cache cleanup must not erase drafts, task evidence, outbox or dedup state.
        for account in ("", "a", "b"):
            self.store.put("messages", "same", "cached", account_id=account)
        for kind in ("drafts", "outbox", "tasks", "listener_processed"):
            self.store.put(kind, "keep", "evidence", account_id="a")
        self.assertEqual(self.store.clear_kind("messages"), 3)
        self.assertEqual(self.store.clear_kind("messages"), 0)
        reader = self.storage.Store(self.root)
        self.addCleanup(reader.close)
        self.assertEqual(reader.list("messages"), [])
        for kind in ("drafts", "outbox", "tasks", "listener_processed"):
            self.assertEqual(reader.get(kind, "keep", account_id="a"), "evidence")

    def test_clear_kind_distinguishes_one_account_and_app_scope(self):
        # Treating account_id='' as all scopes deletes another account's cache.
        for account in ("", "a", "b"):
            self.store.put("dialogs", "same", account or "app", account_id=account)
        self.assertEqual(self.store.clear_kind("dialogs", account_id="a"), 1)
        self.assertEqual(self.store.list("dialogs"), ["app", "b"])
        self.assertEqual(self.store.clear_kind("dialogs", account_id=""), 1)
        self.assertEqual(self.store.list("dialogs"), ["b"])
        self.assertEqual(self.store.clear_kind("dialogs", account_id="missing"), 0)

    def test_clear_kind_rolls_back_every_deletion_if_one_row_is_rejected(self):
        # A per-record deletion loop could commit some rows before a later failure.
        self.store.put("messages", "first", "first", account_id="a")
        self.store.put("messages", "blocked", "blocked", account_id="a")
        with closing(sqlite3.connect(self.root / "workbench.sqlite3")) as connection:
            with connection:
                connection.execute(
                    "CREATE TRIGGER reject_clear BEFORE DELETE ON records "
                    "WHEN OLD.key = 'blocked' BEGIN SELECT RAISE(ABORT, 'rejected'); END"
                )
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.clear_kind("messages")
        self.assertEqual(self.store.list("messages", "a"), ["blocked", "first"])
        self.store.put("messages", "next", "next", account_id="a")
        reader = self.storage.Store(self.root)
        self.addCleanup(reader.close)
        self.assertEqual(reader.list("messages", "a"), ["blocked", "first", "next"])


if __name__ == "__main__":
    unittest.main()

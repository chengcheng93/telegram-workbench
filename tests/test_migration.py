"""Migration tests use temporary files and a read-only fake Telegram boundary."""

import asyncio
from contextlib import closing
import fcntl
import json
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from storage import Store

try:
    from migration import MigrationService
except ModuleNotFoundError:
    MigrationService = None


API_HASH = "1234567890abcdef1234567890abcdef"
PHONE = "15551234567"


def make_session(path, version=7):
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE version (version INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO version VALUES (?)", (version,))
        db.execute("CREATE TABLE sessions (dc_id INTEGER PRIMARY KEY, server_address TEXT, "
                   "port INTEGER, auth_key BLOB, takeout_id INTEGER)")
        db.execute("INSERT INTO sessions VALUES (2,'149.154.167.51',443,?,NULL)", (b"x" * 256,))
        db.execute("CREATE TABLE entities (id INTEGER PRIMARY KEY, hash INTEGER NOT NULL, "
                   "username TEXT, phone INTEGER, name TEXT, date INTEGER)")
        db.execute("CREATE TABLE sent_files (md5_digest BLOB, file_size INTEGER, type INTEGER, "
                   "id INTEGER, hash INTEGER, PRIMARY KEY(md5_digest,file_size,type))")
        db.execute("CREATE TABLE update_state (id INTEGER PRIMARY KEY, pts INTEGER, "
                   "qts INTEGER, date INTEGER, seq INTEGER)")


class FakeClient:
    def __init__(self, factory, path):
        self.factory, self.path = factory, Path(path)
        self.operations = []

    async def connect(self):
        self.operations.append("connect")
        if self.factory.fail:
            raise ConnectionError("secret " + API_HASH + " " + PHONE)

    async def is_user_authorized(self):
        self.operations.append("authorized")
        return self.factory.authorized

    async def get_me(self):
        self.operations.append("get_me")
        return self.factory.identity

    async def disconnect(self):
        self.operations.append("disconnect")
        if self.factory.pending_journal:
            self.path.with_name(self.path.name + "-wal").write_bytes(b"pending sqlite data")
        if self.factory.rewrite_on_disconnect:
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute("UPDATE update_state SET pts = pts + 1")
                db.execute("INSERT OR REPLACE INTO update_state VALUES (0,1,0,0,0)")


class ClientFactory:
    def __init__(self):
        self.clients = []
        self.authorized, self.fail, self.rewrite_on_disconnect = True, False, False
        self.pending_journal = False
        self.identity = SimpleNamespace(id=7001234, first_name="Alice", last_name="Tester",
                                        username="alice_tester", phone=PHONE, bot=False)

    def __call__(self, path, api_id, api_hash):
        if api_id != 12345 or api_hash != API_HASH:
            raise AssertionError("wrong candidate credentials")
        client = FakeClient(self, path)
        self.clients.append(client)
        return client


class FakeAccounts:
    def __init__(self, app):
        self.app = app
        self.client_factory = ClientFactory()
        self.serial = 0

    async def action(self, name, data):
        if name == "add_account":
            if len([p for p in self.app.store.list("accounts") if not p.get("deleted")]) >= 8:
                raise ValueError("最多 8 个账号。")
            self.serial += 1
            aid = f"account{self.serial}"
            directory = self.app.root / "accounts" / aid
            directory.mkdir(parents=True, mode=0o700)
            self.app.store.put("accounts", aid, {"id": aid, "name": data["name"],
                "telegram_id": None, "status": "offline", "stage": "new", "deleted": False})
            return {"account_id": aid}
        if name == "remove_account":
            profile = self.app.store.get("accounts", data["account_id"])
            profile["deleted"] = True
            self.app.store.put("accounts", data["account_id"], profile)
            return {}
        raise AssertionError("migration must not log in or mutate Telegram")


class MigrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.assertIsNotNone(MigrationService, "MigrationService must provide explicit migration")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.source = self.base / "legacy"
        self.source.mkdir()
        self.config = {"api_id": 12345, "api_hash": API_HASH,
                       "group_ids": [-1001234567, -42, -42]}
        self.write_config()
        make_session(self.source / "account.session")
        (self.source / "account.lock").write_text("historical pid\n")
        self.original = {p.name: p.read_bytes() for p in self.source.iterdir()}
        self.app = SimpleNamespace(root=self.base / "new")
        self.app.store = Store(self.app.root)
        self.addCleanup(self.app.store.close)
        self.app.accounts = FakeAccounts(self.app)
        self.app.bump = lambda: None
        self.logs = []
        self.app.log = lambda *args, **kwargs: self.logs.append((args, kwargs))
        self.service = MigrationService(self.app)

    def write_config(self):
        (self.source / "config.json").write_text(json.dumps(self.config))

    async def preview(self):
        return await self.service.action("check_legacy", {"source_path": str(self.source)})

    async def import_preview(self, preview, name="Imported"):
        return await self.service.action("import_legacy", {
            "confirmation_token": preview["confirmation_token"], "name": name})

    def active_profiles(self):
        return [p for p in self.app.store.list("accounts") if not p.get("deleted")]

    async def test_preview_copies_original_files_and_verifies_only_candidate_identity(self):
        self.assertFalse((self.app.root / "migrations").exists())
        preview = await self.preview()
        self.assertTrue(preview["can_restore"])
        self.assertTrue(preview["can_import"])
        self.assertEqual(preview["group_ids"], [-1001234567, -42])
        self.assertEqual(preview["source_path"], str(self.source.resolve()))
        self.assertIn("运行统计", " ".join(preview["non_migratable_fields"]))
        self.assertIn("去重", " ".join(preview["non_migratable_fields"]))
        self.assertIn("停止", preview["listener_after_import"])
        client = self.app.accounts.client_factory.clients[0]
        self.assertNotEqual(client.path, self.source / "account.session")
        self.assertEqual(client.operations, ["connect", "authorized", "get_me", "disconnect"])
        backup = Path(preview["backup_path"])
        for name, original in self.original.items():
            self.assertEqual((self.source / name).read_bytes(), original)
            self.assertEqual((backup / name).read_bytes(), original)
        self.assertEqual(self.active_profiles(), [])
        public = json.dumps(preview)
        for secret in (API_HASH, PHONE, "x" * 256):
            self.assertNotIn(secret, public)
        for path in (self.app.root / "migrations").rglob("*"):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700 if path.is_dir() else 0o600)

    async def test_import_creates_new_offline_account_and_saved_preview_config_once(self):
        self.app.accounts.client_factory.rewrite_on_disconnect = True
        preview = await self.preview()
        result = await self.import_preview(preview)
        aid = result["account_id"]
        self.assertEqual(self.app.store.get("accounts", aid)["telegram_id"], 7001234)
        self.assertEqual(self.app.store.get("accounts", aid)["status"], "offline")
        config = self.app.store.get("listener_config", "current", account_id=aid)
        self.assertEqual(config["group_ids"], [-1001234567, -42])
        self.assertEqual((config["version"], config["mode"]), (1, "preview"))
        self.assertIsNone(self.app.store.get("listener_run", "current", account_id=aid))
        self.assertEqual(self.app.store.setting("telegram_api"),
                         {"api_id": 12345, "api_hash": API_HASH})
        self.assertTrue((self.app.root / "accounts" / aid / "account.session").is_file())
        self.assertEqual(len(self.app.accounts.client_factory.clients), 1)
        with self.assertRaisesRegex(ValueError, "确认|失效"):
            await self.import_preview(preview)
        self.assertEqual(len(self.active_profiles()), 1)

    async def test_busy_source_is_rejected_before_any_copy_or_client(self):
        with (self.source / "account.lock").open("rb") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(ValueError, "占用|运行|停止"):
                await self.preview()
        self.assertEqual(self.app.accounts.client_factory.clients, [])
        self.assertFalse((self.app.root / "migrations").exists())

    async def test_bad_config_and_non_telethon_sqlite_never_reach_client(self):
        for field, value in [("api_id", True), ("api_hash", "bad"),
                             ("group_ids", [1]), ("group_ids", [True]),
                             ("group_ids", ["-123"])]:
            with self.subTest(field=field, value=value):
                self.config[field] = value
                self.write_config()
                with self.assertRaises(ValueError):
                    await self.preview()
                self.config = {"api_id": 12345, "api_hash": API_HASH, "group_ids": [-42]}
        self.write_config()
        (self.source / "account.session").write_bytes(b"not a SQLite session")
        with self.assertRaisesRegex(ValueError, "会话|SQLite|格式"):
            await self.preview()
        self.assertEqual(self.app.accounts.client_factory.clients, [])

    async def test_confirmation_expires_and_changed_source_cannot_import(self):
        with patch("migration.time.time", return_value=1000):
            preview = await self.preview()
        with patch("migration.time.time", return_value=1300):
            with self.assertRaisesRegex(ValueError, "过期|失效"):
                await self.import_preview(preview)
        preview = await self.preview()
        self.config["group_ids"] = [-777]
        self.write_config()
        with self.assertRaisesRegex(ValueError, "来源|变化|重新"):
            await self.import_preview(preview)
        self.assertEqual(self.active_profiles(), [])

    async def test_modified_candidate_cannot_be_bound_to_checked_identity(self):
        preview = await self.preview()
        candidate = self.app.accounts.client_factory.clients[0].path
        with closing(sqlite3.connect(candidate)) as db, db:
            db.execute("UPDATE sessions SET auth_key = ?", (b"y" * 256,))
        with self.assertRaisesRegex(ValueError, "候选|变化|校验"):
            await self.import_preview(preview)
        self.assertEqual(self.active_profiles(), [])

    async def test_import_rechecks_duplicate_identity_and_capacity(self):
        preview = await self.preview()
        self.app.store.put("accounts", "existing", {"id": "existing", "telegram_id": 7001234})
        with self.assertRaisesRegex(ValueError, "已存在|重复"):
            await self.import_preview(preview)
        self.app.store.delete("accounts", "existing")
        preview = await self.preview()
        for i in range(8):
            self.app.store.put("accounts", f"existing{i}", {"id": f"existing{i}", "telegram_id": 9000 + i})
        with self.assertRaisesRegex(ValueError, "8|上限"):
            await self.import_preview(preview)
        self.assertEqual(len(self.active_profiles()), 8)

    async def test_existing_api_credentials_are_never_overwritten(self):
        current = {"api_id": 54321, "api_hash": "f" * 32}
        self.app.store.set_setting("telegram_api", current)
        preview = await self.preview()
        self.assertFalse(preview["can_import"])
        self.assertIn("API", preview["reason"])
        with self.assertRaises(ValueError):
            await self.import_preview(preview)
        self.assertEqual(self.app.store.setting("telegram_api"), current)
        self.assertEqual(self.active_profiles(), [])

    async def test_unauthorized_and_network_errors_return_honest_safe_preview(self):
        self.app.accounts.client_factory.authorized = False
        preview = await self.preview()
        self.assertFalse(preview["can_restore"])
        self.assertFalse(preview["can_import"])
        self.assertIsNone(preview["confirmation_token"])
        self.app.accounts.client_factory.authorized = True
        self.app.accounts.client_factory.fail = True
        preview = await self.preview()
        self.assertFalse(preview["can_restore"])
        self.assertNotIn(API_HASH, json.dumps(preview))
        self.assertNotIn(PHONE, json.dumps(preview))
        self.assertEqual(self.app.accounts.client_factory.clients[-1].operations[-1], "disconnect")

    async def test_source_change_during_registration_rolls_back_the_new_profile(self):
        preview = await self.preview()
        original_action = self.app.accounts.action

        async def changing_action(name, data):
            result = await original_action(name, data)
            if name == "add_account":
                self.config["group_ids"] = [-888]
                self.write_config()
            return result

        self.app.accounts.action = changing_action
        with self.assertRaisesRegex(ValueError, "来源|变化|重新"):
            await self.import_preview(preview)
        self.assertEqual(self.active_profiles(), [])

    async def test_seventh_existing_account_does_not_block_eighth_import(self):
        for i in range(7):
            self.app.store.put("accounts", f"existing{i}", {"id": f"existing{i}", "telegram_id": 9000 + i})
        preview = await self.preview()
        result = await self.import_preview(preview)
        self.assertIsNotNone(result["account_id"])
        self.assertEqual(len(self.active_profiles()), 8)

    async def test_pending_candidate_journal_never_becomes_a_restore_confirmation(self):
        self.app.accounts.client_factory.pending_journal = True
        preview = await self.preview()
        self.assertFalse(preview["can_import"])
        self.assertIsNone(preview["confirmation_token"])

    async def test_candidate_journal_added_after_preview_invalidates_import(self):
        preview = await self.preview()
        candidate = self.app.accounts.client_factory.clients[0].path
        candidate.with_name(candidate.name + "-wal").write_bytes(b"pending sqlite data")
        with self.assertRaisesRegex(ValueError, "候选|日志|变化"):
            await self.import_preview(preview)
        self.assertEqual(self.active_profiles(), [])

    async def test_changed_raw_backup_invalidates_import(self):
        preview = await self.preview()
        (Path(preview["backup_path"]) / "config.json").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "备份|变化|校验"):
            await self.import_preview(preview)
        self.assertEqual(self.active_profiles(), [])

    async def test_rollback_preserves_shared_settings_changed_during_cleanup(self):
        preview = await self.preview()
        original_action = self.app.accounts.action
        replacement = {"api_id": 99999, "api_hash": "a" * 32}

        async def changing_action(name, data):
            result = await original_action(name, data)
            if name == "add_account":
                self.config["group_ids"] = [-888]
                self.write_config()
            if name == "remove_account":
                self.app.store.set_setting("telegram_api", replacement)
            return result

        self.app.accounts.action = changing_action
        with self.assertRaisesRegex(ValueError, "来源|变化|重新"):
            await self.import_preview(preview)
        self.assertEqual(self.app.store.setting("telegram_api"), replacement)

    async def test_source_wal_change_during_import_invalidates_and_rolls_back(self):
        source_session = self.source / "account.session"
        with closing(sqlite3.connect(source_session)) as setup:
            setup.execute("PRAGMA journal_mode=WAL")
        preview = await self.preview()
        original_action = self.app.accounts.action
        writer = sqlite3.connect(source_session)
        self.addCleanup(writer.close)

        async def changing_action(name, data):
            result = await original_action(name, data)
            if name == "add_account":
                writer.execute("UPDATE sessions SET auth_key = ?", (b"z" * 256,))
                writer.commit()
            return result

        self.app.accounts.action = changing_action
        with self.assertRaisesRegex(ValueError, "来源|日志|变化|重新"):
            await self.import_preview(preview)
        self.assertEqual(self.active_profiles(), [])

    async def test_final_audit_failure_rolls_back_instead_of_returning_ambiguous_account(self):
        preview = await self.preview()

        def failing_log(*args, **kwargs):
            raise OSError("audit storage unavailable")

        self.app.log = failing_log
        with self.assertRaises(OSError):
            await self.import_preview(preview)
        self.assertEqual(self.active_profiles(), [])

    async def test_independent_legacy_process_lock_prevents_preview(self):
        process = subprocess.Popen([sys.executable, "-c",
            "import fcntl,sys; f=open(sys.argv[1],'rb'); fcntl.flock(f,fcntl.LOCK_EX); "
            "print('locked',flush=True); sys.stdin.read()", str(self.source / "account.lock")],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertEqual(process.stdout.readline().strip(), "locked")
            with self.assertRaisesRegex(ValueError, "占用|运行|停止"):
                await self.preview()
            self.assertEqual(self.app.accounts.client_factory.clients, [])
        finally:
            process.communicate(input="", timeout=5)

    async def test_bot_and_missing_real_identity_are_not_importable(self):
        for identity in (SimpleNamespace(id=123, bot=True),
                         SimpleNamespace(id=None, bot=False), None):
            self.app.accounts.client_factory.identity = identity
            preview = await self.preview()
            self.assertFalse(preview["can_restore"])
            self.assertIsNone(preview["confirmation_token"])
        self.assertEqual(self.active_profiles(), [])

    async def test_two_confirmations_for_same_identity_never_create_duplicate_profiles(self):
        first = await self.preview()
        second = await self.preview()
        results = await asyncio.gather(self.import_preview(first), self.import_preview(second),
                                       return_exceptions=True)
        self.assertEqual(sum(isinstance(value, dict) for value in results), 1)
        self.assertEqual(sum(isinstance(value, ValueError) for value in results), 1)
        self.assertEqual(len(self.active_profiles()), 1)

    async def test_real_account_manager_import_uses_independent_offline_identity(self):
        try:
            from accounts import AccountManager
        except ModuleNotFoundError as error:
            self.skipTest(f"integration dependency unavailable: {error.name}")
        factory = ClientFactory()
        self.app.accounts = AccountManager(self.app, client_factory=factory)
        self.service = MigrationService(self.app)
        preview = await self.preview()
        result = await self.import_preview(preview)
        aid = result["account_id"]
        account = self.app.accounts.get(aid)
        self.assertFalse(account.connected)
        self.assertFalse(account.logged_in)
        self.assertIsNone(account.client)
        row = self.app.accounts.public()[0]
        self.assertEqual(row["telegram_id"], 7001234)
        self.assertEqual(row["status"], "offline")
        self.assertEqual(row["phone_masked"], "***4567")
        self.assertEqual(len(factory.clients), 1)
        self.assertTrue((self.app.root / "accounts" / aid / "account.session").is_file())

    async def test_persistent_audit_failure_during_real_manager_rollback_clears_config_and_api(self):
        try:
            from accounts import AccountManager
        except ModuleNotFoundError as error:
            self.skipTest(f"integration dependency unavailable: {error.name}")

        async def offline(account_id):
            pass

        self.app.on_account_offline = offline
        self.app.accounts = AccountManager(self.app, client_factory=ClientFactory())
        self.service = MigrationService(self.app)
        preview = await self.preview()
        broken = False

        def persistent_failing_log(account_id, action, detail, **fields):
            nonlocal broken
            if action == "import_legacy":
                broken = True
            if broken:
                raise OSError("persistent audit storage failure")

        self.app.log = persistent_failing_log
        with self.assertRaises(OSError):
            await self.import_preview(preview)
        self.assertEqual(self.active_profiles(), [])
        self.assertEqual(self.app.store.list("listener_config"), [])
        self.assertIsNone(self.app.store.setting("telegram_api"))

    async def test_short_identity_fields_are_fully_masked_in_public_preview(self):
        identity = self.app.accounts.client_factory.identity
        identity.id, identity.phone, identity.username = 7019999, "867", "amy"
        preview = await self.preview()
        self.assertNotIn("867", json.dumps(preview["identity"]))
        self.assertNotIn("amy", json.dumps(preview["identity"]))

    async def test_symlinked_source_file_is_rejected_without_reading_it(self):
        outside = self.base / "outside.session"
        (self.source / "account.session").replace(outside)
        (self.source / "account.session").symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "符号链接|普通文件"):
            await self.preview()
        self.assertEqual(self.app.accounts.client_factory.clients, [])


if __name__ == "__main__":
    unittest.main()

import importlib
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

from telethon.tl import functions, types


def cleanup_module(test):
    try:
        return importlib.import_module("cleanup")
    except ModuleNotFoundError as error:
        if error.name != "cleanup":
            raise
        test.fail("完整联系人范围和备份接口尚未实现")


def registered(user_id=41, phone="15550101", **changes):
    return types.User(
        id=user_id, access_hash=changes.pop("access_hash", 9876),
        first_name="测试", last_name="联系人", phone=phone, contact=True, **changes
    )


class FakeTakeout:
    def __init__(self, owner):
        self.owner = owner

    async def __aenter__(self):
        self.owner.inside_takeout = True
        return self

    async def __aexit__(self, *args):
        self.owner.inside_takeout = False

    async def __call__(self, request):
        if not self.owner.inside_takeout or type(request) is not functions.contacts.GetSavedRequest:
            raise AssertionError("保存号码必须在 takeout 中读取")
        if self.owner.saved_error:
            raise self.owner.saved_error
        return self.owner.saved


class FakeClient:
    def __init__(self, users=None, saved=None, *, saved_count=0, contact_ids=None,
                 saved_error=None, response=None):
        self.users = list(users or [])
        self.saved = [] if saved is None else saved
        self.saved_error = saved_error
        self.response = response
        self.saved_count = saved_count
        self.contact_ids = contact_ids if contact_ids is not None else [u.id for u in self.users]
        self.inside_takeout = False
        self.takeout_requested = False

    async def __call__(self, request):
        if type(request) is not functions.contacts.GetContactsRequest or request.hash != 0:
            raise AssertionError("必须读取 hash=0 的完整联系人快照")
        if self.response is not None:
            return self.response
        return types.contacts.Contacts(
            contacts=[types.Contact(user_id=value, mutual=False) for value in self.contact_ids],
            saved_count=self.saved_count, users=self.users,
        )

    def takeout(self, **kwargs):
        if kwargs != {"contacts": True}:
            raise AssertionError("只申请联系人 takeout 范围")
        self.takeout_requested = True
        return FakeTakeout(self)


class ScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_saved_list_proves_zero_without_using_server_saved_count(self):
        # Inferring unregistered entries from Contacts.saved_count breaks this.
        cleanup = cleanup_module(self)
        client = FakeClient([registered()], saved_count=900)
        snapshot = await cleanup.read_full_snapshot(client, "account-a", 100)
        self.assertEqual(snapshot.account_id, "account-a")
        self.assertEqual(snapshot.own_id, 100)
        self.assertEqual([user.id for user in snapshot.users], [41])
        self.assertEqual(snapshot.saved, ())
        self.assertTrue(snapshot.scope_info["complete"])
        self.assertEqual(snapshot.scope_info["unregistered_count"], 0)
        self.assertEqual(snapshot.scope_info["registered_count"], 1)
        self.assertTrue(client.takeout_requested)
        self.assertFalse(client.inside_takeout)

    async def test_saved_phone_matching_verified_registered_reference_proves_zero(self):
        cleanup = cleanup_module(self)
        saved = types.SavedPhoneContact("+1 555-0101", "联系人", "", None)
        snapshot = await cleanup.read_full_snapshot(FakeClient([registered()], [saved]), "a", 100)
        self.assertTrue(snapshot.scope_info["complete"])
        self.assertEqual(snapshot.scope_info["saved_numbers_count"], 1)
        self.assertEqual(snapshot.scope_info["unregistered_count"], 0)
        self.assertEqual(snapshot.saved, (saved,))

    async def test_unmatched_saved_phone_remains_unknown_and_blocks_full_clear(self):
        # Treating SavedPhoneContact as an unregistered flag would invent counts.
        cleanup = cleanup_module(self)
        saved = types.SavedPhoneContact("15550999", "未知注册状态", "", None)
        snapshot = await cleanup.read_full_snapshot(FakeClient([registered()], [saved]), "a", 100)
        self.assertFalse(snapshot.scope_info["complete"])
        self.assertIsNone(snapshot.scope_info["unregistered_count"])
        self.assertIn("不支持完整清空", snapshot.scope_info["error"])
        self.assertEqual(len(snapshot.users), 1)
        self.assertEqual(len(snapshot.saved), 1)

    async def test_embedded_plus_cannot_prove_registered_phone_match(self):
        # Removing arbitrary '+' characters can falsely classify malformed data.
        cleanup = cleanup_module(self)
        snapshot = await cleanup.read_full_snapshot(
            FakeClient([registered()], [types.SavedPhoneContact("1555+0101", "", "", None)]),
            "a", 100,
        )
        self.assertFalse(snapshot.scope_info["complete"])
        self.assertIsNone(snapshot.scope_info["unregistered_count"])

    async def test_deleted_and_min_users_do_not_verify_a_saved_phone(self):
        cleanup = cleanup_module(self)
        for changes in ({"deleted": True}, {"min": True}):
            with self.subTest(changes=changes):
                snapshot = await cleanup.read_full_snapshot(
                    FakeClient([registered(**changes)], [types.SavedPhoneContact("15550101", "", "", None)]),
                    "a", 100,
                )
                self.assertFalse(snapshot.scope_info["complete"])
                self.assertIsNone(snapshot.scope_info["unregistered_count"])

    async def test_takeout_failure_preserves_registered_rows_and_capability_error(self):
        cleanup = cleanup_module(self)
        snapshot = await cleanup.read_full_snapshot(
            FakeClient([registered()], saved_error=RuntimeError("private phone 555-secret")), "a", 100,
        )
        self.assertEqual([user.id for user in snapshot.users], [41])
        self.assertFalse(snapshot.scope_info["complete"])
        self.assertIsNone(snapshot.scope_info["saved_numbers_count"])
        self.assertIsNone(snapshot.scope_info["unregistered_count"])
        self.assertIn("不支持完整清空", snapshot.scope_info["error"])
        self.assertNotIn("555-secret", snapshot.scope_info["error"])
        self.assertFalse(snapshot.scope_info["capabilities"]["saved_contacts_read"])

    async def test_missing_registered_user_remains_a_visible_unresolvable_reference(self):
        cleanup = cleanup_module(self)
        snapshot = await cleanup.read_full_snapshot(FakeClient([], contact_ids=[41]), "a", 100)
        self.assertEqual(snapshot.scope_info["registered_count"], 1)
        self.assertIsInstance(snapshot.users[0], types.UserEmpty)
        self.assertEqual(snapshot.users[0].id, 41)

    async def test_unreferenced_users_cannot_classify_saved_numbers(self):
        cleanup = cleanup_module(self)
        snapshot = await cleanup.read_full_snapshot(
            FakeClient([registered()], [types.SavedPhoneContact("15550101", "", "", None)], contact_ids=[]),
            "a", 100,
        )
        self.assertEqual(snapshot.users, ())
        self.assertFalse(snapshot.scope_info["complete"])
        self.assertIsNone(snapshot.scope_info["unregistered_count"])

    async def test_hash_zero_not_modified_response_cannot_claim_complete(self):
        cleanup = cleanup_module(self)
        with self.assertRaisesRegex(ValueError, "完整联系人"):
            await cleanup.read_full_snapshot(
                FakeClient(response=types.contacts.ContactsNotModified()), "a", 100,
            )

    async def test_malformed_saved_response_is_unknown_instead_of_empty(self):
        cleanup = cleanup_module(self)
        snapshot = await cleanup.read_full_snapshot(FakeClient([registered()], saved={}), "a", 100)
        self.assertFalse(snapshot.scope_info["complete"])
        self.assertIsNone(snapshot.scope_info["unregistered_count"])

    async def test_fingerprint_detects_account_and_scope_changes_but_not_order(self):
        cleanup = cleanup_module(self)
        first = await cleanup.read_full_snapshot(FakeClient([registered(), registered(42, "15550102")]), "a", 100)
        reordered = await cleanup.read_full_snapshot(FakeClient([registered(42, "15550102"), registered()]), "a", 100)
        changed = await cleanup.read_full_snapshot(FakeClient([registered(42, "15550102")]), "a", 100)
        account = await cleanup.read_full_snapshot(FakeClient([registered(), registered(42, "15550102")]), "b", 100)
        self.assertEqual(first.fingerprint, reordered.fingerprint)
        self.assertNotEqual(first.fingerprint, changed.fingerprint)
        self.assertNotEqual(first.fingerprint, account.fingerprint)


class BackupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cleanup = cleanup_module(self)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.snapshot = await self.cleanup.read_full_snapshot(
            FakeClient([registered()], [types.SavedPhoneContact("15550101", "未导出号码行", "", None)]),
            "account-a", 100,
        )

    async def test_backup_is_private_registered_only_and_validates_after_reread(self):
        metadata = self.cleanup.save_backup(self.root, self.snapshot)
        path = Path(metadata["path"])
        payload = self.cleanup.validate_backup(self.root, metadata, "account-a")
        self.assertEqual(metadata["count"], 1)
        self.assertEqual(metadata["name"], path.name)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(payload["account_id"], "account-a")
        self.assertEqual(payload["contacts"][0]["user_id"], 41)
        self.assertNotIn("saved", payload)
        self.assertNotIn("access_hash", path.read_text())
        self.assertNotIn("session", path.read_text())
        self.assertNotIn("未导出号码行", path.read_text())
        self.assertEqual(list(path.parent.glob("*.tmp")), [])

    async def test_corrupt_bytes_or_count_cannot_be_used_for_confirmation(self):
        metadata = self.cleanup.save_backup(self.root, self.snapshot)
        with self.assertRaises(ValueError):
            self.cleanup.validate_backup(self.root, {**metadata, "count": 2}, "account-a")
        Path(metadata["path"]).write_text("{}", encoding="utf-8")
        with self.assertRaises(ValueError):
            self.cleanup.validate_backup(self.root, metadata, "account-a")

    async def test_backup_is_bound_to_internal_account_and_private_backup_directory(self):
        metadata = self.cleanup.save_backup(self.root, self.snapshot)
        with self.assertRaises(ValueError):
            self.cleanup.validate_backup(self.root, metadata, "account-b")
        outside = self.root / "outside.json"
        outside.write_bytes(Path(metadata["path"]).read_bytes())
        with self.assertRaises(ValueError):
            self.cleanup.validate_backup(self.root, {**metadata, "path": str(outside), "name": outside.name}, "account-a")

    async def test_symlink_backup_file_and_directory_are_rejected(self):
        metadata = self.cleanup.save_backup(self.root, self.snapshot)
        path = Path(metadata["path"])
        other = self.root / "elsewhere.json"
        path.rename(other)
        path.symlink_to(other)
        with self.assertRaises(ValueError):
            self.cleanup.validate_backup(self.root, metadata, "account-a")
        path.unlink()
        path.parent.rmdir()
        path.parent.symlink_to(self.root)
        with self.assertRaises(ValueError):
            self.cleanup.save_backup(self.root, self.snapshot)


class InputUserTests(unittest.TestCase):
    def test_valid_user_becomes_real_input_user(self):
        cleanup = cleanup_module(self)
        result = cleanup.input_user(registered())
        self.assertIs(type(result), types.InputUser)
        self.assertEqual((result.user_id, result.access_hash), (41, 9876))

    def test_invalid_or_incomplete_user_cannot_be_used_for_deletion(self):
        cleanup = cleanup_module(self)
        for user in (types.UserEmpty(41), registered(access_hash=None), registered(min=True),
                     registered(deleted=True), registered(user_id=-1), registered(access_hash=True),
                     registered(access_hash=2 ** 63), {"id": 41, "access_hash": 9876}):
            with self.subTest(user=type(user).__name__):
                with self.assertRaises(ValueError):
                    cleanup.input_user(user)


if __name__ == "__main__":
    unittest.main()

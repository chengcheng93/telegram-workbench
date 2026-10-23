"""Account identity and authorization tests; all Telegram operations use fakes."""
import asyncio
import importlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

try:
    from accounts import AccountManager
except ImportError:
    AccountManager = None


class PhoneMigrateError(Exception):
    new_dc = 2


class SessionPasswordNeededError(Exception):
    pass


class PhoneCodeExpiredError(Exception):
    pass


class SessionRevokedError(Exception):
    pass


class PasswordHashInvalidError(Exception):
    pass


class PhoneNumberFloodError(Exception):
    pass


class FloodWaitError(Exception):
    seconds = 23


class SentCodeTypeApp:
    pass


class FakeClient:
    def __init__(self, identity=101, *, authorized=False, password=False):
        self.connected = False
        self.authorized = authorized
        self.identity = identity
        self.needs_password = password
        self.logouts = 0
        self.code_error = None
        self.code_errors = []
        self.code_requests = 0
        self.sign_started = self.sign_release = None
        self.password_error = None
        self.started = None
        self.release = None
        self.active_codes = 0
        self.max_active_codes = 0
        self.session_path = None
        self.connect_calls = 0
        self.connect_errors = []
        self._disconnected = None

    async def connect(self):
        self.connect_calls += 1
        if self.connect_errors:
            raise self.connect_errors.pop(0)
        self._disconnected = asyncio.get_running_loop().create_future()
        self.connected = True
        self.session_path.write_text('fake local session', encoding='utf-8')

    def is_connected(self):
        return self.connected

    @property
    def disconnected(self):
        return self._disconnected

    def drop(self, error=None):
        self.connected = False
        if self._disconnected is not None and not self._disconnected.done():
            if error:
                self._disconnected.set_exception(error)
            else:
                self._disconnected.set_result(None)

    async def disconnect(self):
        self.drop()

    async def is_user_authorized(self):
        return self.authorized

    async def get_me(self):
        if not self.authorized:
            return None
        return SimpleNamespace(id=self.identity, username='user' + str(self.identity),
                               phone='15551234567', first_name='Test', last_name='User')

    async def send_code_request(self, phone):
        if phone != '+15551234567':
            raise AssertionError('wrong normalized phone at Telegram boundary')
        self.code_requests += 1
        self.active_codes += 1
        self.max_active_codes = max(self.max_active_codes, self.active_codes)
        try:
            if self.started:
                self.started.set()
            if self.release:
                await self.release.wait()
            if self.code_errors:
                raise self.code_errors.pop(0)
            if self.code_error:
                raise self.code_error
            return SimpleNamespace(phone_code_hash='private-code-hash', type=SentCodeTypeApp())
        finally:
            self.active_codes -= 1

    async def sign_in(self, *, phone=None, code=None, phone_code_hash=None, password=None):
        if password is None:
            if (phone, code, phone_code_hash) != ('+15551234567', '12345', 'private-code-hash'):
                raise AssertionError('authorization parameters changed')
            if self.code_error:
                raise self.code_error
            if self.needs_password:
                raise SessionPasswordNeededError('sensitive code 12345')
        else:
            if password != 'private-two-step-password':
                raise AssertionError('wrong password at Telegram boundary')
            if self.password_error:
                raise self.password_error
        self.authorized = True
        if self.sign_started:
            self.sign_started.set()
        if self.sign_release:
            await self.sign_release.wait()
        return await self.get_me()

    async def log_out(self):
        self.logouts += 1
        self.authorized = False
        self.connected = False
        return True


class Factory:
    def __init__(self):
        self.clients = []
        self.created_paths = []

    def queue(self, client):
        self.clients.append(client)
        return client

    def __call__(self, path, api_id, api_hash):
        if (api_id, api_hash) != (12345, 'fake-api-hash'):
            raise AssertionError('missing configured app credentials')
        client = self.clients.pop(0)
        client.session_path = Path(path)
        self.created_paths.append(Path(path))
        return client


class FakeApp:
    def __init__(self, root, store):
        self.root, self.store = root, store
        self.ready = []
        self.offline = []
        self.revision = 0
        self.listener_started = False
        self.paused = []
        self.lifecycle = []

    def bump(self):
        self.revision += 1

    def log(self, account_id, action, detail, **fields):
        self.store.event(account_id, action, detail, **fields)
        self.bump()

    async def on_account_ready(self, account_id):
        self.ready.append(account_id)
        self.lifecycle.append(('ready', account_id))

    async def on_account_offline(self, account_id):
        self.offline.append(account_id)
        self.lifecycle.append(('offline', account_id))

    async def pause_account(self, account_id):
        self.paused.append(account_id)
        self.lifecycle.append(('pause', account_id))


class AccountTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(AccountManager, 'AccountManager implementation is missing')
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = importlib.import_module('storage').Store(self.root)
        self.store.set_setting('telegram_api', {'api_id': 12345, 'api_hash': 'fake-api-hash'})
        self.app = FakeApp(self.root, self.store)
        self.factory = Factory()
        self.manager = AccountManager(self.app, self.factory)
        self.other_managers = []

    async def asyncTearDown(self):
        if hasattr(self, 'manager'):
            await self.manager.close()
            for manager in self.other_managers:
                await manager.close()
            self.store.close()
            self.temp.cleanup()

    async def add(self, name='测试账号'):
        return (await self.manager.action('add_account', {'name': name}))['account_id']

    async def connect(self, account_id, client):
        self.factory.queue(client)
        return await self.manager.action('connect_account', {'account_id': account_id})

    def row(self, account_id):
        return next(row for row in self.manager.public() if row['id'] == account_id)

    async def test_eight_account_limit_and_rename_keep_stable_configuration(self):
        account_id = await self.add('相同名称')
        self.store.put('listener_config', 'saved', {'account_id': account_id, 'group_ids': [-1009]}, account_id=account_id)
        await self.manager.action('rename_account', {'account_id': account_id, 'name': '新名称'})
        for _ in range(7):
            await self.add('相同名称')
        with self.assertRaises(ValueError):
            await self.add()
        self.assertEqual(len(self.manager.public()), 8)
        self.assertEqual(self.row(account_id)['name'], '新名称')
        self.assertEqual(self.store.get('listener_config', 'saved', account_id=account_id)['group_ids'], [-1009])
        self.assertEqual(self.factory.created_paths, [])

    async def test_each_account_has_private_deterministic_session_and_ready_identity(self):
        one, two = await self.add(), await self.add()
        await self.connect(one, FakeClient(101, authorized=True))
        await self.connect(two, FakeClient(202, authorized=True))
        self.assertEqual(self.factory.created_paths, [self.root/'accounts'/one/'account.session', self.root/'accounts'/two/'account.session'])
        self.assertEqual(self.manager.require(one).own_id, 101)
        self.assertEqual(self.manager.require(two).own_id, 202)
        self.assertTrue(self.manager.require(one).epoch.tzinfo)
        self.assertEqual((self.root/'accounts'/one).stat().st_mode & 0o777, 0o700)
        self.assertEqual((self.root/'accounts'/one/'account.session').stat().st_mode & 0o777, 0o600)

    async def test_restore_reuses_valid_session_without_starting_listener(self):
        account_id = await self.add()
        await self.connect(account_id, FakeClient(101, authorized=True))
        await self.manager.close()
        restored_factory = Factory()
        restored_factory.queue(FakeClient(101, authorized=True))
        restored_app = FakeApp(self.root, self.store)
        restored = AccountManager(restored_app, restored_factory)
        self.other_managers.append(restored)
        await restored.restore()
        self.assertEqual(restored.require(account_id).own_id, 101)
        self.assertEqual(restored_app.ready, [account_id])
        self.assertFalse(restored_app.listener_started)
        self.assertEqual(restored.public()[0]['stage'], 'ready')

    async def test_identity_mismatch_never_overwrites_existing_profile(self):
        account_id = await self.add()
        await self.connect(account_id, FakeClient(101, authorized=True))
        await self.manager.action('disconnect_account', {'account_id': account_id})
        untrusted = FakeClient(202, authorized=True)
        with self.assertRaises(ValueError):
            await self.connect(account_id, untrusted)
        profile = self.store.get('accounts', account_id)
        self.assertEqual((profile['telegram_id'], profile['username']), (101, 'user101'))
        self.assertFalse(untrusted.connected)
        self.assertFalse(self.row(account_id)['logged_in'])
        self.assertFalse((self.root/'accounts'/account_id/'account.session').exists())
        with self.assertRaises(ValueError):
            self.manager.require(account_id)

    async def test_duplicate_real_identity_rejected_without_disrupting_first_account(self):
        one, two = await self.add(), await self.add()
        await self.connect(one, FakeClient(101, authorized=True))
        untrusted = FakeClient(101, authorized=True)
        with self.assertRaises(ValueError):
            await self.connect(two, untrusted)
        self.assertEqual(self.manager.require(one).own_id, 101)
        self.assertIsNone(self.store.get('accounts', two).get('telegram_id'))
        self.assertFalse(untrusted.connected)
        self.assertEqual(untrusted.logouts, 0)

    async def test_phone_code_password_flow_does_not_persist_secrets(self):
        account_id = await self.add()
        client = self.factory.queue(FakeClient(101, password=True))
        await self.manager.action('send_code', {'account_id': account_id, 'phone': '+1 555 123 4567'})
        self.assertEqual(self.row(account_id)['stage'], 'code')
        self.assertIn('Telegram', self.row(account_id)['login_channel'])
        await self.manager.action('sign_in', {'account_id': account_id, 'code': '12345'})
        self.assertEqual(self.row(account_id)['stage'], 'password')
        await self.manager.action('password', {'account_id': account_id, 'password': 'private-two-step-password'})
        self.assertEqual(self.manager.require(account_id).own_id, 101)
        exposed = json.dumps(self.manager.public()) + json.dumps(self.store.list('accounts')) + json.dumps(self.store.events())
        for secret in ('private-code-hash', 'private-two-step-password', '15551234567', '12345', 'fake-api-hash'):
            self.assertNotIn(secret, exposed)
        raw_db = (self.root/'workbench.sqlite3').read_bytes()
        for secret in (b'private-code-hash', b'private-two-step-password'):
            self.assertNotIn(secret, raw_db)
        self.assertNotEqual(self.row(account_id)['phone_masked'], '+15551234567')
        self.assertTrue(client.connected)

    async def test_cancel_after_successful_login_preserves_authorized_session_and_tasks(self):
        account_id = await self.add()
        client = self.factory.queue(FakeClient())
        await self.manager.action('send_code', {'account_id': account_id, 'phone': '+15551234567'})
        await self.manager.action('sign_in', {'account_id': account_id, 'code': '12345'})
        lifecycle = list(self.app.lifecycle)
        session_path = self.root/'accounts'/account_id/'account.session'
        await self.manager.action('cancel_login', {'account_id': account_id})
        self.assertTrue(client.connected)
        self.assertTrue(session_path.exists())
        self.assertEqual(self.row(account_id)['stage'], 'ready')
        self.assertEqual(self.manager.require(account_id).own_id, 101)
        self.assertEqual(self.app.lifecycle, lifecycle)
        self.assertEqual(self.app.paused, [])
        self.assertEqual(client.logouts, 0)

    async def test_closing_online_reconnect_drawer_preserves_connection_and_running_tasks(self):
        account_id = await self.add()
        client = FakeClient(101, authorized=True)
        await self.connect(account_id, client)
        await self.manager.action('connect_account', {'account_id': account_id})
        lifecycle = list(self.app.lifecycle)
        await self.manager.action('cancel_login', {'account_id': account_id})
        self.assertTrue(client.connected)
        self.assertTrue((self.root/'accounts'/account_id/'account.session').exists())
        self.assertTrue(self.row(account_id)['logged_in'])
        self.assertEqual(self.app.lifecycle, lifecycle)
        self.assertEqual(self.app.paused, [])
        self.assertEqual(client.connect_calls, 1)

    async def test_cancel_login_clears_challenge_and_keeps_profile(self):
        account_id = await self.add()
        client = self.factory.queue(FakeClient())
        await self.manager.action('send_code', {'account_id': account_id, 'phone': '+15551234567'})
        session_path = self.root/'accounts'/account_id/'account.session'
        self.assertTrue(session_path.exists())
        await self.manager.action('cancel_login', {'account_id': account_id})
        self.assertFalse(session_path.exists())
        self.assertFalse(client.connected)
        self.assertEqual(self.row(account_id)['stage'], 'phone')
        with self.assertRaises(ValueError):
            await self.manager.action('sign_in', {'account_id': account_id, 'code': '12345'})
        self.assertIsNotNone(self.store.get('accounts', account_id))

    async def test_cancel_inflight_login_cannot_resurrect_authorization(self):
        account_id = await self.add()
        client = self.factory.queue(FakeClient())
        client.started, client.release = asyncio.Event(), asyncio.Event()
        pending = asyncio.create_task(self.manager.action('send_code', {'account_id': account_id, 'phone': '+15551234567'}))
        await client.started.wait()
        await self.manager.action('cancel_login', {'account_id': account_id})
        client.release.set()
        result = await asyncio.gather(pending, return_exceptions=True)
        self.assertIsInstance(result[0], ValueError)
        self.assertFalse(self.row(account_id)['connected'])
        self.assertEqual(self.row(account_id)['stage'], 'phone')

    async def test_expired_code_and_wrong_password_have_safe_recoverable_errors(self):
        account_id = await self.add()
        client = self.factory.queue(FakeClient(password=True))
        await self.manager.action('send_code', {'account_id': account_id, 'phone': '+15551234567'})
        client.code_error = PhoneCodeExpiredError('raw 12345 private-code-hash')
        with self.assertRaises(ValueError) as error:
            await self.manager.action('sign_in', {'account_id': account_id, 'code': '12345'})
        self.assertNotIn('12345', str(error.exception))
        self.assertEqual(self.row(account_id)['stage'], 'phone')
        client.code_error = None
        await self.manager.action('send_code', {'account_id': account_id, 'phone': '+15551234567'})
        await self.manager.action('sign_in', {'account_id': account_id, 'code': '12345'})
        client.password_error = PasswordHashInvalidError('private-two-step-password')
        with self.assertRaises(ValueError):
            await self.manager.action('password', {'account_id': account_id, 'password': 'private-two-step-password'})
        self.assertEqual(self.row(account_id)['stage'], 'password')
        self.assertNotIn('private-two-step-password', json.dumps(self.manager.public()))

    async def test_platform_wait_is_persisted_and_does_not_freeze_other_account(self):
        one, two = await self.add(), await self.add()
        blocked = self.factory.queue(FakeClient())
        blocked.code_error = FloodWaitError('unsafe 12345')
        with self.assertRaises(ValueError):
            await self.manager.action('send_code', {'account_id': one, 'phone': '+15551234567'})
        self.assertEqual(self.row(one)['status'], 'waiting')
        self.assertIsNotNone(self.store.get('accounts', one)['wait_until'])
        self.assertNotIn('12345', self.row(one)['error'])
        await self.connect(two, FakeClient(202, authorized=True))
        self.assertEqual(self.manager.require(two).own_id, 202)

    async def test_flock_prevents_two_managers_using_one_session_until_disconnect(self):
        account_id = await self.add()
        await self.connect(account_id, FakeClient(101, authorized=True))
        factory = Factory()
        factory.queue(FakeClient(101, authorized=True))
        other = AccountManager(FakeApp(self.root, self.store), factory)
        self.other_managers.append(other)
        with self.assertRaises(ValueError):
            await other.action('connect_account', {'account_id': account_id})
        self.assertEqual(factory.created_paths, [])
        await self.manager.action('disconnect_account', {'account_id': account_id})
        await other.action('connect_account', {'account_id': account_id})
        self.assertEqual(other.require(account_id).own_id, 101)

    async def test_same_account_auth_is_serial_and_other_account_can_progress(self):
        one, two = await self.add(), await self.add()
        client = self.factory.queue(FakeClient())
        client.started, client.release = asyncio.Event(), asyncio.Event()
        first = asyncio.create_task(self.manager.action('send_code', {'account_id': one, 'phone': '+15551234567'}))
        await client.started.wait()
        second = asyncio.create_task(self.manager.action('send_code', {'account_id': one, 'phone': '+15551234567'}))
        await self.connect(two, FakeClient(202, authorized=True))
        self.assertEqual(self.manager.require(two).own_id, 202)
        client.release.set()
        await asyncio.gather(first, second)
        self.assertEqual(client.max_active_codes, 1)

    async def test_disconnect_preserves_session_logout_revokes_and_remove_keeps_evidence(self):
        one, two = await self.add(), await self.add()
        client_one, client_two = FakeClient(101, authorized=True), FakeClient(202, authorized=True)
        await self.connect(one, client_one)
        await self.connect(two, client_two)
        await self.manager.action('disconnect_account', {'account_id': one})
        self.assertEqual(client_one.logouts, 0)
        self.assertTrue((self.root/'accounts'/one/'account.session').exists())
        with self.assertRaises(ValueError):
            await self.manager.action('logout_account', {'account_id': two})
        await self.manager.action('logout_account', {'account_id': two, 'confirmed': True})
        self.assertEqual(client_two.logouts, 1)
        self.assertIn(two, [row['id'] for row in self.manager.public()])
        self.store.put('tasks', 'evidence', {'account_id': one, 'status': 'paused'}, account_id=one)
        await self.manager.action('remove_account', {'account_id': one, 'confirmed': True})
        self.assertNotIn(one, [row['id'] for row in self.manager.public()])
        self.assertTrue(self.store.get('accounts', one)['deleted'])
        self.assertEqual(self.store.get('tasks', 'evidence', account_id=one)['status'], 'paused')
        self.assertFalse((self.root/'accounts'/one/'account.session').exists())
        self.assertIn(one, self.app.offline)

    async def test_imported_expected_identity_is_reloaded_before_connection(self):
        account_id = await self.add()
        profile = self.store.get('accounts', account_id)
        profile['telegram_id'] = 777
        self.store.put('accounts', account_id, profile)
        with self.assertRaises(ValueError):
            await self.connect(account_id, FakeClient(888, authorized=True))
        self.assertEqual(self.store.get('accounts', account_id)['telegram_id'], 777)

    async def test_identity_rejection_always_disconnects_even_if_offline_hook_fails(self):
        account_id = await self.add()
        await self.connect(account_id, FakeClient(101, authorized=True))
        await self.manager.action('disconnect_account', {'account_id': account_id})
        untrusted = self.factory.queue(FakeClient(202, authorized=True))
        with patch.object(self.app, 'on_account_offline', side_effect=OSError('disk failure')):
            with self.assertRaises(ValueError):
                await self.manager.action('connect_account', {'account_id': account_id})
        self.assertFalse(untrusted.connected)
        self.assertFalse((self.root/'accounts'/account_id/'account.session').exists())
        self.assertEqual(self.store.get('accounts', account_id)['telegram_id'], 101)
        with self.assertRaises(ValueError):
            self.manager.require(account_id)

    async def test_unknown_platform_restriction_survives_disconnect_restore_until_explicit_ack(self):
        account_id = await self.add()
        client = self.factory.queue(FakeClient())
        client.code_error = PhoneNumberFloodError('raw private-code-hash')
        with self.assertRaises(ValueError):
            await self.manager.action('send_code', {'account_id': account_id, 'phone': '+15551234567'})
        self.assertTrue(self.store.get('accounts', account_id).get('wait_restricted'))
        await self.manager.action('disconnect_account', {'account_id': account_id})
        restored = AccountManager(FakeApp(self.root, self.store), self.factory)
        self.other_managers.append(restored)
        with self.assertRaises(ValueError):
            await restored.action('send_code', {'account_id': account_id, 'phone': '+15551234567'})
        with self.assertRaises(ValueError):
            restored.clear_wait(account_id)
        restored.clear_wait(account_id, confirmed=True)
        self.assertFalse(self.store.get('accounts', account_id)['wait_restricted'])
        self.factory.queue(FakeClient())
        await restored.action('send_code', {'account_id': account_id, 'phone': '+15551234567'})
        self.assertEqual(restored.public()[0]['stage'], 'code')

    async def test_known_platform_wait_cannot_be_acknowledged_before_due_time(self):
        account_id = await self.add()
        client = self.factory.queue(FakeClient())
        client.code_error = FloodWaitError('raw error')
        with self.assertRaises(ValueError):
            await self.manager.action('send_code', {'account_id': account_id, 'phone': '+15551234567'})
        result = getattr(self.manager, 'clear_wait', None)
        self.assertIsNotNone(result, 'missing explicit platform-wait acknowledgment')
        with self.assertRaises(ValueError):
            result(account_id, confirmed=True)
        self.assertIsNotNone(self.store.get('accounts', account_id)['wait_until'])

    async def test_cancel_after_server_accepted_sign_in_discards_local_authorized_session(self):
        account_id = await self.add()
        client = self.factory.queue(FakeClient())
        await self.manager.action('send_code', {'account_id': account_id, 'phone': '+15551234567'})
        client.sign_started, client.sign_release = asyncio.Event(), asyncio.Event()
        pending = asyncio.create_task(self.manager.action('sign_in', {'account_id': account_id, 'code': '12345'}))
        await client.sign_started.wait()
        await self.manager.action('cancel_login', {'account_id': account_id})
        client.sign_release.set()
        result = await asyncio.gather(pending, return_exceptions=True)
        self.assertIsInstance(result[0], ValueError)
        self.assertTrue(client.authorized)
        self.assertFalse((self.root/'accounts'/account_id/'account.session').exists())
        self.assertFalse(self.row(account_id)['logged_in'])

    async def test_confirmed_login_dc_migration_is_retried_but_unknown_outcome_is_not(self):
        one, two = await self.add(), await self.add()
        migrating = self.factory.queue(FakeClient())
        migrating.code_errors = [PhoneMigrateError('move to verified data center')]
        outcome = await asyncio.gather(self.manager.action('send_code', {'account_id': one, 'phone': '+15551234567'}), return_exceptions=True)
        self.assertIsInstance(outcome[0], dict)
        self.assertEqual(self.row(one)['stage'], 'code')
        self.assertEqual(migrating.code_requests, 2)
        unknown = self.factory.queue(FakeClient())
        unknown.code_error = ConnectionError('lost response')
        with self.assertRaises(ValueError):
            await self.manager.action('send_code', {'account_id': two, 'phone': '+15551234567'})
        self.assertEqual(unknown.code_requests, 1)

    async def test_add_account_persistence_failure_does_not_leave_orphan_profile_or_runtime(self):
        with patch.object(self.app, 'log', side_effect=OSError('disk full')):
            outcome = await asyncio.gather(self.manager.action('add_account', {'name': '保存失败'}), return_exceptions=True)
        self.assertIsInstance(outcome[0], ValueError)
        self.assertEqual(self.manager.public(), [])
        self.assertEqual(self.store.list('accounts'), [])
        self.assertEqual(list((self.root/'accounts').iterdir()), [])

    async def test_remove_cannot_delete_session_held_by_another_manager(self):
        account_id = await self.add()
        await self.connect(account_id, FakeClient(101, authorized=True))
        other = AccountManager(FakeApp(self.root, self.store), Factory())
        self.other_managers.append(other)
        with self.assertRaises(ValueError):
            await other.action('remove_account', {'account_id': account_id, 'confirmed': True})
        self.assertTrue((self.root/'accounts'/account_id/'account.session').exists())
        self.assertFalse(self.store.get('accounts', account_id)['deleted'])
        self.assertEqual(self.manager.require(account_id).own_id, 101)

    async def test_network_reconnect_closes_gate_then_revalidates_identity_and_epoch(self):
        account_id = await self.add()
        client = FakeClient(101, authorized=True)
        await self.connect(account_id, client)
        old_epoch = self.manager.require(account_id).epoch
        with patch('accounts._RECONNECT_DELAYS', (0, 0, 0), create=True):
            client.drop(ConnectionError('network disconnected'))
            for _ in range(100):
                if len(self.app.ready) == 2:
                    break
                await asyncio.sleep(0)
        self.assertEqual(self.app.lifecycle[:3], [('ready', account_id), ('offline', account_id), ('ready', account_id)])
        self.assertEqual(client.connect_calls, 2)
        self.assertEqual(self.manager.require(account_id).own_id, 101)
        self.assertGreater(self.manager.require(account_id).epoch, old_epoch)
        self.assertEqual(self.app.paused, [])
        self.assertFalse(self.app.listener_started)

    async def test_reconnect_identity_mismatch_or_revoked_auth_cannot_bind_or_request_login(self):
        one, two = await self.add(), await self.add()
        changed = FakeClient(101, authorized=True)
        revoked = FakeClient(202, authorized=True)
        await self.connect(one, changed)
        await self.connect(two, revoked)
        changed.identity = 999
        revoked.authorized = False
        with patch('accounts._RECONNECT_DELAYS', (0, 0, 0), create=True):
            changed.drop()
            revoked.drop(SessionRevokedError('private-code-hash'))
            for _ in range(100):
                if self.row(one)['status'] == 'needs_login' and self.row(two)['status'] == 'needs_login':
                    break
                await asyncio.sleep(0)
        self.assertEqual(self.store.get('accounts', one)['telegram_id'], 101)
        self.assertEqual(self.store.get('accounts', two)['telegram_id'], 202)
        self.assertFalse(changed.connected)
        self.assertFalse(revoked.connected)
        self.assertEqual(self.row(two)['status'], 'needs_login')
        self.assertEqual(changed.connect_calls, 2)
        self.assertLessEqual(revoked.connect_calls, 2)
        self.assertNotIn('private-code-hash', json.dumps(self.manager.public()))

    async def test_reconnect_attempts_are_bounded_and_explicit_disconnect_cancels_backoff(self):
        one, two = await self.add(), await self.add()
        failed, manual = FakeClient(101, authorized=True), FakeClient(202, authorized=True)
        await self.connect(one, failed)
        await self.connect(two, manual)
        failed.connect_errors = [ConnectionError('offline') for _ in range(3)]
        with patch('accounts._RECONNECT_DELAYS', (0, 0, 0), create=True):
            failed.drop()
            for _ in range(100):
                if failed.connect_calls == 4:
                    break
                await asyncio.sleep(0)
            await asyncio.sleep(0)
        self.assertEqual(failed.connect_calls, 4)
        self.assertEqual(self.row(one)['status'], 'offline')
        with patch('accounts._RECONNECT_DELAYS', (60, 60, 60), create=True):
            manual.drop()
            for _ in range(100):
                if two in self.app.offline:
                    break
                await asyncio.sleep(0)
            await self.manager.action('disconnect_account', {'account_id': two})
        self.assertEqual(manual.connect_calls, 1)
        self.assertIn(two, self.app.paused)
        self.assertFalse(self.row(two)['connected'])

    async def test_external_value_error_never_leaks_telegram_secrets(self):
        account_id = await self.add()
        client = self.factory.queue(FakeClient())
        client.code_error = ValueError('12345 private-code-hash private-two-step-password')
        with self.assertRaises(ValueError) as caught:
            await self.manager.action('send_code', {'account_id': account_id, 'phone': '+15551234567'})
        exposed = str(caught.exception) + json.dumps(self.manager.public()) + json.dumps(self.store.events())
        self.assertNotIn('private-code-hash', exposed)
        self.assertNotIn('12345', exposed)
        self.assertNotIn('private-two-step-password', exposed)

    async def test_invalid_account_or_missing_api_never_creates_client(self):
        with self.assertRaises(ValueError):
            self.manager.require('../other')
        account_id = await self.add()
        self.store.set_setting('telegram_api', {})
        with self.assertRaises(ValueError):
            await self.manager.action('connect_account', {'account_id': account_id})
        self.assertEqual(self.factory.created_paths, [])


if __name__ == '__main__':
    unittest.main()

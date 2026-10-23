import asyncio
import copy
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS

from telethon import errors, events, types, utils
from telethon.tl.custom import ParticipantPermissions

try:
    from listeners import ListenerService
except ModuleNotFoundError:
    ListenerService = None

UTC = timezone.utc
G1, G2 = -11, -12


class MemoryStore:
    def __init__(self):
        self.rows = {}

    def get(self, kind, key, default=None, *, account_id=''):
        return copy.deepcopy(self.rows.get((kind, account_id, str(key)), default))

    def put(self, kind, key, value, *, account_id=''):
        self.rows[kind, account_id, str(key)] = copy.deepcopy(value)

    def delete(self, kind, key, *, account_id=''):
        self.rows.pop((kind, account_id, str(key)), None)

    def list(self, kind, account_id=None):
        return [copy.deepcopy(v) for (k, a, _), v in sorted(self.rows.items())
                if k == kind and (account_id is None or a == account_id)]


class FakeClient:
    def __init__(self):
        self.handlers = []
        self.group_ids = [11, 12]
        self.entities = {}
        self.permissions = {}
        self.read_calls = []

    def add_event_handler(self, callback, builder):
        self.handlers.append((callback, builder))

    def remove_event_handler(self, callback, *args):
        self.handlers = [(c, b) for c, b in self.handlers if c != callback]

    async def iter_dialogs(self):
        for gid in self.group_ids:
            yield NS(entity=types.PeerChat(gid), id=-gid, name=f'群{gid}', is_group=True)

    async def get_entity(self, group_id):
        self.read_calls.append(('entity', group_id))
        result = self.entities[group_id]
        if isinstance(result, BaseException):
            raise result
        return result

    async def get_permissions(self, entity, who):
        gid = utils.get_peer_id(entity)
        self.read_calls.append(('permissions', gid, who))
        result = self.permissions[gid]
        if isinstance(result, BaseException):
            raise result
        return result



class FakeAccounts:
    def __init__(self):
        self.sessions = {a: NS(id=a, client=FakeClient(), own_id=1, logged_in=True,
                               stage='ready', status='online', wait_until=None,
                               epoch=datetime.now(UTC) - timedelta(seconds=3)) for a in ['A', 'B']}

    def require(self, account_id):
        session = self.sessions[account_id]
        if not session.logged_in:
            raise ValueError('请先登录账号')
        return session

    def get(self, account_id):
        return self.sessions[account_id]


class FakeContacts:
    def __init__(self, app):
        self.app = app
        self.rows = {'A': [], 'B': []}
        self.submitted = []
        self.entered = asyncio.Event()
        self.release = None
        self.error = None
        self.refresh_error = None

    def cached(self, account_id):
        return copy.deepcopy(self.rows[account_id])

    async def refresh(self, account_id):
        if self.refresh_error:
            raise self.refresh_error
        return self.cached(account_id)

    async def add_person(self, account_id, user, source='manual'):
        # ContactService owns shared scheduling. Listener must never nest execute.
        async def rpc():
            listener = getattr(self.app, 'listeners', None)
            if listener and callable(getattr(listener, 'allow_submission', None)):
                if not listener.allow_submission(account_id, user.id):
                    raise ValueError('监听候选已失效')
                listener.mark_submitted(account_id, user.id)
            self.submitted.append((account_id, user.id, source))
            self.entered.set()
            if self.release:
                await self.release.wait()
            if self.error:
                raise self.error
            row = {'id': user.id, 'user_id': user.id, 'name': user.first_name, 'source': source}
            self.rows[account_id].append(row)
            return row
        return await self.app.execute(account_id, rpc, priority=10)


class FakeTasks:
    def __init__(self):
        self.locks = {'A': asyncio.Lock(), 'B': asyncio.Lock()}
        self.blocked = set()

    def contact_lock(self, account_id):
        return self.locks[account_id]

    def contact_blocked(self, account_id):
        return account_id in self.blocked


class FakeApp:
    def __init__(self, store=None):
        self.store = store or MemoryStore()
        self.accounts = FakeAccounts()
        self.tasks = FakeTasks()
        self.contacts = FakeContacts(self)
        self.global_paused = False
        self.waiting = set()
        self.logs = []
        self.executing = set()
        self.revision = 0
        self.wait_errors = []

    def bump(self):
        self.revision += 1

    def log(self, account_id, action, detail, **fields):
        self.logs.append(dict(account_id=account_id, action=action, detail=detail, **fields))
        self.bump()

    def record_wait(self, account_id, error):
        self.waiting.add(account_id)
        self.wait_errors.append((account_id, type(error).__name__))

    def automation_allowed(self, account_id):
        return not self.global_paused and account_id not in self.waiting

    def wait_state(self, account_id):
        return {'wait_until': '2099-01-01T00:00:00+00:00' if account_id in self.waiting else None}

    async def execute(self, account_id, operation, **kwargs):
        if account_id in self.executing:
            raise AssertionError('同账号调度嵌套会死锁')
        self.executing.add(account_id)
        try:
            return await operation()
        finally:
            self.executing.discard(account_id)


def user(uid=10, **changes):
    fields = dict(id=uid, first_name=f'用户{uid}', last_name=None, username=None,
                  bot=False, deleted=False, is_self=False, contact=False)
    fields.update(changes)
    return NS(**fields)


def basic_group(gid=11, **kwargs):
    return types.Chat(id=gid, title=f'普通群{gid}', photo=types.ChatPhotoEmpty(),
                      participants_count=12, date=datetime.now(UTC), version=1, **kwargs)


def super_group(gid=21, **kwargs):
    return types.Channel(id=gid, title=f'超级群{gid}', photo=types.ChatPhotoEmpty(),
                         date=datetime.now(UTC), megagroup=True, access_hash=gid * 100, **kwargs)


async def set_groups(client, entities):
    client.entities = {utils.get_peer_id(entity): entity for entity in entities}
    async def dialogs():
        for entity in entities:
            yield NS(entity=entity, id=utils.get_peer_id(entity), name=entity.title, is_group=True)
    client.iter_dialogs = dialogs



def event(uid=10, mid=100, gid=G1, date=None, sender=None, message=None):
    sender = sender or user(uid)
    msg = message or types.Message(id=mid, peer_id=types.PeerChat(abs(gid)),
                                    from_id=types.PeerUser(uid), date=date or datetime.now(UTC) + timedelta(seconds=2),
                                    message='不得进入日志的原文')
    async def get_sender():
        return sender
    return NS(message=msg, chat_id=gid, sender_id=uid, is_group=True,
              get_sender=get_sender)


async def settle():
    for _ in range(15):
        await asyncio.sleep(0)


class ListenerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(ListenerService, '监听模块尚未实现')
        self.app = FakeApp()
        self.service = ListenerService(self.app)
        self.app.listeners = self.service
        for aid in ['A', 'B']:
            await self.service.ready(aid)
            await self.service.action('refresh_groups', {'account_id': aid})

    async def asyncTearDown(self):
        if hasattr(self, 'service'):
            if self.app.contacts.release:
                self.app.contacts.release.set()
            await self.service.close()

    async def start(self, aid='A', mode='live', group_ids=None):
        current = self.service.snapshot(aid)['config']['version']
        await self.service.action('save_listener', {'account_id': aid, 'group_ids': group_ids or [G1, G2],
                                                   'mode': mode, 'expected_version': current})
        return await self.service.action('start_listener', {'account_id': aid, 'mode': mode})

    async def test_two_accounts_same_group_and_user_are_independent(self):
        await self.start('A')
        await self.start('B', 'preview')
        await self.service.handle_event('A', event())
        await self.service.handle_event('B', event())
        await settle()
        self.assertEqual([(a, uid) for a, uid, _ in self.app.contacts.submitted], [('A', 10)])
        self.assertEqual(self.service.snapshot('A')['run']['counts']['added'], 1)
        self.assertEqual(self.service.snapshot('B')['run']['counts']['preview'], 1)
        self.assertEqual(self.app.store.list('listener_processed', 'B'), [])
        self.assertNotIn('不得进入日志', str(self.app.logs))

    async def test_preview_repeated_candidates_never_prevent_future_live_add(self):
        await self.start(mode='preview')
        await self.service.handle_event('A', event(mid=100))
        await self.service.handle_event('A', event(mid=101))
        self.assertEqual(self.service.snapshot('A')['run']['counts']['preview'], 2)
        await self.service.action('stop_listener', {'account_id': 'A'})
        await self.start()
        await self.service.handle_event('A', event(mid=102))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 1)

    async def test_cross_group_user_and_same_message_dedup_persist_after_delete_restart(self):
        await self.start()
        await self.service.handle_event('A', event(mid=100))
        await self.service.handle_event('A', event(mid=101, gid=G2))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 1)
        await self.service.close()
        self.app.contacts.rows['A'] = []  # Manual deletion retains processed evidence.
        self.service = ListenerService(self.app)
        self.app.listeners = self.service
        self.assertEqual(self.service.snapshot('A')['state'], 'stopped')
        await self.service.ready('A')
        await self.start()
        await self.service.handle_event('A', event(mid=100))
        await self.service.handle_event('A', event(mid=102))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 1)
        await self.service.action('reset_processed', {'account_id': 'A', 'user_ids': [10], 'confirmed': True})
        await self.service.handle_event('A', event(mid=103))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 2)

    async def test_rejects_history_edits_services_and_strict_start_boundary(self):
        await self.start()
        started = datetime.fromisoformat(self.service.snapshot('A')['run']['started_at'])
        for idx, timestamp in enumerate([started - timedelta(microseconds=1), started]):
            await self.service.handle_event('A', event(mid=200 + idx, date=timestamp))
        await self.service.handle_event('A', event(mid=202), source='history')
        msg = event(mid=203).message
        await self.service.handle_event('A', events.MessageEdited.Event(msg))
        service = types.MessageService(id=204, peer_id=types.PeerChat(11), date=started + timedelta(seconds=3),
                                       action=types.MessageActionChatAddUser(users=[10]), from_id=types.PeerUser(10))
        await self.service.handle_event('A', event(mid=204, message=service))
        await settle()
        self.assertEqual(self.app.contacts.submitted, [])
        await self.service.handle_event('A', event(mid=205, date=started + timedelta(microseconds=1)))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 1)

    async def test_reconnect_and_resume_establish_new_receive_boundaries(self):
        await self.start()
        old = datetime.now(UTC)
        await self.service.offline('A')
        await self.service.handle_event('A', event(mid=300))
        self.app.accounts.get('A').epoch = datetime.now(UTC)
        await self.service.ready('A')
        await self.service.handle_event('A', event(mid=301, date=old))
        await self.service.action('pause_listener', {'account_id': 'A'})
        paused = datetime.now(UTC)
        await self.service.action('resume_listener', {'account_id': 'A'})
        await self.service.handle_event('A', event(mid=302, date=paused))
        await self.service.handle_event('A', event(mid=303))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 1)
        self.assertEqual(len(self.app.accounts.get('A').client.handlers), 1)

    async def test_config_cas_and_running_apply_cancel_old_pending_keep_inflight(self):
        await self.start()
        self.app.contacts.release = asyncio.Event()
        await self.service.handle_event('A', event(uid=10, mid=400))
        await self.app.contacts.entered.wait()
        await self.service.handle_event('A', event(uid=20, mid=401))
        with self.assertRaises(ValueError):
            await self.service.action('save_listener', {'account_id': 'A', 'group_ids': [G2], 'mode': 'live', 'expected_version': 0})
        with self.assertRaises(ValueError):
            await self.service.action('save_listener', {'account_id': 'A', 'group_ids': [G2], 'mode': 'live', 'expected_version': 1})
        await self.service.action('save_listener', {'account_id': 'A', 'group_ids': [G2], 'mode': 'live',
                                                   'expected_version': 1, 'apply_confirmed': True})
        self.app.contacts.release.set()
        await settle()
        self.assertEqual([uid for _, uid, _ in self.app.contacts.submitted], [10])
        snap = self.service.snapshot('A')
        self.assertEqual(snap['run']['version'], 2)
        self.assertEqual(snap['run']['counts']['added'], 1)
        self.assertEqual(snap['run']['counts']['pending'], 0)
        records = self.app.store.list('listener_processed', 'A')
        self.assertEqual(records[0]['config_version'], 1)
        self.assertEqual(records[0]['run_id'], snap['run']['id'])

    async def test_sender_resolution_cannot_cross_config_epoch(self):
        await self.start()
        entered, release = asyncio.Event(), asyncio.Event()
        e = event(mid=450)
        async def get_sender():
            entered.set()
            await release.wait()
            return user()
        e.get_sender = get_sender
        pending = asyncio.create_task(self.service.handle_event('A', e))
        await entered.wait()
        await self.service.action('save_listener', {'account_id': 'A', 'group_ids': [G2], 'mode': 'live',
                                                   'expected_version': 1, 'apply_confirmed': True})
        release.set()
        await pending
        await settle()
        self.assertEqual(self.app.contacts.submitted, [])

    async def test_stop_closes_gate_cancels_pending_and_drains_inflight(self):
        await self.start()
        self.app.contacts.release = asyncio.Event()
        await self.service.handle_event('A', event(mid=500))
        await self.app.contacts.entered.wait()
        await self.service.handle_event('A', event(uid=20, mid=501))
        stopping = asyncio.create_task(self.service.stop('A'))
        await settle()
        self.assertFalse(stopping.done())
        self.assertEqual(self.service.snapshot('A')['state'], 'stopping')
        await self.service.handle_event('A', event(uid=30, mid=502))
        self.app.contacts.release.set()
        await stopping
        self.assertEqual([uid for _, uid, _ in self.app.contacts.submitted], [10])
        self.assertEqual(self.service.snapshot('A')['state'], 'stopped')
        self.assertEqual(self.service.snapshot('A')['run']['counts']['pending'], 0)

    async def test_cleanup_gate_rechecked_after_contact_lock_and_does_not_block_B(self):
        await self.start('A')
        await self.start('B')
        lock = self.app.tasks.contact_lock('A')
        await lock.acquire()
        await self.service.handle_event('A', event(mid=600))
        await settle()
        self.app.tasks.blocked.add('A')
        lock.release()
        await self.service.handle_event('B', event(mid=600))
        await settle()
        self.assertEqual([a for a, _, _ in self.app.contacts.submitted], ['B'])
        self.assertEqual(self.service.snapshot('A')['run']['counts']['pending'], 0)

    async def test_waiting_account_and_global_pause_do_not_accumulate_work(self):
        await self.start('A')
        await self.start('B')
        self.app.waiting.add('A')
        await self.service.handle_event('A', event(mid=650))
        await self.service.handle_event('B', event(mid=650))
        await settle()
        self.assertEqual([a for a, _, _ in self.app.contacts.submitted], ['B'])
        self.assertEqual(self.service.snapshot('A')['state'], 'waiting')
        self.app.global_paused = True
        await self.service.handle_event('B', event(uid=30, mid=651))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 1)
        self.assertEqual(self.service.snapshot('B')['run']['counts']['pending'], 0)

    async def test_queue_is_bounded_when_first_request_is_blocked(self):
        await self.start()
        self.app.contacts.release = asyncio.Event()
        await self.service.handle_event('A', event(mid=700))
        await self.app.contacts.entered.wait()
        for uid in range(20, 220):
            await self.service.handle_event('A', event(uid=uid, mid=700 + uid))
        snap = self.service.snapshot('A')
        self.assertLessEqual(snap['run']['counts']['pending'], 101)
        self.assertGreater(snap['run']['counts']['skipped'], 50)
        self.app.contacts.release.set()
        await self.service.stop('A')

    async def test_unknown_outcome_recovers_by_read_only_verification_and_never_retries_absent(self):
        await self.start()
        self.app.contacts.error = ConnectionError('断开')
        await self.service.handle_event('A', event(mid=900))
        await settle()
        records = self.app.store.list('listener_processed', 'A')
        self.assertEqual(records[0]['status'], 'unverified')
        self.assertEqual(self.service.snapshot('A')['run']['counts']['added'], 0)
        await self.service.close()
        self.app.contacts.error = None
        self.service = ListenerService(self.app)
        self.app.listeners = self.service
        await self.service.ready('A')
        await self.start()
        await self.service.handle_event('A', event(mid=901))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 1)
        self.app.contacts.rows['A'] = [{'id': 10, 'user_id': 10}]
        await self.service.action('refresh_groups', {'account_id': 'A'})
        self.assertEqual(self.app.store.list('listener_processed', 'A')[0]['status'], 'added')
        self.assertEqual(self.service.snapshot('A')['run']['counts']['added'], 0)

    async def test_crashed_inflight_never_replays_and_failed_reconciliation_preserves_unknown(self):
        self.app.store.put('listener_processed', '10', {'account_id': 'A', 'user_id': 10,
                           'run_id': 'old-run', 'config_version': 1, 'status': 'inflight'}, account_id='A')
        self.app.contacts.refresh_error = ConnectionError('离线')
        await self.service.close()
        self.service = ListenerService(self.app)
        self.app.listeners = self.service
        await self.service.ready('A')
        await self.start()
        await self.service.handle_event('A', event(mid=950))
        await settle()
        self.assertEqual(self.app.contacts.submitted, [])
        self.assertEqual(self.app.store.list('listener_processed', 'A')[0]['status'], 'unverified')

    async def inspect_group(self, account_id, group_id):
        try:
            return await self.service.action('inspect_group', {'account_id': account_id, 'group_id': group_id})
        except ValueError as error:
            self.fail(f'应返回只读群权限核实结果：{error}')

    async def test_server_error_after_actual_add_keeps_unknown_until_read_verification(self):
        await self.start()
        original = self.app.contacts.add_person
        async def ambiguous_add(aid, person, source='manual'):
            await original(aid, person, source)
            raise errors.ServerError(None, 'INTERNAL', 500)
        self.app.contacts.add_person = ambiguous_add
        await self.service.handle_event('A', event())
        await settle()
        record = self.app.store.get('listener_processed', '10', account_id='A')
        self.assertIsNotNone(record, '提交后服务器错误必须保留持久未知占位')
        self.assertEqual(record['status'], 'unverified')
        self.assertEqual(self.service.snapshot('A')['run']['counts']['added'], 0)
        await self.service.handle_event('A', event(mid=101))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 1)
        await self.service.action('resume_listener', {'account_id': 'A'})
        record = self.app.store.get('listener_processed', '10', account_id='A')
        self.assertEqual(record['status'], 'added')
        self.assertEqual(self.service.snapshot('A')['run']['counts']['added'], 1)
        self.app.contacts.rows['A'] = []
        await self.service.handle_event('A', event(mid=102))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 1)

    async def test_group_refresh_infers_types_and_three_state_permissions_without_extra_rpc(self):
        client = self.app.accounts.get('A').client
        entities = [
            basic_group(11, creator=True),
            basic_group(12, admin_rights=types.ChatAdminRights(invite_users=False)),
            basic_group(13, default_banned_rights=types.ChatBannedRights(until_date=None)),
            basic_group(14, default_banned_rights=types.ChatBannedRights(until_date=None, invite_users=True)),
            basic_group(15),
            super_group(21, admin_rights=types.ChatAdminRights(invite_users=True)),
            super_group(22, admin_rights=types.ChatAdminRights(invite_users=False)),
            super_group(23, default_banned_rights=types.ChatBannedRights(until_date=None),
                        banned_rights=types.ChatBannedRights(until_date=None, invite_users=True)),
            super_group(24),
        ]
        await set_groups(client, entities)
        await self.service.action('refresh_groups', {'account_id': 'A'})
        rows = self.service.snapshot('A')['groups']
        self.assertEqual([(row.get('group_type'), row.get('can_invite')) for row in rows], [
            ('basic', True), ('basic', True), ('basic', True), ('basic', False), ('basic', None),
            ('supergroup', True), ('supergroup', False), ('supergroup', False), ('supergroup', None)])
        self.assertTrue(all(row.get('permission_checked_at') is None for row in rows))
        self.assertEqual(client.read_calls, [], '刷新列表不可为每群追加权限请求')

    async def test_group_refresh_excludes_broadcast_and_migrated_group_even_if_dialog_flag_is_group(self):
        client = self.app.accounts.get('A').client
        broadcast = types.Channel(id=22, title='广播频道', photo=types.ChatPhotoEmpty(),
                                  date=datetime.now(UTC), broadcast=True, access_hash=2200)
        migrated = basic_group(13, migrated_to=types.InputChannel(channel_id=21, access_hash=2100))
        await set_groups(client, [basic_group(), broadcast, migrated])
        await self.service.action('refresh_groups', {'account_id': 'A'})
        self.assertEqual([row['id'] for row in self.service.snapshot('A')['groups']], [G1])

    async def test_inspect_restricted_member_uses_actual_invite_ban_not_admin_only_property(self):
        client = self.app.accounts.get('A').client
        entity = super_group(default_banned_rights=types.ChatBannedRights(until_date=None))
        gid = -1000000000021
        await set_groups(client, [entity])
        await self.service.action('refresh_groups', {'account_id': 'A'})
        for invite_banned, expected in [(False, True), (True, False)]:
            participant = types.ChannelParticipantBanned(peer=types.PeerUser(1), kicked_by=9,
                date=datetime.now(UTC), left=False,
                banned_rights=types.ChatBannedRights(until_date=None, send_messages=True, invite_users=invite_banned))
            client.permissions[gid] = ParticipantPermissions(participant, False)
            row = (await self.inspect_group('A', gid))['group']
            self.assertIs(row['can_invite'], expected)
            self.assertTrue(row['available'])
            self.assertIsNotNone(row['permission_checked_at'])
        self.assertEqual(self.app.contacts.submitted, [])

    async def test_inspect_permission_shapes_and_network_failure_do_not_invent_permission(self):
        client = self.app.accounts.get('A').client
        entity = basic_group(creator=True)
        await set_groups(client, [entity])
        await self.service.action('refresh_groups', {'account_id': 'A'})
        # A member shape for channels does not prove membership of a basic group.
        client.permissions[G1] = ParticipantPermissions(types.ChannelParticipant(user_id=1, date=datetime.now(UTC)), False)
        row = (await self.inspect_group('A', G1))['group']
        self.assertIsNone(row['can_invite'])
        self.assertIsNone(row['permission_checked_at'])
        client.permissions[G1] = ConnectionError('任意远端异常原文不得公开')
        with self.assertRaises(ValueError) as error:
            await self.service.action('inspect_group', {'account_id': 'A', 'group_id': G1})
        self.assertNotIn('任意远端异常原文', str(error.exception))
        self.assertIsNone(self.service.snapshot('A')['groups'][0]['can_invite'])
        self.assertEqual(self.app.wait_errors, [('A', 'ConnectionError')])

    async def test_inspect_group_checks_current_membership_and_normal_member_default_rights_read_only(self):
        client = self.app.accounts.get('A').client
        entity = basic_group(default_banned_rights=types.ChatBannedRights(until_date=None))
        await set_groups(client, [entity])
        client.permissions[G1] = ParticipantPermissions(types.ChatParticipant(user_id=1, inviter_id=9,
                                                                               date=datetime.now(UTC)), True)
        await self.service.action('refresh_groups', {'account_id': 'A'})
        result = await self.inspect_group('A', G1)
        row = result['group']
        self.assertTrue(row['can_invite'])
        self.assertEqual(row['group_type'], 'basic')
        self.assertIsNotNone(datetime.fromisoformat(row['permission_checked_at']))
        self.assertEqual(client.read_calls, [('entity', G1), ('permissions', G1, 'me')])
        self.assertEqual(self.app.contacts.submitted, [])
        self.assertEqual(self.app.store.list('listener_processed', 'A'), [])
        self.assertEqual(self.service.snapshot('A')['state'], 'stopped')

    async def test_inspect_supergroup_denial_retains_row_and_reason(self):
        client = self.app.accounts.get('A').client
        entity = super_group(admin_rights=types.ChatAdminRights(invite_users=False))
        gid = -1000000000021
        await set_groups(client, [entity])
        client.permissions[gid] = ParticipantPermissions(types.ChannelParticipantAdmin(
            user_id=1, promoted_by=9, date=datetime.now(UTC), admin_rights=types.ChatAdminRights(invite_users=False)), False)
        await self.service.action('refresh_groups', {'account_id': 'A'})
        result = await self.inspect_group('A', gid)
        self.assertIs(result['group']['can_invite'], False)
        self.assertTrue(result['group']['permission_reason'])
        self.assertIsNotNone(result['group']['permission_checked_at'])
        self.assertEqual(len(self.service.snapshot('A')['groups']), 1)
        self.assertTrue(result['group']['available'])
        self.assertEqual(self.app.contacts.submitted, [])

    async def test_inspect_group_rejects_unknown_or_other_account_group_before_rpc(self):
        client = self.app.accounts.get('A').client
        for data in [{'account_id': 'A', 'group_id': -99}, {'group_id': G1},
                     {'account_id': 'A', 'group_id': True}]:
            with self.assertRaises(ValueError):
                await self.service.action('inspect_group', data)
        self.assertEqual(client.read_calls, [])
        self.assertEqual([row['id'] for row in self.service.snapshot('A')['groups']], [G1, G2])

    async def test_inspect_group_cannot_accept_other_identity_or_mismatched_entity(self):
        client = self.app.accounts.get('A').client
        entity = basic_group(default_banned_rights=types.ChatBannedRights(until_date=None))
        await set_groups(client, [entity])
        client.permissions[G1] = ParticipantPermissions(types.ChatParticipant(user_id=999, inviter_id=9,
                                                                               date=datetime.now(UTC)), True)
        await self.service.action('refresh_groups', {'account_id': 'A'})
        row = (await self.inspect_group('A', G1))['group']
        self.assertIsNone(row['can_invite'])
        self.assertIsNone(row['permission_checked_at'])
        client.entities[G1] = basic_group(99, creator=True)
        with self.assertRaises(ValueError):
            await self.service.action('inspect_group', {'account_id': 'A', 'group_id': G1})
        self.assertEqual(self.service.snapshot('A')['groups'][0]['id'], G1)

    async def test_inspect_nonmember_and_permission_denied_are_explicit_without_dropping_group(self):
        client = self.app.accounts.get('A').client
        await set_groups(client, [basic_group()])
        await self.service.action('refresh_groups', {'account_id': 'A'})
        for error, expected in [(errors.UserNotParticipantError(None), False),
                                (errors.ChatAdminRequiredError(None), None),
                                (errors.ChatAdminInviteRequiredError(None), None),
                                (errors.ChatWriteForbiddenError(None), None)]:
            client.permissions[G1] = error
            row = (await self.inspect_group('A', G1))['group']
            self.assertIs(row['can_invite'], expected)
            if expected is False:
                self.assertIsNotNone(row['permission_checked_at'])
            else:
                self.assertIsNone(row['permission_checked_at'])
            self.assertTrue(row['permission_reason'])
            self.assertEqual(len(self.service.snapshot('A')['groups']), 1)

    async def test_inspect_resolution_failure_clears_old_verified_permission_without_raw_error(self):
        client = self.app.accounts.get('A').client
        entity = basic_group(creator=True)
        await set_groups(client, [entity])
        client.permissions[G1] = ParticipantPermissions(types.ChatParticipantCreator(user_id=1), True)
        await self.service.action('refresh_groups', {'account_id': 'A'})
        verified = (await self.inspect_group('A', G1))['group']
        self.assertTrue(verified['can_invite'])
        self.assertIsNotNone(verified['permission_checked_at'])
        client.entities[G1] = ValueError('任意账号路径/远端明文不得公开')
        with self.assertRaises(ValueError) as error:
            await self.service.action('inspect_group', {'account_id': 'A', 'group_id': G1})
        row = self.service.snapshot('A')['groups'][0]
        self.assertIsNone(row['can_invite'])
        self.assertIsNone(row['permission_checked_at'])
        self.assertNotIn('任意账号路径', str(error.exception))
        self.assertEqual(row['id'], G1)

    async def test_inspect_group_platform_wait_stays_unknown_and_only_waits_current_account(self):
        client = self.app.accounts.get('A').client
        await set_groups(client, [basic_group(creator=True)])
        await self.service.action('refresh_groups', {'account_id': 'A'})
        client.permissions[G1] = errors.FloodWaitError(None, capture=20)
        with self.assertRaises(ValueError):
            await self.service.action('inspect_group', {'account_id': 'A', 'group_id': G1})
        row = self.service.snapshot('A')['groups'][0]
        self.assertIsNone(row.get('can_invite'))
        self.assertIsNone(row.get('permission_checked_at'))
        self.assertEqual(self.app.wait_errors, [('A', 'FloodWaitError')])
        self.assertNotIn('B', self.app.waiting)
        self.assertEqual(len(client.read_calls), 2)

    async def test_real_shared_scheduler_cancels_listener_queued_behind_manual_chat(self):
        from scheduler import AccountScheduler
        scheduler = AccountScheduler(self.app)
        self.app.execute = scheduler.execute
        entered, release = asyncio.Event(), asyncio.Event()
        async def manual_chat():
            entered.set()
            await release.wait()
        manual = asyncio.create_task(self.app.execute('A', manual_chat, priority=0, automation=False))
        try:
            await entered.wait()
            await self.start()
            await self.service.handle_event('A', event())
            await settle()
            self.assertEqual(self.app.contacts.submitted, [])
            await self.service.action('save_listener', {'account_id': 'A', 'group_ids': [G1], 'mode': 'preview',
                                                       'expected_version': 1, 'apply_confirmed': True})
            release.set()
            await manual
            await scheduler.drain('A')
            self.assertEqual(self.app.contacts.submitted, [])
            self.assertEqual(self.app.store.list('listener_processed', 'A'), [])
            self.assertEqual(self.service.snapshot('A')['run']['counts']['pending'], 0)
        finally:
            release.set()
            await manual
            await scheduler.close()

    async def test_two_live_accounts_add_same_person_independently(self):
        await self.start('A')
        await self.start('B')
        await self.service.handle_event('A', event())
        await settle()
        await self.service.handle_event('B', event())
        await settle()
        self.assertEqual([aid for aid, _, _ in self.app.contacts.submitted], ['A', 'B'])
        self.assertEqual(self.service.snapshot('A')['run']['counts']['added'], 1)
        self.assertEqual(self.service.snapshot('B')['run']['counts']['added'], 1)

    async def test_definite_failure_has_no_success_dedup_and_no_unbounded_retries(self):
        await self.start()
        self.app.contacts.error = ValueError('明确无效身份')
        await self.service.handle_event('A', event())
        await settle()
        self.assertEqual(self.app.store.list('listener_processed', 'A'), [])
        self.assertEqual(self.service.snapshot('A')['run']['counts']['added'], 0)
        self.assertEqual(self.service.snapshot('A')['run']['counts']['failed'], 1)
        self.app.contacts.error = None
        await self.service.handle_event('A', event(mid=101))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 1)
        await self.service.action('reset_processed', {'account_id': 'A', 'user_ids': [10], 'confirmed': True})
        await self.service.handle_event('A', event(mid=102))
        await settle()
        self.assertEqual(len(self.app.contacts.submitted), 2)

    async def test_stop_timeout_keeps_inflight_contact_lock_until_rpc_finishes(self):
        await self.start()
        self.service.DRAIN_TIMEOUT = 0.01
        self.app.contacts.release = asyncio.Event()
        await self.service.handle_event('A', event())
        await self.app.contacts.entered.wait()
        with self.assertRaises(ValueError):
            await self.service.stop('A')
        self.assertEqual(self.service.snapshot('A')['state'], 'stopping')
        self.assertTrue(self.app.tasks.contact_lock('A').locked())
        record = self.app.store.get('listener_processed', '10', account_id='A')
        self.assertEqual(record['status'], 'unverified')
        self.app.contacts.release.set()
        await settle()
        self.assertEqual(self.service.snapshot('A')['state'], 'stopped')
        self.assertFalse(self.app.tasks.contact_lock('A').locked())

    async def test_config_apply_cancels_shared_scheduler_item_before_rpc(self):
        await self.start()
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.app.execute
        async def deferred_execute(aid, operation, **kwargs):
            entered.set()
            await release.wait()
            return await original(aid, operation, **kwargs)
        self.app.execute = deferred_execute
        try:
            await self.service.handle_event('A', event())
            await entered.wait()
            await self.service.action('save_listener', {'account_id': 'A', 'group_ids': [G1], 'mode': 'preview',
                                                       'expected_version': 1, 'apply_confirmed': True})
            release.set()
            await settle()
            self.assertEqual(self.app.contacts.submitted, [])
            self.assertEqual(self.app.store.list('listener_processed', 'A'), [])
            self.assertEqual(self.service.snapshot('A')['run']['counts']['pending'], 0)
        finally:
            release.set()

    async def test_stop_cancels_shared_scheduler_item_without_waiting_for_rpc(self):
        await self.start()
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.app.execute
        async def deferred_execute(aid, operation, **kwargs):
            entered.set()
            await release.wait()
            return await original(aid, operation, **kwargs)
        self.app.execute = deferred_execute
        try:
            await self.service.handle_event('A', event())
            await entered.wait()
            stopped = asyncio.create_task(self.service.stop('A'))
            await settle()
            self.assertTrue(stopped.done(), '停止必须取消共享调度队列中尚未提交的项')
            await stopped
            release.set()
            await settle()
            self.assertEqual(self.app.contacts.submitted, [])
        finally:
            release.set()

    async def test_stop_during_start_read_cannot_reopen_listener(self):
        await self.start()
        await self.service.stop('A')
        self.app.store.put('listener_processed', '10', {'account_id': 'A', 'user_id': 10,
                           'run_id': 'old-run', 'config_version': 1, 'status': 'unverified'}, account_id='A')
        entered, release = asyncio.Event(), asyncio.Event()
        async def delayed_refresh(aid):
            entered.set()
            await release.wait()
            return []
        self.app.contacts.refresh = delayed_refresh
        starting = asyncio.create_task(self.service.action('start_listener', {'account_id': 'A', 'mode': 'live'}))
        await entered.wait()
        await self.service.stop('A')
        release.set()
        await asyncio.gather(starting, return_exceptions=True)
        self.assertEqual(self.service.snapshot('A')['state'], 'stopped')
        await self.service.handle_event('A', event(uid=20))
        await settle()
        self.assertEqual(self.app.contacts.submitted, [])

    async def test_stop_during_resume_read_cannot_reopen_listener(self):
        await self.start()
        await self.service.action('pause_listener', {'account_id': 'A'})
        self.app.store.put('listener_processed', '10', {'account_id': 'A', 'user_id': 10,
                           'run_id': 'old-run', 'config_version': 1, 'status': 'unverified'}, account_id='A')
        entered, release = asyncio.Event(), asyncio.Event()
        async def delayed_refresh(aid):
            entered.set()
            await release.wait()
            return []
        self.app.contacts.refresh = delayed_refresh
        resuming = asyncio.create_task(self.service.action('resume_listener', {'account_id': 'A'}))
        await entered.wait()
        await self.service.stop('A')
        release.set()
        await asyncio.gather(resuming, return_exceptions=True)
        self.assertEqual(self.service.snapshot('A')['state'], 'stopped')

    async def test_forbidden_group_is_unavailable_and_cannot_start(self):
        await self.start(group_ids=[G1])
        await self.service.stop('A')
        async def forbidden_dialogs():
            yield NS(entity=types.ChatForbidden(id=11, title='已失效群'), id=G1,
                     name='已失效群', is_group=True)
        self.app.accounts.get('A').client.iter_dialogs = forbidden_dialogs
        await self.service.action('refresh_groups', {'account_id': 'A'})
        group = next(g for g in self.service.snapshot('A')['groups'] if g['id'] == G1)
        self.assertFalse(group['available'])
        with self.assertRaises(ValueError):
            await self.service.action('start_listener', {'account_id': 'A', 'mode': 'live'})

    async def test_retained_missing_contact_row_cannot_verify_unknown_success(self):
        self.app.store.put('listener_processed', '10', {'account_id': 'A', 'user_id': 10,
                           'run_id': 'old-run', 'config_version': 1, 'status': 'unverified'}, account_id='A')
        self.app.contacts.rows['A'] = [{'id': '10', 'user_id': 10, 'current': False,
                                      'eligible': False, 'write_status': 'unverified'}]
        await self.service.action('refresh_groups', {'account_id': 'A'})
        self.assertEqual(self.app.store.get('listener_processed', '10', account_id='A')['status'], 'unverified')
        self.assertEqual(self.service.snapshot('A')['totals']['added'], 0)

    async def test_concurrent_verifications_only_count_recovered_success_once(self):
        self.app.store.put('listener_processed', '10', {'account_id': 'A', 'user_id': 10,
                           'run_id': 'old-run', 'config_version': 1, 'status': 'unverified'}, account_id='A')
        entered, release = asyncio.Event(), asyncio.Event()
        async def delayed_refresh(aid):
            entered.set()
            await release.wait()
            return [{'id': '10', 'user_id': 10, 'current': True}]
        self.app.contacts.refresh = delayed_refresh
        first = asyncio.create_task(self.service.action('refresh_groups', {'account_id': 'A'}))
        await entered.wait()
        second = asyncio.create_task(self.service.action('refresh_groups', {'account_id': 'A'}))
        await settle()
        release.set()
        await asyncio.gather(first, second)
        self.assertEqual(self.service.snapshot('A')['totals']['added'], 1)

    async def test_preview_existing_contact_never_writes_success_dedup(self):
        await self.start(mode='preview')
        await self.service.handle_event('A', event(sender=user(contact=True)))
        self.assertEqual(self.app.store.list('listener_processed', 'A'), [])

    async def test_read_only_refresh_cannot_count_current_inflight_twice(self):
        await self.start()
        self.app.contacts.release = asyncio.Event()
        await self.service.handle_event('A', event())
        await self.app.contacts.entered.wait()
        self.app.contacts.rows['A'] = [{'id': 10, 'user_id': 10}]
        await self.service.action('refresh_groups', {'account_id': 'A'})
        self.app.contacts.release.set()
        await settle()
        self.assertEqual(self.service.snapshot('A')['run']['counts']['added'], 1)
        self.assertEqual(self.service.snapshot('A')['totals']['added'], 1)

    async def test_inflight_log_retains_submitted_mode_when_config_changes(self):
        await self.start()
        self.app.contacts.release = asyncio.Event()
        await self.service.handle_event('A', event())
        await self.app.contacts.entered.wait()
        await self.service.action('save_listener', {'account_id': 'A', 'group_ids': [G1], 'mode': 'preview',
                                                   'expected_version': 1, 'apply_confirmed': True})
        self.app.contacts.release.set()
        await settle()
        completed = [e for e in self.app.logs if e.get('result') == 'added']
        self.assertEqual(completed[0]['mode'], 'live')
        self.assertEqual(completed[0]['config_version'], 1)

    async def test_contact_created_while_waiting_lock_is_rechecked_before_add(self):
        await self.start()
        lock = self.app.tasks.contact_lock('A')
        await lock.acquire()
        await self.service.handle_event('A', event())
        await settle()
        self.app.contacts.rows['A'] = [{'id': 10, 'user_id': 10}]
        lock.release()
        await settle()
        self.assertEqual(self.app.contacts.submitted, [])
        self.assertEqual(self.service.snapshot('A')['run']['counts']['pending'], 0)

    async def test_group_refresh_cannot_overwrite_config_saved_during_read(self):
        await self.start()
        entered, release = asyncio.Event(), asyncio.Event()
        client = self.app.accounts.get('A').client
        original = client.iter_dialogs
        async def delayed_dialogs():
            entered.set()
            await release.wait()
            async for dialog in original():
                yield dialog
        client.iter_dialogs = delayed_dialogs
        refreshing = asyncio.create_task(self.service.action('refresh_groups', {'account_id': 'A'}))
        await entered.wait()
        await self.service.action('save_listener', {'account_id': 'A', 'group_ids': [G2], 'mode': 'preview',
                                                   'expected_version': 1, 'apply_confirmed': True})
        release.set()
        await refreshing
        config = self.service.snapshot('A')['config']
        self.assertEqual(config, {'group_ids': [G2], 'mode': 'preview', 'version': 2})

    async def test_normalized_groups_and_unavailable_selection_stays_visible(self):
        snap = self.service.snapshot('A')
        self.assertEqual([g['id'] for g in snap['groups']], [G1, G2])
        await self.start(group_ids=[G1])
        await self.service.stop('A')
        self.app.accounts.get('A').client.group_ids = [12]
        await self.service.action('refresh_groups', {'account_id': 'A'})
        unavailable = [g for g in self.service.snapshot('A')['groups'] if g['id'] == G1]
        self.assertEqual(len(unavailable), 1)
        self.assertFalse(unavailable[0]['available'])
        with self.assertRaises(ValueError):
            await self.service.action('start_listener', {'account_id': 'A', 'mode': 'live'})

    async def test_self_bot_anonymous_existing_and_invalid_modes_are_excluded(self):
        await self.start()
        for uid, changes in [(1, {}), (10, {'bot': True}), (11, {'deleted': True}), (12, {'contact': True})]:
            await self.service.handle_event('A', event(uid=uid, mid=1000 + uid, sender=user(uid, **changes)))
        anon = event(uid=-100123, mid=1010)
        anon.message.from_id = types.PeerChannel(123)
        await self.service.handle_event('A', anon)
        await settle()
        self.assertEqual(self.app.contacts.submitted, [])
        with self.assertRaises(ValueError):
            await self.service.action('start_listener', {'account_id': 'A', 'mode': 'preview'})
        with self.assertRaises(ValueError):
            await self.service.action('reset_processed', {'account_id': 'A', 'all': True})


if __name__ == '__main__':
    unittest.main()

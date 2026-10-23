"""Isolated contract tests; Telegram transport is replaced, never connected."""
import asyncio
import copy
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from telethon import errors, functions, types
from telethon.tl.custom import ParticipantPermissions

try:
    from contacts import ContactService
except ImportError:
    ContactService = None


class MemoryStore:
    def __init__(self): self.values = {}
    def get(self, kind, key, default=None, *, account_id=''):
        return copy.deepcopy(self.values.get((kind, account_id, str(key)), default))
    def put(self, kind, key, value, *, account_id=''):
        self.values[kind, account_id, str(key)] = copy.deepcopy(value)
    def delete(self, kind, key, *, account_id=''):
        self.values.pop((kind, account_id, str(key)), None)
    def list(self, kind, account_id=None):
        return [copy.deepcopy(value) for (k, a, _), value in sorted(self.values.items())
                if k == kind and (account_id is None or a == account_id)]
    def setting(self, key, default=None): return self.get('settings', key, default)
    def set_setting(self, key, value): self.put('settings', key, value)


def user(identifier, **kwargs):
    return types.User(identifier, access_hash=identifier * 10,
                      first_name=f'用户{identifier}', contact=True, **kwargs)


class FakeTakeout:
    def __init__(self, client): self.client = client
    async def __aenter__(self):
        if self.client.saved_error: raise self.client.saved_error
        self.client.takeout_open = True
        return self.client
    async def __aexit__(self, *_): self.client.takeout_open = False


class FakeClient:
    def __init__(self, own_id=100):
        self.own_id = own_id
        self.users = {1: user(1, phone='15550000001'), 2: user(2)}
        self.saved = []
        self.saved_count = 91
        self.saved_error = None
        self.takeout_open = False
        self.mutations = []
        self.requests = []
        self.members = set()
        self.invite_errors = {}
        self.invite_unverified = set()
        self.before_mutation = None
        self.block = None
        self.read_gate = None
        self.read_started = None
        self.group = types.Chat(9, '测试群', types.ChatPhotoEmpty(), 2,
                               datetime.now(timezone.utc), 1, creator=True)
    def takeout(self, **kwargs):
        assert kwargs.get('contacts') is True
        return FakeTakeout(self)
    async def get_entity(self, peer):
        if peer == -9: return self.group
        if isinstance(peer, (types.User, types.InputUser)): peer = peer.id if hasattr(peer, 'id') else peer.user_id
        if isinstance(peer, str):
            for item in self.users.values():
                if item.username == peer.lstrip('@') or item.phone == peer.lstrip('+'): return item
            raise ValueError('找不到用户')
        return self.users.get(peer, user(peer))
    async def get_permissions(self, target, person):
        identifier = self.own_id if person == 'me' else person.id
        if identifier == self.own_id:
            return ParticipantPermissions(types.ChatParticipantCreator(identifier), True)
        if identifier not in self.members: raise errors.UserNotParticipantError(None)
        return ParticipantPermissions(types.ChatParticipant(identifier, 100, datetime.now(timezone.utc)), True)
    async def __call__(self, request):
        self.requests.append(request)
        if isinstance(request, functions.contacts.GetContactsRequest):
            if self.read_started: self.read_started.set()
            if self.read_gate: await self.read_gate.wait()
            assert request.hash == 0
            return types.contacts.Contacts([types.Contact(i, False) for i in self.users],
                                           self.saved_count, list(self.users.values()))
        if isinstance(request, functions.contacts.GetSavedRequest):
            assert self.takeout_open, 'GetSaved outside takeout'
            return self.saved
        self.mutations.append(request)
        if self.before_mutation: self.before_mutation(request)
        if self.block: await self.block.wait()
        if isinstance(request, functions.contacts.AddContactRequest):
            self.users[request.id.id] = request.id
            return types.Updates([], [], [], datetime.now(timezone.utc), 1)
        if isinstance(request, functions.contacts.DeleteContactsRequest):
            for item in request.id: self.users.pop(item.user_id, None)
            return types.Updates([], [], [], datetime.now(timezone.utc), 1)
        if isinstance(request, functions.contacts.ResetSavedRequest):
            self.saved = []
            return True
        if isinstance(request, functions.messages.AddChatUserRequest):
            identifier = request.user_id.user_id
            if identifier in self.invite_errors: raise self.invite_errors[identifier]
            if identifier not in self.invite_unverified: self.members.add(identifier)
            return types.messages.InvitedUsers(types.Updates([], [], [], datetime.now(timezone.utc), 1), [])
        raise AssertionError(type(request))


class PhoneClient(FakeClient):
    """A read-only phone endpoint, separate from the existing-contact cache."""
    def __init__(self, result=None, *, error=None, before_result=None):
        super().__init__()
        self.result = result
        self.error = error
        self.before_result = before_result
        self.phone_requests = []
        self.entity_queries = []

    async def get_entity(self, peer):
        self.entity_queries.append(peer)
        return await super().get_entity(peer)

    async def __call__(self, request):
        if type(request) is functions.contacts.ResolvePhoneRequest:
            self.requests.append(request)
            self.phone_requests.append(request.phone)
            if self.before_result:
                self.before_result()
            if self.error:
                raise self.error
            return self.result
        return await super().__call__(request)


class FakeListeners:
    def __init__(self, app):
        self.app, self.stopped = app, []
        self.allowed, self.submitted = True, []
    def allow_submission(self, account_id, user_id): return self.allowed
    def mark_submitted(self, account_id, user_id): self.submitted.append((account_id,user_id))
    async def stop(self, account_id, reason='', drain=True):
        assert self.app.tasks.contact_blocked(account_id), 'write protection before drain'
        self.stopped.append(account_id)


class FakeChats:
    def __init__(self): self.sent = []
    async def send_confirmed(self, account_id, chat_id, text, **kwargs):
        if kwargs.get('before_submit'): kwargs['before_submit']()
        self.sent.append((account_id, chat_id, text, kwargs))
        return {'status': 'sent', 'id': kwargs['request_id']}
    async def action(self, name, data): return {'status': 'sent'}


class FakeApp:
    def __init__(self, root, store=None):
        self.root, self.store = Path(root), store or MemoryStore()
        self.global_paused = False
        self.sessions = {a: SimpleNamespace(id=a, own_id=i, client=FakeClient(i),
                                           logged_in=True, status='online')
                         for a, i in [('a', 100), ('b', 200)]}
        self.accounts = SimpleNamespace(get=lambda a: self.sessions[a], require=self.require,
                                        public=lambda: [{'id': a, 'name': a.upper()} for a in self.sessions])
        for a in self.sessions:
            self.store.put('accounts', a, {'id': a, 'name': a.upper(), 'telegram_id': self.sessions[a].own_id})
        self.listeners = FakeListeners(self)
        self.chats = FakeChats()
        self.events, self.revision = [], 0
    def require(self, a):
        s = self.sessions[a]
        if not s.logged_in: raise ValueError('账号未登录')
        return s
    def bump(self): self.revision += 1
    def log(self, account_id, action, detail, **fields): self.events.append((account_id, action, detail, fields))
    async def execute(self, a, operation, *, priority=10, automation=True):
        if self.global_paused and automation: raise ValueError('全局已暂停')
        return await operation()
    def automation_allowed(self, a): return not self.global_paused
    def wait_state(self, a): return {}


class ContactTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(ContactService, '缺少 ContactService 实现')
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.app = FakeApp(self.tmp.name)
        self.service = self.app.contacts = ContactService(self.app)
        self.app.tasks = SimpleNamespace(contact_blocked=lambda a: False)

    async def test_refresh_includes_original_contacts_preserves_metadata_and_account_scope(self):
        await self.service.refresh('a')
        await self.service.action('update_contact', {'account_id':'a','user_id':1,'tags':['客户'],
                                                   'note':'已核对','consent_record':'同意测试群','consent_group_id':-9})
        result = await self.service.refresh('a')
        self.assertEqual({r['user_id'] for r in result if r.get('user_id')}, {1,2})
        self.assertEqual(next(r for r in result if r.get('user_id')==1)['tags'], ['客户'])
        self.assertEqual(self.service.cached('b'), [])
        self.assertTrue(next(r for r in result if r.get('user_id')==1)['consent_at'])

    async def test_deleted_contacts_are_visible_ineligible_and_not_lost(self):
        self.app.sessions['a'].client.users[3] = user(3, deleted=True)
        rows = await self.service.refresh('a')
        deleted = next(r for r in rows if r.get('user_id') == 3)
        self.assertFalse(deleted['eligible'])
        self.assertTrue(deleted['exclusion_reason'])

    async def test_unknown_saved_number_is_visible_and_scope_is_not_fabricated(self):
        client = self.app.sessions['a'].client
        client.saved = [types.SavedPhoneContact('15550009999','未知','',None)]
        rows = await self.service.refresh('a')
        self.assertEqual(len(rows), 3)
        self.assertFalse(self.service.scope_info('a')['complete'])
        self.assertIsNone(self.service.scope_info('a')['unregistered_count'])
        self.assertTrue(any(not r['eligible'] and r.get('phone') == '15550009999' for r in rows))

    async def test_saved_read_failure_keeps_registered_contacts_and_capability_error(self):
        self.app.sessions['a'].client.saved_error = RuntimeError('takeout unavailable')
        rows = await self.service.refresh('a')
        self.assertEqual(len(rows), 2)
        self.assertIsNone(self.service.scope_info('a')['saved_numbers_count'])
        self.assertIn('不支持完整清空', self.service.scope_info('a')['error'])

    async def test_csv_preview_has_strict_headers_aliases_duplicates_and_no_write(self):
        data = '账号,姓名,手机号,用户名,标签,备注,同意邀请的记录\na,甲,+15550000001,,客户,备注,电话同意\na,甲,+15550000001,,客户,,\nb,坏,xxx,,,,\n'
        result = await self.service.action('preview_import', {'account_id':'a','csv':data})
        self.assertEqual(len(result['rows']), 1)
        self.assertEqual(len(result['duplicates']), 1)
        self.assertEqual(len(result['invalid']), 1)
        self.assertEqual(self.app.sessions['a'].client.mutations, [])
        with self.assertRaises(ValueError):
            await self.service.action('preview_import', {'account_id':'a','csv':'name,extra\nA,B'})

    async def test_add_uses_no_phone_privacy_exception_and_metadata_is_visible(self):
        await self.service.add_person('a', user(6), source='listener')
        request = self.app.sessions['a'].client.mutations[0]
        self.assertEqual(request.phone, '')
        self.assertFalse(request.add_phone_privacy_exception)
        self.assertEqual(next(r for r in self.service.cached('a') if r['user_id']==6)['source'], 'listener')

    async def test_add_rechecks_cleanup_gate_at_rpc_boundary(self):
        self.app.tasks.contact_blocked = lambda a: True
        with self.assertRaises(ValueError): await self.service.add_person('a', user(6))
        self.assertEqual(self.app.sessions['a'].client.mutations, [])

    async def test_resolve_does_not_accept_foreign_input_access_hash(self):
        with self.assertRaises(ValueError):
            await self.service.resolve_person('a', {'user_id':1,'access_hash':987})
        resolved = await self.service.resolve_person('b', {'user_id':1})
        self.assertEqual(resolved.id, 1)

    async def test_listener_rechecks_permission_inside_scheduled_rpc_and_marks_submission(self):
        self.app.listeners.allowed=False
        with self.assertRaises(ValueError): await self.service.add_person('a',user(9),source='listener')
        self.assertEqual(self.app.sessions['a'].client.mutations,[])
        self.assertEqual(self.app.listeners.submitted,[])
        self.app.listeners.allowed=True
        await self.service.add_person('a',user(9),source='listener')
        self.assertEqual(self.app.listeners.submitted,[('a',9)])

    async def test_failed_add_does_not_create_an_eligible_selected_contact(self):
        client=self.app.sessions['a'].client
        client.block=asyncio.Event()
        pending=asyncio.create_task(self.service.add_person('a',user(9)))
        for _ in range(4): await asyncio.sleep(0)
        row=next(r for r in self.service.cached('a') if r['user_id']==9)
        self.assertFalse(row['eligible'])
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError): await pending
        row=next(r for r in self.service.cached('a') if r['user_id']==9)
        self.assertFalse(row['eligible'])

    async def test_add_preserves_metadata_changed_while_waiting_scheduler_and_rpc(self):
        await self.service.refresh('a')
        client=self.app.sessions['a'].client
        client.block=asyncio.Event()
        pending=asyncio.create_task(self.service.add_person('a',user(1)))
        for _ in range(4): await asyncio.sleep(0)
        await self.service.action('update_contact',{'account_id':'a','user_id':1,'tags':['新标签'],
            'note':'新的备注','consent_record':'新的同意记录'})
        client.block.set()
        await pending
        row=next(r for r in self.service.cached('a') if r.get('user_id')==1)
        self.assertEqual(row['note'],'新的备注')
        self.assertEqual(row['tags'],['新标签'])
        self.assertEqual(row['consent_record'],'新的同意记录')
        self.assertEqual(row['source'],'original')

    async def test_self_identity_is_rejected_even_when_user_flag_is_absent(self):
        with self.assertRaises(ValueError): await self.service.resolve_person('a',{'user_id':100})
        with self.assertRaises(ValueError): await self.service.add_person('a',user(100))

    async def test_new_phone_resolves_read_only_without_adding_to_existing_contacts(self):
        # get_entity('+phone') cannot resolve a new number in Telethon 1.45.
        person = types.User(9, access_hash=901, first_name='新联系人', phone='15550000009')
        client = PhoneClient(types.contacts.ResolvedPeer(types.PeerUser(9), [], [person]))
        self.app.sessions['a'].client = client
        before = copy.deepcopy(self.app.store.values)
        try:
            result = await self.service.resolve_person('a', {'phone': '+1 (555) 000-0009'})
        except ValueError as error:
            self.fail(f'新号码只读解析尚不可用：{error}')
        self.assertIs(result, person)
        self.assertEqual(client.phone_requests, ['15550000009'])
        self.assertEqual(client.entity_queries, [])
        self.assertEqual(client.mutations, [])
        self.assertEqual(self.app.store.values, before)
        self.assertNotIn(9, client.users)

    async def test_phone_resolver_rejects_malformed_peer_without_fallback_to_other_data(self):
        person = user(9, phone='15550000009')
        for response in (
            None,
            SimpleNamespace(peer=types.PeerUser(9), users=[person], chats=[]),
            types.contacts.ResolvedPeer(types.PeerChat(9), [], [person]),
            types.contacts.ResolvedPeer(types.PeerChannel(9), [], [person]),
            types.contacts.ResolvedPeer(types.PeerUser(True), [], [person]),
            types.contacts.ResolvedPeer(types.PeerUser(-9), [], [person]),
            types.contacts.ResolvedPeer(types.PeerUser(2 ** 53), [], [person]),
        ):
            with self.subTest(response=type(response).__name__, peer=getattr(response, 'peer', None)):
                client = PhoneClient(response)
                self.app.sessions['a'].client = client
                with self.assertRaises(ValueError):
                    await self.service.resolve_person('a', {'phone': '15550000009'})
                self.assertEqual(client.phone_requests, ['15550000009'])
                self.assertEqual(client.entity_queries, [])
                self.assertEqual(client.mutations, [])

    async def test_phone_resolver_requires_one_matching_user_identity(self):
        person = user(9, phone='15550000009')
        for users in ([], [user(8)], [person, person], [person, types.UserEmpty(9)],
                      [types.UserEmpty(9)], [SimpleNamespace(id=9, access_hash=90)], None):
            with self.subTest(users=users):
                client = PhoneClient(types.contacts.ResolvedPeer(types.PeerUser(9), [], users))
                self.app.sessions['a'].client = client
                with self.assertRaises(ValueError):
                    await self.service.resolve_person('a', {'phone': '15550000009'})
                self.assertEqual(client.phone_requests, ['15550000009'])
                self.assertEqual(client.mutations, [])

    async def test_phone_resolver_rejects_incomplete_unsafe_or_different_phone_user(self):
        for person in (
            user(9, min=True), user(9, deleted=True), user(9, bot=True), user(9, is_self=True),
            user(100), types.User(9, access_hash=None), types.User(9, access_hash=0),
            types.User(9, access_hash=True), types.User(9, access_hash=2 ** 63),
            user(9, phone='15550000008'), user(9, phone='1555+0000009'),
            user(9, phone=15550000009),
        ):
            with self.subTest(person=person.to_dict()):
                client = PhoneClient(types.contacts.ResolvedPeer(types.PeerUser(person.id), [], [person]))
                self.app.sessions['a'].client = client
                with self.assertRaises(ValueError):
                    await self.service.resolve_person('a', {'phone': '15550000009'})
                self.assertEqual(client.phone_requests, ['15550000009'])
                self.assertEqual(client.mutations, [])

    async def test_phone_mapping_uses_matching_user_and_never_chat_or_unrelated_user(self):
        person = user(9, phone='+1 (555) 000-0009')
        client = PhoneClient(types.contacts.ResolvedPeer(types.PeerUser(9), [self.app.sessions['a'].client.group],
                                                       [user(8), person]))
        self.app.sessions['a'].client = client
        try:
            result = await self.service.resolve_person('a', {'phone': '15550000009'})
        except ValueError as error:
            self.fail(f'未按本次号码映射选择唯一用户：{error}')
        self.assertIs(result, person)
        self.assertEqual(client.entity_queries, [])
        self.assertEqual(client.mutations, [])

    async def test_phone_mapping_with_private_phone_still_requires_complete_user(self):
        person = user(9)
        client = PhoneClient(types.contacts.ResolvedPeer(types.PeerUser(9), [], [person]))
        self.app.sessions['a'].client = client
        try:
            result = await self.service.resolve_person('a', {'phone': '15550000009'})
        except ValueError as error:
            self.fail(f'有效只读映射不应依赖可选的phone字段：{error}')
        self.assertIs(result, person)
        self.assertEqual(client.mutations, [])

    async def test_phone_resolution_is_separate_per_account_and_never_reuses_hash(self):
        first = types.User(9, access_hash=901, first_name='同一用户')
        second = types.User(9, access_hash=902, first_name='同一用户')
        a = PhoneClient(types.contacts.ResolvedPeer(types.PeerUser(9), [], [first]))
        b = PhoneClient(types.contacts.ResolvedPeer(types.PeerUser(9), [], [second]))
        self.app.sessions['a'].client, self.app.sessions['b'].client = a, b
        try:
            resolved_a = await self.service.resolve_person('a', {'phone': '15550000009'})
            self.assertEqual(b.phone_requests, [])
            resolved_b = await self.service.resolve_person('b', {'phone': '15550000009'})
        except ValueError as error:
            self.fail(f'分账号手机号解析尚不可用：{error}')
        self.assertEqual((resolved_a.access_hash, resolved_b.access_hash), (901, 902))
        self.assertEqual(a.phone_requests, ['15550000009'])
        self.assertEqual(b.phone_requests, ['15550000009'])
        self.assertEqual(a.mutations + b.mutations, [])

    async def test_phone_resolution_rechecks_client_and_own_identity_after_await(self):
        for change in ('client', 'identity'):
            with self.subTest(change=change):
                response = types.contacts.ResolvedPeer(types.PeerUser(9), [], [user(9)])
                def replace():
                    if change == 'client': self.app.sessions['a'].client = PhoneClient(response)
                    else: self.app.sessions['a'].own_id = 999
                client = PhoneClient(response, before_result=replace)
                self.app.sessions['a'].client = client
                self.app.sessions['a'].own_id = 100
                with self.assertRaises(ValueError):
                    await self.service.resolve_person('a', {'phone': '15550000009'})
                self.assertEqual(client.phone_requests, ['15550000009'])
                self.assertEqual(client.mutations, [])

    async def test_unregistered_phone_is_a_read_only_user_facing_failure(self):
        client = PhoneClient(error=errors.PhoneNotOccupiedError(None))
        self.app.sessions['a'].client = client
        with self.assertRaises(ValueError):
            await self.service.resolve_person('a', {'phone': '15550000009'})
        self.assertEqual(client.phone_requests, ['15550000009'])
        self.assertEqual(client.mutations, [])

    async def test_invalid_phone_input_is_rejected_before_any_telegram_read(self):
        response = types.contacts.ResolvedPeer(types.PeerUser(9), [], [user(9, phone='15550000009')])
        for value in (15550000009, ' ', '+', '1555+0000009'):
            with self.subTest(value=value):
                client = PhoneClient(response)
                self.app.sessions['a'].client = client
                with self.assertRaises(ValueError):
                    await self.service.resolve_person('a', {'phone': value})
                self.assertEqual(client.requests, [])
                self.assertEqual(client.entity_queries, [])
                self.assertEqual(client.mutations, [])

    async def test_username_resolution_keeps_current_account_entity_path(self):
        client = PhoneClient()
        client.users[1].username = 'known_user'
        self.app.sessions['a'].client = client
        result = await self.service.resolve_person('a', {'username': '@known_user', 'phone': '15550000009'})
        self.assertEqual(result.id, 1)
        self.assertEqual(client.entity_queries, ['known_user'])
        self.assertEqual(client.phone_requests, [])


if __name__ == '__main__': unittest.main()

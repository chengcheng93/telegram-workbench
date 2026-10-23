import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from telethon import errors, functions, types
from test_contacts import FakeApp, FakeClient, user

try:
    from contacts import ContactService
    from tasks import TaskService
except ImportError:
    ContactService = TaskService = None


class TaskTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(TaskService, '缺少 TaskService 实现')
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.app = FakeApp(self.tmp.name)
        self.app.contacts = ContactService(self.app)
        self.service = self.app.tasks = TaskService(self.app)
        self.addAsyncCleanup(self.service.close)
        await self.app.contacts.refresh('a')
        await self.app.contacts.refresh('b')

    async def prepare(self, type='invite', **kwargs):
        return (await self.service.action('prepare_task', {'account_id':'a','type':type,
                'user_ids':[1,2], 'group_id':-9, 'consent_confirmed':True, **kwargs}))['task']

    async def confirm(self, task):
        result = await self.service.action('confirm_task', {'account_id':task['account_id'],
                     'task_id':task['id'],'confirmation_token':task['token']})
        await self.settle()
        return result

    async def settle(self):
        for _ in range(25): await asyncio.sleep(0)

    def detail(self, task):
        return next(t for t in self.service.snapshot() if t['id']==task['id'])

    async def test_preview_is_read_only_and_confirmation_is_bound_to_account_and_token(self):
        task = await self.prepare()
        self.assertEqual(self.app.sessions['a'].client.mutations, [])
        for data in [{'account_id':'b','confirmation_token':task['token']},
                     {'account_id':'a','confirmation_token':'wrong'}]:
            with self.assertRaises(ValueError):
                await self.service.action('confirm_task', {'task_id':task['id'], **data})
        await self.confirm(task)
        self.assertEqual(self.detail(task)['counts']['invited'], 2)
        self.assertEqual(self.app.sessions['b'].client.mutations, [])

    async def test_confirmation_rejects_rebound_runtime_and_expiry(self):
        task = await self.prepare()
        self.app.sessions['a'].own_id = 999
        with self.assertRaises(ValueError): await self.confirm(task)
        self.app.sessions['a'].own_id = 100
        stored = self.app.store.get('tasks', task['id'], account_id='a')
        stored['expires_at'] = (datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
        self.app.store.put('tasks', task['id'], stored, account_id='a')
        with self.assertRaises(ValueError): await self.confirm(task)

    async def test_persisted_reservation_blocks_second_preview_and_success_across_tasks(self):
        first = await self.prepare()
        second = await self.prepare()
        self.assertEqual(second['counts']['remaining'], 0)
        await self.confirm(first)
        third = await self.prepare()
        self.assertEqual(third['counts']['already'], 2)
        self.assertEqual(third['counts']['remaining'], 0)

    async def test_membership_rechecked_at_execution_and_five_results_conserve_counts(self):
        client = self.app.sessions['a'].client
        client.users.update({i:user(i) for i in [3,4,5]})
        await self.app.contacts.refresh('a')
        task = await self.prepare(user_ids=[1,2,3,4,5])
        client.members.add(1)
        client.invite_errors[2] = errors.UserPrivacyRestrictedError(None)
        client.invite_errors[3] = errors.UserIdInvalidError(None)
        client.invite_unverified.add(4)
        await self.confirm(task)
        counts = self.detail(task)['counts']
        self.assertEqual({k:counts[k] for k in ['invited','already','restricted','unverified','failed']},
                         {'invited':1,'already':1,'restricted':1,'unverified':1,'failed':1})
        self.assertEqual(counts['processed'] + counts['remaining'], 5)
        self.assertEqual(sum(counts[k] for k in ['invited','already','restricted','unverified','failed']), 5)
        self.assertEqual(self.app.chats.sent, [])

    async def test_rpc_inflight_is_durable_before_write_and_restart_never_replays(self):
        task = await self.prepare()
        client = self.app.sessions['a'].client
        client.block = asyncio.Event()
        observed = []
        def before(_):
            items = self.app.store.list('task_items','a')
            observed.append(any(i['status']=='inflight' and i.get('submitted_at') for i in items))
        client.before_mutation = before
        await self.service.action('confirm_task', {'task_id':task['id'],'confirmation_token':task['token']})
        await self.settle()
        self.assertEqual(observed, [True])
        recovered = TaskService(self.app)
        self.assertEqual(recovered.snapshot()[0]['status'], 'paused')
        self.assertEqual(recovered.snapshot()[0]['counts']['unverified'], 1)
        self.assertEqual(len(client.mutations), 1)
        await recovered.close()
        client.block.set()

    async def test_fatal_network_error_pauses_batch_and_unknown_only_verifies(self):
        client = self.app.sessions['a'].client
        client.invite_errors[1] = ConnectionError('offline')
        task = await self.prepare()
        await self.confirm(task)
        state = self.detail(task)
        self.assertEqual(state['status'], 'paused')
        self.assertEqual(state['counts']['unverified'], 1)
        self.assertEqual(state['counts']['remaining'], 1)
        count = len(client.mutations)
        item = (await self.service.action('task_detail', {'task_id':task['id']}))['items'][0]
        await self.service.action('verify_task_item', {'task_id':task['id'],'item_id':item['id']})
        self.assertEqual(len(client.mutations), count)
        with self.assertRaises(ValueError):
            await self.service.action('retry_task', {'task_id':task['id'],'confirmed':True})

    async def test_resume_requires_fresh_checks_and_retries_only_definite_failed(self):
        client = self.app.sessions['a'].client
        client.invite_errors[1] = errors.UserIdInvalidError(None)
        task = await self.prepare()
        await self.confirm(task)
        self.assertEqual(self.detail(task)['counts']['failed'], 1)
        del client.invite_errors[1]
        await self.service.action('retry_task', {'task_id':task['id'],'confirmed':True})
        await self.settle()
        self.assertEqual(self.detail(task)['counts']['invited'], 2)
        self.assertEqual(len(client.mutations), 3)

    async def test_batch_messages_need_explicit_existing_selection_and_consent(self):
        for data in [{'user_ids':[999]}, {'user_ids':[]}, {'consent_confirmed':False}]:
            with self.assertRaises(ValueError): await self.prepare(type='send_message', text='hello', **data)
        task = await self.prepare(type='send_message', text='已确认的测试消息')
        await self.confirm(task)
        self.assertEqual(len(self.app.chats.sent), 2)
        ids = [r[3]['request_id'] for r in self.app.chats.sent]
        items = (await self.service.action('task_detail', {'task_id':task['id']}))['items']
        self.assertEqual(ids, [i['request_id'] for i in items])
        self.assertEqual(len(set(ids)),2)

    async def test_csv_export_escapes_formula_prefix(self):
        task = await self.prepare(type='send_invite_link', text='https://t.me/+explicit', name='=evil')
        out = await self.service.action('export_task', {'task_id':task['id']})
        self.assertNotIn(',=evil',out['csv'])
        self.assertIn("'=evil",out['csv'])

    async def test_cleanup_gate_before_drain_and_read_backup_then_explicit_confirm(self):
        task = (await self.service.action('prepare_clear', {'account_id':'a'}))['task']
        self.assertTrue(self.service.contact_blocked('a'))
        self.assertFalse(self.service.contact_blocked('b'))
        self.assertEqual(self.app.listeners.stopped, ['a'])
        self.assertEqual(task['registered_count'], 2)
        self.assertEqual(task['unregistered_count'], 0)
        self.assertEqual(task['backup_count'], 2)
        self.assertTrue(Path(task['backup_path']).is_file())
        self.assertEqual(self.app.sessions['a'].client.mutations, [])
        with self.assertRaises(ValueError):
            await self.service.action('confirm_clear', {'task_id':task['id'],'confirmation_token':task['token'],
                'account_name':'wrong','understood_backup':True,'confirmed_scope':True})
        await self.service.action('confirm_clear', {'task_id':task['id'],'confirmation_token':task['token'],
                'account_name':'A','understood_backup':True,'confirmed_scope':True})
        await self.settle()
        state = self.detail(task)
        self.assertEqual(state['status'], 'completed')
        self.assertTrue(state['verified'])
        self.assertEqual(state['counts']['deleted'], 2)
        self.assertEqual(state['counts']['remaining'], 0)
        self.assertFalse(self.service.contact_blocked('a'))
        self.assertEqual(self.app.sessions['b'].client.mutations, [])

    async def test_cleanup_rejects_unknown_saved_scope_and_releases_protection(self):
        self.app.sessions['a'].client.saved = [types.SavedPhoneContact('19999999999','未知','',None)]
        with self.assertRaisesRegex(ValueError, '不支持完整清空'):
            await self.service.action('prepare_clear', {'account_id':'a'})
        self.assertFalse(self.service.contact_blocked('a'))
        self.assertEqual(self.app.sessions['a'].client.mutations, [])

    async def test_cleanup_snapshot_change_or_backup_damage_never_deletes(self):
        task = (await self.service.action('prepare_clear', {'account_id':'a'}))['task']
        self.app.sessions['a'].client.users[6] = user(6)
        with self.assertRaises(ValueError):
            await self.service.action('confirm_clear', {'task_id':task['id'],'confirmation_token':task['token'],
                'account_name':'A','understood_backup':True,'confirmed_scope':True})
        self.assertEqual(self.app.sessions['a'].client.mutations, [])
        self.assertFalse(self.service.contact_blocked('a'))
        task = (await self.service.action('prepare_clear', {'account_id':'a'}))['task']
        Path(task['backup_path']).write_text('{}')
        with self.assertRaises(ValueError):
            await self.service.action('confirm_clear', {'task_id':task['id'],'confirmation_token':task['token'],
                'account_name':'A','understood_backup':True,'confirmed_scope':True})
        self.assertEqual(self.app.sessions['a'].client.mutations, [])

    async def test_cleanup_cancel_expiry_and_restart_release_without_listener_restart(self):
        task = (await self.service.action('prepare_clear', {'account_id':'a'}))['task']
        await self.service.action('cancel_clear', {'task_id':task['id']})
        self.assertFalse(self.service.contact_blocked('a'))
        task = (await self.service.action('prepare_clear', {'account_id':'a'}))['task']
        saved = self.app.store.get('tasks',task['id'],account_id='a')
        saved['expires_at']=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
        self.app.store.put('tasks',task['id'],saved,account_id='a')
        self.assertFalse(self.service.contact_blocked('a'))
        recovered = TaskService(self.app)
        self.assertFalse(recovered.contact_blocked('a'))
        await recovered.close()
        self.assertEqual(self.app.sessions['a'].client.mutations, [])

    async def test_clear_resume_reprepares_and_requires_full_form_not_generic_retry(self):
        task = (await self.service.action('prepare_clear', {'account_id':'a'}))['task']
        await self.service.action('pause_task', {'task_id':task['id']})
        with self.assertRaises(ValueError):
            await self.service.action('retry_task', {'task_id':task['id'],'confirmed':True})
        resumed=(await self.service.action('resume_task', {'task_id':task['id'],'confirmed':True}))['task']
        self.assertEqual(resumed['status'],'awaiting_confirmation')
        self.assertNotEqual(resumed['token'],task['token'])
        self.assertNotEqual(resumed['backup_path'],task['backup_path'])
        self.assertEqual(self.app.sessions['a'].client.mutations, [])

    async def test_cleanup_blocks_same_account_adds_but_manual_chat_remains_available(self):
        task = (await self.service.action('prepare_clear', {'account_id':'a'}))['task']
        with self.assertRaises(ValueError): await self.app.contacts.add_person('a',user(9))
        await self.app.contacts.add_person('b',user(9))
        await self.app.chats.send_confirmed('a',1,'人工聊天',request_id='manual')
        self.assertEqual(len(self.app.chats.sent),1)
        await self.service.action('cancel_clear',{'task_id':task['id']})

    async def test_unconfirmed_pause_or_recovery_cannot_bypass_confirmation(self):
        task=await self.prepare()
        await self.service.action('pause_task',{'task_id':task['id']})
        for name in ['resume_task','retry_task']:
            with self.assertRaises(ValueError):
                await self.service.action(name,{'task_id':task['id'],'confirmed':True})
        self.assertEqual(self.app.sessions['a'].client.mutations,[])

    async def test_cancelled_preview_alias_does_not_permanently_reserve_user(self):
        first=await self.prepare()
        await self.prepare()
        await self.service.action('cancel_task',{'task_id':first['id'],'confirmed':True})
        third=await self.prepare()
        self.assertEqual(third['counts']['remaining'],2)

    async def test_prepared_clear_releases_protection_after_account_removal(self):
        task=(await self.service.action('prepare_clear',{'account_id':'a'}))['task']
        account=self.app.store.get('accounts','a')
        account['deleted']=True
        self.app.store.put('accounts','a',account)
        self.assertFalse(self.service.contact_blocked('a'))
        self.assertEqual(self.detail(task)['status'],'cancelled')

    async def test_clear_rechecks_scope_between_confirmation_and_first_delete(self):
        task=(await self.service.action('prepare_clear',{'account_id':'a'}))['task']
        original=self.service._kick
        self.service._kick=lambda _:None
        await self.service.action('confirm_clear',{'task_id':task['id'],'confirmation_token':task['token'],
            'account_name':'A','understood_backup':True,'confirmed_scope':True})
        self.app.sessions['a'].client.users[6]=user(6)
        self.service._kick=original
        original('a')
        await self.settle()
        self.assertEqual(self.app.sessions['a'].client.mutations,[])
        self.assertEqual(self.detail(task)['status'],'paused')

    async def test_add_task_marks_inflight_at_actual_rpc_not_while_waiting_scheduler(self):
        queued=[]
        async def execute(a,operation,**kwargs):
            queued.append(operation)
            await asyncio.Future()
        self.app.execute=execute
        task=(await self.service.action('prepare_task',{'account_id':'a','type':'add_contacts','entries':[{'user_id':9}]}))['task']
        await self.service.action('confirm_task',{'task_id':task['id'],'confirmation_token':task['token']})
        await self.settle()
        items=(await self.service.action('task_detail',{'task_id':task['id']}))['items']
        self.service._workers['a'].cancel()
        await self.settle()
        self.assertEqual(items[0]['status'],'queued')

    async def test_runtime_target_permission_failure_pauses_whole_batch_without_failed_people(self):
        task=await self.prepare()
        client=self.app.sessions['a'].client
        old=client.get_permissions
        async def permission(target,person):
            if person=='me': raise ValueError('权限资料变化')
            return await old(target,person)
        client.get_permissions=permission
        await self.confirm(task)
        state=self.detail(task)
        self.assertEqual(state['status'],'paused')
        self.assertEqual(state['counts']['remaining'],2)
        self.assertEqual(state['counts']['failed'],0)
        self.assertEqual(client.mutations,[])

    async def test_scheduler_pause_is_batch_pause_and_preserves_pending(self):
        from scheduler import OperationPaused
        task=await self.prepare()
        async def execute(*args,**kwargs): raise OperationPaused('等待平台允许')
        self.app.execute=execute
        await self.confirm(task)
        state=self.detail(task)
        self.assertEqual(state['status'],'paused')
        self.assertEqual(state['counts']['remaining'],2)
        self.assertEqual(state['counts']['failed'],0)

    async def test_unknown_listener_drain_retains_protection_and_has_recoverable_task(self):
        async def stop(*args,**kwargs): raise ValueError('在途添加尚未结束')
        self.app.listeners.stop=stop
        self.app.listeners.snapshot=lambda a:{'state':'stopping'}
        before=len(self.app.sessions['a'].client.requests)
        with self.assertRaises(ValueError): await self.service.action('prepare_clear',{'account_id':'a'})
        self.assertTrue(self.service.contact_blocked('a'))
        tasks=self.service.snapshot('a')
        self.assertEqual(len(tasks),1)
        self.assertEqual(tasks[0]['status'],'paused')
        self.assertEqual(len(self.app.sessions['a'].client.requests),before)
        with self.assertRaises(ValueError): await self.service.action('cancel_clear',{'task_id':tasks[0]['id']})
        self.assertTrue(self.service.contact_blocked('a'))
        async def finished(*args,**kwargs): return None
        self.app.listeners.stop=finished
        self.app.listeners.snapshot=lambda a:{'state':'stopped'}
        await self.service.action('cancel_clear',{'task_id':tasks[0]['id']})
        self.assertFalse(self.service.contact_blocked('a'))

    async def test_normal_add_task_preserves_import_metadata_and_delete_only_affects_selected_contact(self):
        task=(await self.service.action('prepare_task',{'account_id':'a','type':'add_contacts',
            'entries':[{'user_id':9,'tags':['测试'],'note':'检查','consent_record':'已同意'}]}))['task']
        await self.confirm(task)
        self.assertEqual(self.detail(task)['counts']['added'],1)
        added=next(r for r in self.app.contacts.cached('a') if r.get('user_id')==9)
        self.assertEqual(added['tags'],['测试'])
        task=await self.prepare(type='delete_contacts',user_ids=[9])
        await self.confirm(task)
        self.assertEqual(self.detail(task)['counts']['deleted'],1)
        self.assertEqual(set(self.app.sessions['a'].client.users),{1,2})
        self.assertEqual(self.app.sessions['b'].client.mutations,[])

    async def test_cancel_during_clear_confirmation_read_cannot_restart_deletion(self):
        task=(await self.service.action('prepare_clear',{'account_id':'a'}))['task']
        client=self.app.sessions['a'].client
        client.read_gate,client.read_started=asyncio.Event(),asyncio.Event()
        confirmation=asyncio.create_task(self.service.action('confirm_clear',{'task_id':task['id'],
            'confirmation_token':task['token'],'account_name':'A','understood_backup':True,'confirmed_scope':True}))
        await client.read_started.wait()
        await self.service.action('cancel_clear',{'task_id':task['id']})
        client.read_gate.set()
        try: await confirmation
        except ValueError: pass
        await self.settle()
        self.assertEqual(client.mutations,[])
        self.assertEqual(self.detail(task)['status'],'cancelled')

    async def test_cancel_during_clear_preparation_read_does_not_reacquire_protection(self):
        client=self.app.sessions['a'].client
        client.read_gate,client.read_started=asyncio.Event(),asyncio.Event()
        preparing=asyncio.create_task(self.service.action('prepare_clear',{'account_id':'a'}))
        await client.read_started.wait()
        task=self.service.snapshot('a')[0]
        await self.service.action('cancel_clear',{'task_id':task['id']})
        client.read_gate.set()
        try: await preparing
        except ValueError: pass
        self.assertFalse(self.service.contact_blocked('a'))
        self.assertEqual(self.detail(task)['status'],'cancelled')
        self.assertFalse(list(Path(self.tmp.name).rglob('contacts-*.json')))

    async def test_cancel_during_resume_checks_does_not_submit_or_override_cancelled(self):
        task=await self.prepare()
        # Start from a confirmed safe paused boundary without a Telegram write.
        await self.service.action('confirm_task',{'task_id':task['id'],'confirmation_token':task['token']})
        await self.service.action('pause_task',{'task_id':task['id']})
        await self.settle()
        started,gate=asyncio.Event(),asyncio.Event()
        async def authorized():
            started.set()
            await gate.wait()
            return True
        self.app.sessions['a'].client.is_user_authorized=authorized
        resuming=asyncio.create_task(self.service.action('resume_task',{'task_id':task['id'],'confirmed':True}))
        await started.wait()
        await self.service.action('cancel_task',{'task_id':task['id'],'confirmed':True})
        gate.set()
        try: await resuming
        except ValueError: pass
        await self.settle()
        self.assertEqual(self.detail(task)['status'],'cancelled')
        self.assertEqual(self.app.sessions['a'].client.mutations,[])

    async def test_reset_saved_rechecks_scope_inside_actual_scheduler_dispatch(self):
        task=(await self.service.action('prepare_clear',{'account_id':'a'}))['task']
        client=self.app.sessions['a'].client
        original=self.app.execute
        submitted=0
        async def execute(account,operation,**kwargs):
            nonlocal submitted
            submitted+=1
            if submitted==3:
                client.saved.append(types.SavedPhoneContact('19999999999','外部新增','',None))
            return await original(account,operation,**kwargs)
        self.app.execute=execute
        await self.service.action('confirm_clear',{'task_id':task['id'],'confirmation_token':task['token'],
            'account_name':'A','understood_backup':True,'confirmed_scope':True})
        await self.settle()
        self.assertFalse(any(isinstance(r,functions.contacts.ResetSavedRequest) for r in client.mutations))
        self.assertEqual(client.saved[0].phone,'19999999999')
        self.assertEqual(self.detail(task)['status'],'paused')

    async def test_message_task_waiting_for_rpc_keeps_pending_and_retry_has_explicit_fixed_key(self):
        task=await self.prepare(type='send_message',text='确认消息',user_ids=[1])
        called=[]
        async def queued(*args,**kwargs):
            called.append(kwargs)
            return {'id':'outbox-one','status':'queued'}
        self.app.chats.send_confirmed=queued
        await self.confirm(task)
        self.assertEqual(self.detail(task)['counts']['remaining'],1)
        self.assertEqual(self.detail(task)['counts']['unverified'],0)
        self.assertEqual(self.detail(task)['status'],'paused')

    async def test_message_retry_is_explicit_and_reuses_original_request_id(self):
        calls=[]
        async def sender(*args,**kwargs):
            kwargs['before_submit']()
            calls.append(kwargs)
            return {'id':'outbox-one','status':'failed' if len(calls)==1 else 'sent'}
        self.app.chats.send_confirmed=sender
        task=await self.prepare(type='send_message',text='确认消息',user_ids=[1])
        await self.confirm(task)
        await self.service.action('retry_task',{'task_id':task['id'],'confirmed':True})
        await self.settle()
        self.assertEqual([c['retry_failed'] for c in calls],[False,True])
        self.assertEqual(calls[0]['request_id'],calls[1]['request_id'])
        self.assertEqual([(c.get('priority'),c.get('automation')) for c in calls],[(20,True),(20,True)])
        self.assertEqual(self.detail(task)['counts']['sent'],1)

    async def test_crashed_message_item_matches_canonical_outbox_request_id_without_resend(self):
        import uuid
        task=await self.prepare(type='send_message',text='确认消息',user_ids=[1])
        item=(await self.service.action('task_detail',{'task_id':task['id']}))['items'][0]
        item.update(status='unverified',kind='unverified')
        self.app.store.put('task_items',item['id'],item,account_id='a')
        self.app.store.put('outbox','known',{'id':'known','account_id':'a',
            'request_id':str(uuid.UUID(item['request_id'])),'status':'sent','chat_id':1,'text':task['text'],
            'task_id':task['id'],'item_id':item['id'],'reply_to':None,'attachment_ids':[]},account_id='a')
        result=await self.service.action('verify_task_item',{'task_id':task['id'],'item_id':item['id']})
        self.assertEqual(result['item']['status'],'sent')
        self.assertEqual(self.app.chats.sent,[])

    async def test_paused_clear_preview_expires_and_releases_protection(self):
        task=(await self.service.action('prepare_clear',{'account_id':'a'}))['task']
        await self.service.action('pause_task',{'task_id':task['id']})
        saved=self.app.store.get('tasks',task['id'],account_id='a')
        saved['expires_at']=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
        self.app.store.put('tasks',task['id'],saved,account_id='a')
        self.assertFalse(self.service.contact_blocked('a'))

    async def test_clear_scope_is_rechecked_when_first_delete_reaches_scheduler(self):
        task=(await self.service.action('prepare_clear',{'account_id':'a'}))['task']
        client=self.app.sessions['a'].client
        original=self.app.execute
        changed=False
        async def execute(account,operation,**kwargs):
            nonlocal changed
            if not changed:
                client.users[6]=user(6)
                changed=True
            return await original(account,operation,**kwargs)
        self.app.execute=execute
        await self.service.action('confirm_clear',{'task_id':task['id'],'confirmation_token':task['token'],
            'account_name':'A','understood_backup':True,'confirmed_scope':True})
        await self.settle()
        self.assertEqual(client.mutations,[])
        self.assertEqual(self.detail(task)['status'],'paused')

    async def test_malformed_or_foreign_import_entry_is_a_read_only_validation_error(self):
        for entries in [['bad'],[{'account':'b','user_id':9}]]:
            error=None
            try: await self.service.action('prepare_task',{'account_id':'a','type':'add_contacts','entries':entries})
            except Exception as actual: error=actual
            self.assertIsInstance(error,ValueError)
        self.assertEqual(self.app.sessions['a'].client.mutations,[])

    async def test_clear_check_exposes_latest_remaining_scope_without_replaying(self):
        task=(await self.service.action('prepare_clear',{'account_id':'a'}))['task']
        checked=(await self.service.action('check_task',{'task_id':task['id']}))['task']
        self.assertEqual(checked.get('checked_scope',{}).get('registered_count'),2)
        self.assertEqual(checked.get('checked_fingerprint'),task['fingerprint'])
        self.assertEqual(self.app.sessions['a'].client.mutations,[])

    def real_message_service(self):
        from chats import ChatService
        from test_chats import msg
        self.message_attempts=[]
        self.message_errors=[]
        async def send(chat_id,text,**kwargs):
            self.message_attempts.append((chat_id,text,kwargs))
            if self.message_errors:
                error=self.message_errors.pop(0)
                if error: raise error
            return msg(100+len(self.message_attempts),chat_id,text,True)
        self.app.sessions['a'].client.send_message=send
        self.app.chats=ChatService(self.app)
        self.addAsyncCleanup(self.app.chats.close)
        return self.app.chats

    async def test_cancelled_task_message_cannot_be_retried_or_reclaimed_from_chat(self):
        chats=self.real_message_service()
        self.message_errors=[errors.UserIsBlockedError(None)]
        task=await self.prepare(type='send_message',text='仅任务中心控制',user_ids=[1])
        await self.confirm(task)
        item=(await self.service.action('task_detail',{'task_id':task['id']}))['items'][0]
        outbox=self.app.store.list('outbox','a')[0]
        self.assertEqual((outbox.get('task_id'),outbox.get('item_id')),(task['id'],item['id']))
        self.assertEqual(self.detail(task)['counts']['failed'],1)
        await self.service.action('cancel_task',{'task_id':task['id'],'confirmed':True})
        self.app.global_paused=True
        for name,data in [('retry_message',{'outbox_id':outbox['id']}),
                          ('cancel_message',{'outbox_id':outbox['id']}),
                          ('send_message',{'chat_id':1,'text':task['text'],'request_id':item['request_id'],
                                           'task_id':task['id'],'item_id':item['id']})]:
            with self.assertRaises(ValueError): await chats.action(name,dict(account_id='a',**data))
        checked=await chats.action('check_message',{'account_id':'a','outbox_id':outbox['id']})
        self.assertEqual(checked['status'],'failed')
        self.assertEqual(len(self.message_attempts),1)
        self.assertEqual(self.detail(task)['status'],'cancelled')
        self.assertEqual(self.detail(task)['counts']['failed'],1)

    async def test_task_message_retry_owns_same_outbox_and_updates_conserved_counts(self):
        chats=self.real_message_service()
        self.message_errors=[errors.UserIsBlockedError(None)]
        task=await self.prepare(type='send_invite_link',text='https://t.me/+explicit-task',user_ids=[1])
        await self.confirm(task)
        before=self.app.store.list('outbox','a')[0]
        self.assertEqual(self.detail(task)['counts']['failed'],1)
        await self.service.action('retry_task',{'task_id':task['id'],'confirmed':True})
        await self.settle()
        items=(await self.service.action('task_detail',{'task_id':task['id']}))['items']
        rows=self.app.store.list('outbox','a')
        self.assertEqual(len(rows),1)
        self.assertEqual((rows[0]['id'],rows[0]['request_id']),(before['id'],before['request_id']))
        self.assertEqual((rows[0].get('task_id'),rows[0].get('item_id')),(task['id'],items[0]['id']))
        self.assertEqual(items[0]['outbox_id'],rows[0]['id'])
        self.assertEqual(items[0]['status'],'sent')
        counts=self.detail(task)['counts']
        self.assertEqual({k:counts[k] for k in ['total','processed','remaining','failed','sent']},
                         {'total':1,'processed':1,'remaining':0,'failed':0,'sent':1})
        self.assertEqual(len(self.message_attempts),2)
        with self.assertRaises(ValueError):
            await chats.action('retry_message',{'account_id':'a','outbox_id':rows[0]['id']})
        self.assertEqual(len(self.message_attempts),2)

    async def test_queued_task_outbox_stays_blocked_after_task_cancel_and_global_pause(self):
        from scheduler import OperationPaused
        chats=self.real_message_service()
        original=self.app.execute
        async def blocked(*args,**kwargs): raise OperationPaused('尚未提交')
        self.app.execute=blocked
        task=await self.prepare(type='send_message',text='排队任务',user_ids=[1])
        await self.confirm(task)
        row=self.app.store.list('outbox','a')[0]
        self.assertEqual(row['status'],'queued')
        self.assertEqual(row.get('task_id'),task['id'])
        await self.service.action('cancel_task',{'task_id':task['id'],'confirmed':True})
        self.app.execute=original
        self.app.global_paused=True
        with self.assertRaises(ValueError):
            await chats.action('send_message',{'account_id':'a','chat_id':1,'text':task['text'],'request_id':row['request_id']})
        self.assertEqual(self.message_attempts,[])
        self.assertEqual(self.detail(task)['counts']['remaining'],1)

    async def test_recovery_binds_legacy_task_outbox_before_chat_can_replay_it(self):
        from scheduler import OperationPaused
        chats=self.real_message_service()
        original=self.app.execute
        async def blocked(*args,**kwargs): raise OperationPaused('尚未提交')
        self.app.execute=blocked
        task=await self.prepare(type='send_message',text='旧版本排队任务',user_ids=[1])
        await self.confirm(task)
        row=self.app.store.list('outbox','a')[0]
        row.pop('task_id',None);row.pop('item_id',None)
        row['request_id']=row['request_id'].replace('-','')
        self.app.store.put('outbox',row['id'],row,account_id='a')
        recovered=TaskService(self.app)
        self.addAsyncCleanup(recovered.close)
        self.app.tasks=recovered
        self.app.execute=original
        bound=self.app.store.get('outbox',row['id'],account_id='a')
        items=(await recovered.action('task_detail',{'task_id':task['id']}))['items']
        self.assertEqual((bound.get('task_id'),bound.get('item_id')),(task['id'],items[0]['id']))
        with self.assertRaises(ValueError):
            await chats.action('send_message',{'account_id':'a','chat_id':1,'text':task['text'],'request_id':row['request_id']})
        self.assertEqual(self.message_attempts,[])
        await recovered.action('resume_task',{'task_id':task['id'],'confirmed':True})
        await self.settle()
        self.assertEqual(len(self.app.store.list('outbox','a')),1)
        self.assertEqual(self.app.store.list('outbox','a')[0]['id'],row['id'])
        self.assertEqual(len(self.message_attempts),1)
        self.assertEqual(recovered.snapshot('a')[0]['counts']['sent'],1)

    async def test_recovery_quarantines_conflicting_legacy_task_outbox(self):
        from scheduler import OperationPaused
        chats=self.real_message_service()
        async def blocked(*args,**kwargs): raise OperationPaused('尚未提交')
        self.app.execute=blocked
        task=await self.prepare(type='send_message',text='旧任务正文',user_ids=[1])
        await self.confirm(task)
        row=self.app.store.list('outbox','a')[0]
        row.pop('task_id',None);row.pop('item_id',None)
        row['text']='与任务不一致'
        self.app.store.put('outbox',row['id'],row,account_id='a')
        recovered=TaskService(self.app)
        self.addAsyncCleanup(recovered.close)
        self.app.tasks=recovered
        quarantined=self.app.store.get('outbox',row['id'],account_id='a')
        self.assertTrue(quarantined.get('task_ownership_error'))
        with self.assertRaises(ValueError):
            await chats.action('send_message',{'account_id':'a','chat_id':1,'text':row['text'],'request_id':row['request_id']})
        state=recovered.snapshot('a')[0]
        self.assertEqual(state['counts']['unverified'],1)
        self.assertEqual(self.message_attempts,[])

    async def test_add_server_error_after_apply_is_unverified_and_only_read_checks_success(self):
        class AppliedThenServerError(FakeClient):
            async def __call__(self,request):
                result=await super().__call__(request)
                if isinstance(request,functions.contacts.AddContactRequest):
                    raise errors.ServerError(request,'RPC_CALL_FAIL',500)
                return result
        client=self.app.sessions['a'].client=AppliedThenServerError(100)
        task=(await self.service.action('prepare_task',{'account_id':'a','type':'add_contacts',
            'entries':[{'user_id':9}]}))['task']
        await self.confirm(task)
        self.assertEqual(self.detail(task)['counts']['unverified'],1)
        self.assertEqual(self.detail(task)['counts']['failed'],0)
        with self.assertRaises(ValueError):
            await self.service.action('retry_task',{'task_id':task['id'],'confirmed':True})
        item=(await self.service.action('task_detail',{'task_id':task['id']}))['items'][0]
        checked=await self.service.action('verify_task_item',{'task_id':task['id'],'item_id':item['id']})
        self.assertEqual(checked['item']['status'],'added')
        self.assertEqual(len(client.mutations),1)

    async def test_delete_unknown_rpc_after_apply_is_not_a_definite_failure_or_retryable(self):
        class AppliedThenUnknownRPC(FakeClient):
            async def __call__(self,request):
                result=await super().__call__(request)
                if isinstance(request,functions.contacts.DeleteContactsRequest):
                    raise errors.RPCError(request,'UNEXPLAINED',0)
                return result
        client=self.app.sessions['a'].client=AppliedThenUnknownRPC(100)
        task=await self.prepare(type='delete_contacts',user_ids=[1])
        await self.confirm(task)
        self.assertEqual(self.detail(task)['counts']['unverified'],1)
        self.assertEqual(self.detail(task)['counts']['failed'],0)
        with self.assertRaises(ValueError):
            await self.service.action('retry_task',{'task_id':task['id'],'confirmed':True})
        item=(await self.service.action('task_detail',{'task_id':task['id']}))['items'][0]
        result=await self.service.action('verify_task_item',{'task_id':task['id'],'item_id':item['id']})
        self.assertEqual(result['item']['status'],'deleted')
        self.assertEqual(len(client.mutations),1)

    async def test_clear_server_error_is_unknown_until_final_read_proves_absence(self):
        observed=[]
        app=self.app
        class AppliedThenServerError(FakeClient):
            raised=False
            async def __call__(self,request):
                result=await super().__call__(request)
                if isinstance(request,functions.contacts.DeleteContactsRequest) and not self.raised:
                    self.raised=True
                    raise errors.ServerError(request,'RPC_CALL_FAIL',500)
                if isinstance(request,functions.contacts.GetContactsRequest) and self.raised:
                    observed.append(next(row for row in app.store.list('task_items','a') if row['user_id']==1)['status'])
                return result
        client=self.app.sessions['a'].client=AppliedThenServerError(100)
        task=(await self.service.action('prepare_clear',{'account_id':'a'}))['task']
        await self.service.action('confirm_clear',{'task_id':task['id'],'confirmation_token':task['token'],
            'account_name':'A','understood_backup':True,'confirmed_scope':True})
        await self.settle()
        self.assertEqual(observed[0],'unverified')
        self.assertEqual(self.detail(task)['counts']['failed'],0)
        self.assertEqual(self.detail(task)['counts']['deleted'],2)
        self.assertTrue(self.detail(task)['verified'])
        self.assertEqual(sum(isinstance(r,functions.contacts.DeleteContactsRequest) for r in client.mutations),2)

    async def test_unknown_rpc_before_submission_pauses_with_pending_instead_of_failed_person(self):
        client=self.app.sessions['a'].client
        task=await self.prepare(type='delete_contacts',user_ids=[1])
        async def unresolved(peer): raise errors.ServerError(None,'RPC_CALL_FAIL',500)
        client.get_entity=unresolved
        await self.confirm(task)
        state=self.detail(task)
        self.assertEqual(state['status'],'paused')
        self.assertEqual(state['counts']['remaining'],1)
        self.assertEqual(state['counts']['failed'],0)
        self.assertEqual(client.mutations,[])

    async def test_username_mapping_change_cannot_replace_the_previewed_add_recipient(self):
        client=self.app.sessions['a'].client
        original=client.get_entity
        mapping=[user(3,username='target_name')]
        async def resolve(peer):
            if peer=='target_name': return mapping[0]
            return await original(peer)
        client.get_entity=resolve
        task=(await self.service.action('prepare_task',{'account_id':'a','type':'add_contacts',
            'entries':[{'username':'target_name'}]}))['task']
        mapping[0]=user(4,username='target_name')
        await self.confirm(task)
        items=(await self.service.action('task_detail',{'task_id':task['id']}))['items']
        self.assertEqual(items[0]['user_id'],3)
        self.assertEqual(items[0]['status'],'failed')
        self.assertEqual(client.mutations,[])
        self.assertEqual(self.detail(task)['counts']['added'],0)

    async def test_resume_checks_reject_changed_phone_or_username_mapping_before_any_write(self):
        client=self.app.sessions['a'].client
        original=client.get_entity
        mapping=[user(3,username='target_name')]
        async def resolve(peer):
            if peer=='target_name': return mapping[0]
            return await original(peer)
        client.get_entity=resolve
        task=(await self.service.action('prepare_task',{'account_id':'a','type':'add_contacts',
            'entries':[{'username':'target_name'}]}))['task']
        await self.service.action('confirm_task',{'task_id':task['id'],'confirmation_token':task['token']})
        await self.service.action('pause_task',{'task_id':task['id']})
        await self.settle()
        mapping[0]=user(4,username='target_name')
        with self.assertRaises(ValueError):
            await self.service.action('resume_task',{'task_id':task['id'],'confirmed':True})
        self.assertEqual(client.mutations,[])

    async def test_new_phone_mapping_change_cannot_silently_add_a_different_user(self):
        class MappingPhoneClient(FakeClient):
            identifier=3
            access_hash=30
            async def __call__(self,request):
                if isinstance(request,functions.contacts.ResolvePhoneRequest):
                    self.requests.append(request)
                    person=types.User(self.identifier,access_hash=self.access_hash,first_name='号码对象',phone=request.phone)
                    return types.contacts.ResolvedPeer(types.PeerUser(self.identifier),[],[person])
                return await super().__call__(request)
        client=self.app.sessions['a'].client=MappingPhoneClient(100)
        task=(await self.service.action('prepare_task',{'account_id':'a','type':'add_contacts',
            'entries':[{'phone':'+15550009999'}]}))['task']
        client.identifier=4
        client.access_hash=40
        await self.confirm(task)
        self.assertEqual(client.mutations,[])
        items=(await self.service.action('task_detail',{'task_id':task['id']}))['items']
        self.assertEqual((items[0]['user_id'],items[0]['status']),(3,'failed'))
        # A fresh preview binds the new identity, but execution still resolves its
        # latest reference from this account rather than retaining an old hash.
        next_task=(await self.service.action('prepare_task',{'account_id':'a','type':'add_contacts',
            'entries':[{'phone':'+15550009999'}]}))['task']
        client.access_hash=4444
        await self.confirm(next_task)
        self.assertEqual(self.detail(next_task)['counts']['added'],1)
        self.assertEqual(client.mutations[0].id.access_hash,4444)

    async def test_delete_postcheck_read_rejection_does_not_mean_mutation_failed(self):
        class PostcheckRejected(FakeClient):
            deleted=False
            rejected=False
            async def __call__(self,request):
                if isinstance(request,functions.contacts.GetContactsRequest) and self.deleted and not self.rejected:
                    self.rejected=True
                    raise errors.UserIdInvalidError(request)
                result=await super().__call__(request)
                if isinstance(request,functions.contacts.DeleteContactsRequest): self.deleted=True
                return result
        client=self.app.sessions['a'].client=PostcheckRejected(100)
        task=await self.prepare(type='delete_contacts',user_ids=[1])
        await self.confirm(task)
        self.assertEqual(self.detail(task)['counts']['unverified'],1)
        self.assertEqual(self.detail(task)['counts']['failed'],0)
        with self.assertRaises(ValueError):
            await self.service.action('retry_task',{'task_id':task['id'],'confirmed':True})
        self.assertEqual(len(client.mutations),1)


if __name__ == '__main__': unittest.main()

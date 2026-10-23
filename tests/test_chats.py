"""Chat boundary behavior. All Telegram operations use deterministic local fakes."""
import asyncio
import copy
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch

try:
    from chats import ChatService
except ModuleNotFoundError:
    ChatService = None


class MemoryStore:
    def __init__(self): self.rows = {}
    def get(self, kind, key, default=None, *, account_id=''):
        return copy.deepcopy(self.rows.get((kind, account_id, str(key)), default))
    def put(self, kind, key, value, *, account_id=''):
        self.rows[kind, account_id, str(key)] = copy.deepcopy(value)
    def delete(self, kind, key, *, account_id=''):
        self.rows.pop((kind, account_id, str(key)), None)
    def list(self, kind, account_id=None):
        return [copy.deepcopy(v) for (k, a, _), v in sorted(self.rows.items()) if k == kind and (account_id is None or a == account_id)]


def msg(mid=1, chat_id=7, text='hello', out=False, **kw):
    return NS(id=mid, chat_id=chat_id, text=text, message=text,
              date=datetime(2026, 10, 22, tzinfo=timezone.utc), out=out,
              reply_to=None, media=None, file=None, photo=None, document=None, **kw)


class Client:
    def __init__(self):
        self.sent = []; self.handlers = []; self.history = []; self.error = None
        self.dialogs = []; self.edit_error = None; self.downloads = 0; self.gate = None; self.deleted = []
    def add_event_handler(self, callback, builder): self.handlers.append((callback, builder))
    def remove_event_handler(self, callback, builder=None):
        self.handlers = [(c,b) for c,b in self.handlers if c is not callback]
    async def get_dialogs(self, limit=100): return self.dialogs
    async def get_messages(self, chat_id, **kw):
        if 'ids' in kw:
            return next((m for m in self.history if m.id == kw['ids'] and m.chat_id == chat_id), None)
        before = kw.get('max_id', 0)
        return [m for m in self.history if m.chat_id == chat_id and (not before or m.id < before)][:kw.get('limit', 50)]
    async def send_message(self, chat_id, text, **kw):
        self.sent.append((chat_id, text, kw))
        if self.gate: await self.gate.wait()
        if self.error: raise self.error
        return msg(100 + len(self.sent), chat_id, text, True)
    async def send_file(self, chat_id, file, **kw):
        callback = kw['progress_callback']
        await callback(2, 4)
        if self.gate: await self.gate.wait()
        await callback(4, 4)
        self.sent.append((chat_id, kw.get('caption'), {'file': file, **kw}))
        return msg(100 + len(self.sent), chat_id, kw.get('caption') or '', True)
    async def edit_message(self, chat_id, message_id, text, **kw):
        if self.edit_error: raise self.edit_error
        return msg(message_id, chat_id, text, True)
    async def delete_messages(self, chat_id, ids, revoke=True):
        self.deleted.append((chat_id,ids)); return []
    async def get_permissions(self, chat_id, who): return NS(delete_messages=False, is_admin=False, is_creator=False)
    async def download_media(self, message, file, progress_callback=None):
        self.downloads += 1
        Path(file).write_bytes(b'image')
        return str(file)


class App:
    def __init__(self, root, store=None):
        self.root = Path(root); self.store = store or MemoryStore(); self.revision = 0
        self.clients = {a: Client() for a in ('a', 'b')}
        self.sessions = {a: NS(id=a, client=c, own_id=500, logged_in=True, status='online') for a,c in self.clients.items()}
        self.accounts = NS(get=self.sessions.get, require=self.require, sessions=self.sessions)
        self.executions = []; self.logs = []
        self.listeners = NS(handle_event=lambda *a, **k: (_ for _ in ()).throw(AssertionError('history reached listener')))
    def require(self, account_id):
        session = self.sessions.get(account_id)
        if not session or not session.logged_in: raise ValueError('账号离线')
        return session
    async def execute(self, account_id, operation, *, priority=10, automation=True):
        self.executions.append((account_id, priority, automation))
        return await operation()
    def bump(self): self.revision += 1
    def log(self, *args, **kw): self.logs.append((args, kw)); self.bump()


class ChatTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(ChatService, 'ChatService 尚未实现')
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.app = App(self.temp.name); self.service = ChatService(self.app)
        self.addAsyncCleanup(self.service.close)
    async def action(self, name, **kw):
        return await self.service.action(name, {'account_id':'a', **kw})

    async def test_drafts_and_message_keys_cannot_cross_account_or_chat(self):
        await self.action('save_draft', chat_id=7, text='A7', reply_to=2)
        await self.action('save_draft', account_id='b', chat_id=7, text='B7')
        await self.action('save_draft', chat_id=8, text='A8')
        self.assertEqual((await self.action('get_draft', chat_id=7))['text'], 'A7')
        self.assertEqual((await self.action('get_draft', account_id='b', chat_id=7))['text'], 'B7')
        self.app.clients['a'].history = [msg(1, 7, 'A')]
        self.app.clients['b'].history = [msg(1, 7, 'B')]
        await self.action('messages', chat_id=7)
        await self.action('messages', account_id='b', chat_id=7)
        self.assertEqual(self.app.store.get('messages', '7:1', account_id='a')['text'], 'A')
        self.assertEqual(self.app.store.get('messages', '7:1', account_id='b')['text'], 'B')

    async def test_local_search_filters_use_cached_message_content_without_network(self):
        for a,title,unread,group in [('a','项目群',2,True),('b','朋友',0,False)]:
            self.app.clients[a].dialogs = [NS(id=7, title=title, message=msg(1,7,'最近'), date=None, unread_count=unread, is_group=group, is_channel=False, pinned=True, dialog=NS(notify_settings=NS(mute_until=None)))]
            await self.service.action('refresh_chats', {'account_id': a})
        self.app.clients['a'].history = [msg(9,7,'历史关键词')]
        await self.action('messages', chat_id=7)
        result = await self.service.action('list_chats', {'query':'关键词','filter':'unread'})
        self.assertEqual([(r['account_id'],r['id']) for r in result['dialogs']], [('a',7)])
        self.assertEqual(len((await self.service.action('list_chats', {'filter':'private'}))['dialogs']), 1)

    async def test_history_pagination_never_calls_listener(self):
        self.app.clients['a'].history = [msg(i,7,str(i)) for i in range(60,0,-1)]
        first = await self.action('messages', chat_id=7)
        self.assertEqual(len(first['messages']), 50)
        self.assertTrue(first['has_more'])
        older = await self.action('messages', chat_id=7, before_id=11)
        self.assertEqual([m['id'] for m in older['messages']], list(range(1,11)))
        self.assertFalse(older['has_more'])

    async def test_ready_is_idempotent_and_live_new_edit_delete_update_cache(self):
        await self.service.ready('a'); await self.service.ready('a')
        self.assertEqual(len(self.app.clients['a'].handlers), 3)
        callbacks = [c for c,_ in self.app.clients['a'].handlers]
        await callbacks[0](NS(message=msg(4,7,'live'), chat_id=7))
        await callbacks[0](NS(message=msg(4,7,'live'), chat_id=7))
        dialogs = (await self.action('list_chats'))['dialogs']
        self.assertEqual(dialogs[0]['unread'], 1)
        await callbacks[1](NS(message=msg(4,7,'edited'), chat_id=7))
        self.assertEqual(self.app.store.get('messages','7:4',account_id='a')['text'], 'edited')
        await callbacks[2](NS(deleted_ids=[4], chat_id=7))
        self.assertEqual(self.app.store.get('messages','7:4',account_id='a')['status'], 'deleted')
        await self.service.offline('a')
        self.assertEqual(self.app.clients['a'].handlers, [])

    async def test_send_is_persisted_before_rpc_and_request_id_is_immutable(self):
        request_id = str(uuid.uuid4())
        original = self.app.clients['a'].send_message
        async def checked(chat_id, text, **kw):
            rows = self.app.store.list('outbox','a')
            self.assertEqual(rows[0]['status'], 'inflight')
            return await original(chat_id,text,**kw)
        self.app.clients['a'].send_message = checked
        await self.action('save_draft',chat_id=7,text='hello')
        sent = await self.action('send_message',chat_id=7,text='hello',reply_to=3,request_id=request_id)
        again = await self.action('send_message',chat_id=7,text='hello',reply_to=3,request_id=request_id)
        self.assertEqual(sent['id'],again['id']); self.assertEqual(sent['status'],'sent')
        self.assertEqual(len(self.app.clients['a'].sent),1)
        self.assertEqual(self.app.executions,[('a',0,False)])
        with self.assertRaises(ValueError):
            await self.action('send_message',chat_id=8,text='hello',reply_to=3,request_id=request_id)
        other = await self.action('send_message',account_id='b',chat_id=7,text='other',request_id=request_id)
        self.assertEqual(other['status'],'sent')

    async def test_simultaneous_duplicate_send_has_single_network_mutation(self):
        key=str(uuid.uuid4()); self.app.clients['a'].gate=asyncio.Event()
        pending=asyncio.create_task(self.action('send_message',chat_id=7,text='one',request_id=key))
        await asyncio.sleep(0)
        duplicate=await self.action('send_message',chat_id=7,text='one',request_id=key)
        self.assertEqual(duplicate['status'],'inflight')
        self.app.clients['a'].gate.set(); result=await pending
        self.assertEqual(result['status'],'sent'); self.assertEqual(len(self.app.clients['a'].sent),1)

    async def test_ambiguous_send_retains_draft_and_cannot_be_retried(self):
        self.app.clients['a'].error=TimeoutError('private server text')
        await self.action('save_draft',chat_id=7,text='keep me')
        result=await self.action('send_message',chat_id=7,text='keep me',request_id=str(uuid.uuid4()))
        self.assertEqual(result['status'],'unverified')
        self.assertEqual((await self.action('get_draft',chat_id=7))['text'],'keep me')
        with self.assertRaises(ValueError): await self.action('retry_message',outbox_id=result['id'])
        checked=await self.action('check_message',outbox_id=result['id'])
        self.assertEqual(checked['status'],'unverified'); self.assertNotIn('private server text',str(checked))
        self.assertEqual(len(self.app.clients['a'].sent),1)

    async def test_check_message_can_verify_known_message_id_without_resending(self):
        row={'id':'x','account_id':'a','chat_id':7,'request_id':str(uuid.uuid4()),'text':'done','status':'unverified','message_ids':[90]}
        self.app.store.put('outbox','x',row,account_id='a')
        self.app.clients['a'].history=[msg(90,7,'done',True)]
        result=await self.action('check_message',outbox_id='x')
        self.assertEqual(result['status'],'sent'); self.assertEqual(self.app.clients['a'].sent,[])

    async def test_definite_rejection_can_retry_same_outbox_without_changing_scope(self):
        class ChatWriteForbiddenError(Exception): pass
        self.app.clients['a'].error=ChatWriteForbiddenError()
        result=await self.action('send_message',chat_id=7,text='retry',request_id=str(uuid.uuid4()))
        self.assertEqual(result['status'],'failed')
        self.app.clients['a'].error=None
        retried=await self.action('retry_message',outbox_id=result['id'])
        self.assertEqual(retried['status'],'sent'); self.assertEqual(retried['id'],result['id'])
        with self.assertRaises(ValueError): await self.action('check_message',account_id='b',outbox_id=result['id'])

    async def test_offline_and_restart_never_replay_outbox(self):
        self.app.sessions['a'].logged_in=False
        result=await self.action('send_message',chat_id=7,text='later',request_id=str(uuid.uuid4()))
        self.assertEqual(result['status'],'queued')
        self.app.sessions['a'].logged_in=True
        await self.service.ready('a')
        restarted=ChatService(self.app); self.addAsyncCleanup(restarted.close)
        await restarted.ready('a')
        self.assertEqual(self.app.clients['a'].sent,[])
        cancelled=await self.action('cancel_message',outbox_id=result['id'])
        self.assertEqual(cancelled['status'],'cancelled')
        inflight={**result,'id':'crash','status':'inflight'}
        self.app.store.put('outbox','crash',inflight,account_id='a')
        recovered=ChatService(self.app); self.addAsyncCleanup(recovered.close)
        self.assertEqual(self.app.store.get('outbox','crash',account_id='a')['status'],'unverified')

    async def test_attachment_is_private_scoped_and_upload_path_is_not_browser_controlled(self):
        attachment=await self.service.upload('a','../../secret.png',b'png')
        self.assertNotIn('path',attachment); self.assertEqual(attachment['name'],'secret.png')
        with self.assertRaises(ValueError):
            await self.action('send_message',account_id='b',chat_id=7,text='',attachment_ids=[attachment['id']],request_id=str(uuid.uuid4()))
        result=await self.action('send_message',chat_id=7,text='caption',attachment_ids=[attachment['id']],request_id=str(uuid.uuid4()))
        self.assertEqual(result['status'],'sent'); self.assertEqual(result['progress'],100)
        self.assertNotIn(self.temp.name,str(result))
        with self.assertRaises(ValueError): await self.service.upload('a','big',b'0'*(25*1024*1024+1))

    async def test_partial_multifile_error_cannot_retry_the_whole_batch(self):
        first=await self.service.upload('a','one.txt',b'one')
        second=await self.service.upload('a','two.txt',b'two')
        class ChatWriteForbiddenError(Exception): pass
        async def partially_sent(chat_id,file,**kwargs):
            self.app.clients['a'].sent.append((chat_id,'one',{}))
            raise ChatWriteForbiddenError()
        self.app.clients['a'].send_file=partially_sent
        result=await self.action('send_message',chat_id=7,text='',attachment_ids=[first['id'],second['id']],request_id=str(uuid.uuid4()))
        self.assertEqual(result['status'],'unverified')
        with self.assertRaises(ValueError): await self.action('retry_message',outbox_id=result['id'])
        self.assertEqual(len(self.app.clients['a'].sent),1)

    async def test_attachment_disk_failure_returns_chinese_without_filesystem_path(self):
        with patch('chats.os.open',side_effect=OSError('secret-path-permission-error')):
            try:
                await self.service.upload('a','x.txt',b'x')
            except Exception as error:
                self.assertIsInstance(error,ValueError)
                self.assertNotIn('secret-path',str(error))
                self.assertTrue(any('\u4e00'<=c<='\u9fff' for c in str(error)))
            else:
                self.fail('磁盘写入失败时不应显示附件已就绪')

    async def test_lost_queued_attachment_is_failed_locally_without_network_submission(self):
        attachment=await self.service.upload('a','lost.txt',b'data')
        key=str(uuid.uuid4()); self.app.sessions['a'].logged_in=False
        queued=await self.action('send_message',chat_id=7,text='',attachment_ids=[attachment['id']],request_id=key)
        metadata=self.app.store.get('messages','attachment:'+attachment['id'],account_id='a')
        Path(metadata['path']).unlink(); self.app.sessions['a'].logged_in=True
        result=await self.action('send_message',chat_id=7,text='',attachment_ids=[attachment['id']],request_id=key)
        self.assertEqual(result['status'],'failed')
        self.assertIn('附件',result['error'])
        self.assertEqual(self.app.clients['a'].sent,[])

    async def test_message_capabilities_conservatively_hide_expired_edit_and_group_delete(self):
        old=msg(1,-7,'old',True); old.date=datetime(2020,1,1,tzinfo=timezone.utc)
        self.app.clients['a'].history=[old,msg(2,-7,'someone else')]
        messages=(await self.action('messages',chat_id=-7))['messages']
        self.assertFalse(messages[0]['can_edit'])
        self.assertFalse(messages[1]['can_delete'])
        self.assertTrue(messages[0]['can_delete'])

    async def test_restricted_admin_without_delete_right_cannot_revoke_others_group_message(self):
        self.app.clients['a'].history=[msg(1,-7,'someone else')]
        async def rights(chat,who): return NS(delete_messages=False,is_admin=True,is_creator=False)
        self.app.clients['a'].get_permissions=rights
        with self.assertRaises(ValueError):
            await self.action('delete_message',chat_id=-7,message_id=1,confirmed=True)
        self.assertEqual(self.app.clients['a'].deleted,[])

    async def test_late_history_cannot_overwrite_live_edit_or_resurrect_live_delete(self):
        await self.service.ready('a'); callbacks=[c for c,_ in self.app.clients['a'].handlers]
        for event_type in ('edit','delete'):
            with self.subTest(event_type=event_type):
                initial=msg(40,7,'old history')
                await callbacks[0](NS(message=initial,chat_id=7))
                started=asyncio.Event(); finish=asyncio.Event()
                async def delayed(chat_id,**kwargs):
                    started.set(); await finish.wait(); return [initial]
                self.app.clients['a'].get_messages=delayed
                loading=asyncio.create_task(self.action('messages',chat_id=7)); await started.wait()
                if event_type=='edit': await callbacks[1](NS(message=msg(40,7,'fresh edit'),chat_id=7))
                else: await callbacks[2](NS(deleted_ids=[40],chat_id=7))
                finish.set(); result=await loading
                if event_type=='edit': self.assertEqual(result['messages'][0]['text'],'fresh edit')
                else: self.assertEqual(result['messages'][0]['status'],'deleted')

    async def test_live_delete_before_history_is_cached_creates_tombstone(self):
        await self.service.ready('a'); callbacks=[c for c,_ in self.app.clients['a'].handlers]
        started=asyncio.Event(); finish=asyncio.Event()
        async def delayed(chat_id,**kwargs):
            started.set(); await finish.wait(); return [msg(80,7,'already deleted')]
        self.app.clients['a'].get_messages=delayed
        loading=asyncio.create_task(self.action('messages',chat_id=7)); await started.wait()
        await callbacks[2](NS(deleted_ids=[80],chat_id=7)); finish.set()
        result=await loading
        self.assertEqual(result['messages'][0]['status'],'deleted')
        self.assertEqual(result['messages'][0]['text'],'')

    async def test_unscoped_private_delete_before_first_history_remains_deleted_after_restart(self):
        await self.service.ready('a'); callbacks=[c for c,_ in self.app.clients['a'].handlers]
        await callbacks[2](NS(deleted_ids=[88],chat_id=None))
        restarted=ChatService(self.app); self.addAsyncCleanup(restarted.close)
        self.app.clients['a'].history=[msg(88,7,'deleted private'),msg(88,-1000000000007,'channel independent')]
        private=await restarted.action('messages',{'account_id':'a','chat_id':7})
        channel=await restarted.action('messages',{'account_id':'a','chat_id':-1000000000007})
        self.assertEqual(private['messages'][0]['status'],'deleted')
        self.assertEqual(channel['messages'][0]['status'],'sent')

    async def test_confirmed_task_journal_callback_runs_only_at_real_rpc_boundary(self):
        recorded=[]; original=self.app.clients['a'].send_message
        async def journal(): recorded.append('submitted')
        async def rpc(chat_id,text,**kwargs):
            self.assertEqual(recorded,['submitted'])
            self.assertEqual(self.app.store.list('outbox','a')[0]['status'],'inflight')
            return await original(chat_id,text,**kwargs)
        self.app.clients['a'].send_message=rpc
        key=str(uuid.uuid4())
        try:
            result=await self.service.send_confirmed('a',7,'task',request_id=key,before_submit=journal)
        except TypeError as error:
            self.fail('缺少真实RPC前的任务journal回调：'+str(error))
        self.assertEqual(result['status'],'sent')
        await self.service.send_confirmed('a',7,'task',request_id=key,before_submit=journal)
        self.assertEqual(recorded,['submitted'])
        self.app.sessions['a'].logged_in=False
        offline=await self.service.send_confirmed('a',7,'offline',request_id=str(uuid.uuid4()),before_submit=journal)
        self.assertEqual(offline['status'],'queued')
        self.assertEqual(recorded,['submitted'])

    async def test_confirmed_task_retry_is_explicit_and_reuses_immutable_request_once(self):
        class ChatWriteForbiddenError(Exception): pass
        self.app.clients['a'].error=ChatWriteForbiddenError()
        key=str(uuid.uuid4())
        failed=await self.service.send_confirmed('a',7,'task retry',request_id=key)
        self.assertEqual(failed['status'],'failed')
        self.app.clients['a'].error=None
        unchanged=await self.service.send_confirmed('a',7,'task retry',request_id=key)
        self.assertEqual(unchanged['status'],'failed')
        self.assertEqual(len(self.app.clients['a'].sent),1)
        journal=[]
        try:
            retried=await self.service.send_confirmed('a',7,'task retry',request_id=key,retry_failed=True,before_submit=lambda:journal.append('inflight'))
        except TypeError as error:
            self.fail('缺少仅对已确认失败任务开放的重试参数：'+str(error))
        self.assertEqual(retried['status'],'sent'); self.assertEqual(retried['id'],failed['id'])
        repeated=await self.service.send_confirmed('a',7,'task retry',request_id=key,retry_failed=True)
        self.assertEqual(repeated['id'],failed['id'])
        self.assertEqual(len(self.app.clients['a'].sent),2); self.assertEqual(journal,['inflight'])
        with self.assertRaises(ValueError):
            await self.service.send_confirmed('a',8,'changed',request_id=key,retry_failed=True)

    async def test_successful_dialog_history_and_live_sync_report_explicit_account_now(self):
        synced=[]
        def mark_synced(account_id):
            synced.append(account_id)
            profile=self.app.store.get('accounts',account_id,{})
            profile['last_sync']='current sync time'
            self.app.store.put('accounts',account_id,profile)
        self.app.mark_synced=mark_synced
        ancient=msg(1,7,'old history'); ancient.date=datetime(2020,1,1,tzinfo=timezone.utc)
        self.app.clients['a'].dialogs=[NS(id=7,title='同步',message=ancient,date=ancient.date,unread_count=0,is_group=False,is_channel=False,pinned=False,dialog=None)]
        await self.action('refresh_chats')
        self.assertEqual(synced,['a'])
        self.assertEqual(self.app.store.get('accounts','a')['last_sync'],'current sync time')
        await self.action('list_chats')
        self.assertEqual(synced,['a'])
        self.app.clients['a'].history=[ancient]
        await self.action('messages',chat_id=7)
        self.assertEqual(synced,['a','a'])
        self.assertTrue(self.app.store.get('messages','7:1',account_id='a')['date'].startswith('2020-01-01'))
        await self.service.ready('b'); callbacks=[c for c,_ in self.app.clients['b'].handlers]
        await callbacks[0](NS(message=msg(2,7,'new'),chat_id=7))
        await callbacks[1](NS(message=msg(2,7,'edited'),chat_id=7))
        await callbacks[2](NS(deleted_ids=[2],chat_id=7))
        self.assertEqual(synced,['a','a','b','b','b'])
        self.app.sessions['a'].logged_in=False
        await self.action('messages',chat_id=7)
        self.assertEqual(synced,['a','a','b','b','b'])

    async def test_confirmed_task_uses_automation_queue_and_global_pause_blocks_queued_rpc(self):
        from scheduler import AccountScheduler
        self.app.global_paused=False
        self.app.wait_state=lambda account_id: {'blocked':False}
        self.app.record_wait=lambda account_id,error: None
        scheduler=AccountScheduler(self.app); self.addAsyncCleanup(scheduler.close)
        async def execute(account_id,operation,*,priority=10,automation=True):
            self.app.executions.append((account_id,priority,automation))
            return await scheduler.execute(account_id,operation,priority=priority,automation=automation)
        self.app.execute=execute
        started=asyncio.Event(); release=asyncio.Event(); submitted=[]
        async def blocker():
            started.set(); await release.wait()
        blocking=asyncio.create_task(scheduler.execute('a',blocker,priority=20,automation=False))
        await started.wait()
        async def task_send():
            return await self.service.send_confirmed('a',7,'automatic task',request_id=str(uuid.uuid4()),priority=20,automation=True,before_submit=lambda:submitted.append('rpc'))
        pending=asyncio.create_task(task_send())
        try:
            await asyncio.sleep(0)
            if pending.done():
                try: await pending
                except TypeError as error: self.fail('缺少任务队列属性参数：'+str(error))
            self.app.global_paused=True
            release.set(); await blocking
            result=await pending
            self.assertEqual(result['status'],'queued')
            self.assertEqual(submitted,[])
            self.assertEqual(self.app.clients['a'].sent,[])
            # Browser-provided queue flags cannot override the manual defaults.
            manual=await self.action('send_message',chat_id=7,text='manual',request_id=str(uuid.uuid4()),priority=20,automation=True)
            self.assertEqual(manual['status'],'sent')
            self.assertEqual([item[1] for item in self.app.clients['a'].sent],['manual'])
            self.assertEqual(self.app.executions,[('a',20,True),('a',0,False)])
        finally:
            release.set()
            await asyncio.gather(blocking,pending,return_exceptions=True)

    async def test_task_owned_outbox_blocks_browser_resume_retry_cancel_and_owner_forgery(self):
        key=str(uuid.uuid4()); task_id='1'*32; item_id='2'*32
        self.app.sessions['a'].logged_in=False
        try:
            queued=await self.service.send_confirmed('a',7,'batch',request_id=key,task_id=task_id,item_id=item_id,priority=20,automation=True,before_submit=lambda:None)
        except TypeError as error:
            self.fail('缺少持久任务消息归属参数：'+str(error))
        self.assertEqual((queued['task_id'],queued['item_id']),(task_id,item_id))
        self.app.sessions['a'].logged_in=True
        restarted=ChatService(self.app); self.addAsyncCleanup(restarted.close)
        for status in ('queued','failed','unverified'):
            row=self.app.store.get('outbox',queued['id'],account_id='a'); row['status']=status
            self.app.store.put('outbox',row['id'],row,account_id='a')
            for action in ('send_message','retry_message','cancel_message'):
                with self.subTest(status=status,action=action):
                    with self.assertRaisesRegex(ValueError,'任务中心'):
                        await restarted.action(action,{'account_id':'a','chat_id':7,'text':'batch','request_id':key,'outbox_id':row['id'],'task_id':task_id,'item_id':item_id,'priority':20,'automation':True,'retry_failed':True})
        self.assertEqual(self.app.clients['a'].sent,[])
        checked=await restarted.action('check_message',{'account_id':'a','outbox_id':queued['id']})
        self.assertEqual(checked['status'],'unverified')
        self.assertEqual(self.app.clients['a'].sent,[])

    async def test_task_retry_requires_same_owner_and_boundary_and_preserves_manual_draft(self):
        class ChatWriteForbiddenError(Exception): pass
        key=str(uuid.uuid4()); task_id='3'*32; item_id='4'*32
        self.app.clients['a'].error=ChatWriteForbiddenError()
        params={'request_id':key,'task_id':task_id,'item_id':item_id,'priority':20,'automation':True,'before_submit':lambda:None}
        try:
            failed=await self.service.send_confirmed('a',7,'same text',**params)
        except TypeError as error:
            self.fail('缺少持久任务消息归属参数：'+str(error))
        self.assertEqual(failed['status'],'failed')
        self.app.clients['a'].error=None
        for change in ({'task_id':'5'*32},{'item_id':'6'*32},{'priority':0,'automation':False},{'before_submit':None}):
            with self.subTest(change=change):
                with self.assertRaises(ValueError):
                    await self.service.send_confirmed('a',7,'same text',**{**params,**change,'retry_failed':True})
        await self.action('save_draft',chat_id=7,text='same text')
        submitted=[]; params['before_submit']=lambda:submitted.append('journal')
        sent=await self.service.send_confirmed('a',7,'same text',**params,retry_failed=True)
        self.assertEqual(sent['status'],'sent'); self.assertEqual(sent['id'],failed['id'])
        self.assertEqual(submitted,['journal']); self.assertEqual(len(self.app.clients['a'].sent),2)
        self.assertEqual((await self.action('get_draft',chat_id=7))['text'],'same text')
        self.assertEqual(self.app.logs[-1][0][1],'task_message')

    async def test_json_cannot_create_task_owned_outbox_and_ownership_conflict_cannot_dispatch(self):
        manual=await self.action('send_message',chat_id=7,text='manual',request_id=str(uuid.uuid4()),task_id='7'*32,item_id='8'*32,automation=True,priority=20)
        self.assertFalse(manual.get('task_id')); self.assertFalse(manual.get('item_id'))
        row={'id':'bad-owner','account_id':'a','chat_id':7,'request_id':str(uuid.uuid4()),'text':'blocked','status':'queued','message_ids':[],'task_ownership_error':'任务消息归属存在冲突'}
        self.app.store.put('outbox',row['id'],row,account_id='a')
        for action in ('retry_message','cancel_message'):
            with self.assertRaisesRegex(ValueError,'任务中心'):
                await self.action(action,outbox_id=row['id'])
        with self.assertRaises(ValueError): await self.service._dispatch(row)
        self.assertEqual([entry[1] for entry in self.app.clients['a'].sent],['manual'])

    async def test_sent_document_keeps_user_visible_filename(self):
        attachment=await self.service.upload('a','原始报告.txt',b'report')
        await self.action('send_message',chat_id=7,text='',attachment_ids=[attachment['id']],request_id=str(uuid.uuid4()))
        outgoing=self.app.clients['a'].sent[0][2]['file']
        self.assertEqual(getattr(outgoing,'name',None),'原始报告.txt')

    async def test_upload_cancellation_stops_pending_media_and_reports_unknown_if_rpc_started(self):
        attachment=await self.service.upload('a','x.txt',b'x')
        self.app.clients['a'].gate=asyncio.Event()
        sending=asyncio.create_task(self.action('send_message',chat_id=7,text='',attachment_ids=[attachment['id']],request_id=str(uuid.uuid4())))
        await asyncio.sleep(0)
        row=self.app.store.list('outbox','a')[0]
        cancelled=await self.action('cancel_message',outbox_id=row['id'])
        self.assertEqual(cancelled['status'],'unverified')
        self.app.clients['a'].gate.set()
        final=await sending
        self.assertEqual(final['status'],'unverified'); self.assertEqual(self.app.clients['a'].sent,[])

    async def test_media_download_checks_current_account_message_and_safe_cache(self):
        self.app.clients['a'].history=[msg(2,7,'image')]
        result=await self.service.media('a',7,2)
        self.assertEqual(Path(result['path']).read_bytes(),b'image')
        self.assertTrue(Path(result['path']).resolve().is_relative_to(self.app.root.resolve()))
        with self.assertRaises(ValueError): await self.service.media('b',7,2)
        self.assertEqual(self.app.clients['a'].downloads,1)

    async def test_media_symlink_escape_is_rejected_before_telegram_read(self):
        attachment=await self.service.upload('a','x.txt',b'x')
        metadata=self.app.store.get('messages','attachment:'+attachment['id'],account_id='a')
        path=Path(metadata['path']); path.unlink(); outside=self.app.root/'outside'; outside.write_bytes(b'secret'); path.symlink_to(outside)
        with self.assertRaises(ValueError):
            await self.action('send_message',chat_id=7,text='',attachment_ids=[attachment['id']],request_id=str(uuid.uuid4()))
        self.assertEqual(self.app.clients['a'].sent,[])

    async def test_edit_requires_own_message_and_delete_requires_confirmation(self):
        self.app.clients['a'].history=[msg(1,7,'theirs'),msg(2,7,'mine',True)]
        with self.assertRaises(ValueError): await self.action('edit_message',chat_id=7,message_id=1,text='bad')
        edited=await self.action('edit_message',chat_id=7,message_id=2,text='new')
        self.assertEqual(edited['message']['text'],'new')
        with self.assertRaises(ValueError): await self.action('delete_message',chat_id=7,message_id=2)
        deleted=await self.action('delete_message',chat_id=7,message_id=2,confirmed=True)
        self.assertTrue(deleted['deleted'])
        self.assertTrue(all(p==0 and not auto for _,p,auto in self.app.executions))

    async def test_takeover_cancels_only_unsent_ai_drafts_in_this_chat(self):
        for key,a,chat,status in [('x','a',7,'draft'),('y','b',7,'draft'),('z','a',8,'draft'),('used','a',7,'adopted')]:
            self.app.store.put('ai_drafts',key,{'id':key,'account_id':a,'chat_id':chat,'status':status,'type':'reply'},account_id=a)
        result=await self.action('takeover_chat',chat_id=7,enabled=True)
        self.assertTrue(result['enabled'])
        self.assertEqual(self.app.store.get('ai_drafts','x',account_id='a')['status'],'cancelled')
        self.assertEqual(self.app.store.get('ai_drafts','y',account_id='b')['status'],'draft')
        self.assertEqual(self.app.store.get('ai_drafts','z',account_id='a')['status'],'draft')
        self.assertEqual(self.app.store.get('ai_drafts','used',account_id='a')['status'],'adopted')
        self.assertEqual(self.app.clients['a'].sent,[])


    async def test_unscoped_delete_event_does_not_delete_channel_message_with_same_number(self):
        await self.service.ready('a')
        callbacks=[c for c,_ in self.app.clients['a'].handlers]
        await callbacks[0](NS(message=msg(4,7,'private'),chat_id=7))
        await callbacks[0](NS(message=msg(4,-1000000000007,'channel'),chat_id=-1000000000007))
        await callbacks[2](NS(deleted_ids=[4],chat_id=None))
        self.assertEqual(self.app.store.get('messages','7:4',account_id='a')['status'],'deleted')
        self.assertEqual(self.app.store.get('messages','-1000000000007:4',account_id='a')['status'],'sent')

    async def test_successful_idempotent_request_survives_media_cache_removal(self):
        attachment=await self.service.upload('a','original.txt',b'document')
        key=str(uuid.uuid4())
        sent=await self.action('send_message',chat_id=7,text='',attachment_ids=[attachment['id']],request_id=key)
        metadata=self.app.store.get('messages','attachment:'+attachment['id'],account_id='a')
        Path(metadata['path']).unlink()
        repeated=await self.action('send_message',chat_id=7,text='',attachment_ids=[attachment['id']],request_id=key)
        self.assertEqual(repeated['id'],sent['id'])
        self.assertEqual(repeated['status'],'sent')
        self.assertEqual(len(self.app.clients['a'].sent),1)

    async def test_new_input_typed_during_send_is_preserved_after_success(self):
        self.app.clients['a'].gate=asyncio.Event()
        await self.action('save_draft',chat_id=7,text='first')
        sending=asyncio.create_task(self.action('send_message',chat_id=7,text='first',request_id=str(uuid.uuid4())))
        await asyncio.sleep(0)
        await self.action('save_draft',chat_id=7,text='new input')
        self.app.clients['a'].gate.set()
        await sending
        self.assertEqual((await self.action('get_draft',chat_id=7))['text'],'new input')

    async def test_cancel_before_account_dispatch_prevents_network_write(self):
        dispatch=asyncio.Event()
        async def delayed(account_id,operation,**kwargs):
            await dispatch.wait()
            return await operation()
        self.app.execute=delayed
        sending=asyncio.create_task(self.action('send_message',chat_id=7,text='do not send',request_id=str(uuid.uuid4())))
        await asyncio.sleep(0)
        row=self.app.store.list('outbox','a')[0]
        await self.action('cancel_message',outbox_id=row['id'])
        dispatch.set()
        self.assertEqual((await sending)['status'],'cancelled')
        self.assertEqual(self.app.clients['a'].sent,[])

    async def test_rejects_unbound_account_invalid_request_id_and_empty_send(self):
        for data in [{'account_id':'missing','chat_id':7,'text':'x','request_id':str(uuid.uuid4())},{'chat_id':7,'text':'x','request_id':'wrong'},{'chat_id':7,'text':'','request_id':str(uuid.uuid4())}]:
            with self.assertRaises(ValueError): await self.action('send_message',**data)
        self.assertEqual(self.app.clients['a'].sent,[])


if __name__ == '__main__': unittest.main()

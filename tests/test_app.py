import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock

from telethon import errors

from app import Workbench


class AppTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.app = Workbench(Path(self.tmp.name), client_factory=lambda *_: None)

    async def asyncTearDown(self):
        await self.app.close()
        self.tmp.cleanup()

    async def test_settings_do_not_echo_api_or_ai_secrets(self):
        await self.app.action('save_settings', {'telegram_api': {'api_id': 123, 'api_hash': 'a' * 32},
                                               'ai': {'enabled': True, 'endpoint': 'https://api.example.test/v1',
                                                      'model': 'example', 'api_key': 'private-ai-key'}})
        raw = json.dumps(self.app.snapshot())
        self.assertNotIn('a' * 32, raw)
        self.assertNotIn('private-ai-key', raw)
        self.assertTrue(self.app.snapshot()['settings']['api_configured'])

    async def test_mutations_require_account_and_never_fall_back_to_current(self):
        for action in ('save_listener', 'prepare_task', 'send_message', 'prepare_clear'):
            with self.assertRaises(ValueError):
                await self.app.action(action, {})

    async def test_eight_stable_account_ids_and_rename_preserves_listener_config(self):
        accounts = []
        for number in range(8):
            result = await self.app.action('add_account', {'name': f'账号{number}'})
            accounts.append(result['account_id'])
        self.assertEqual(len(set(accounts)), 8)
        with self.assertRaises(ValueError):
            await self.app.action('add_account', {'name': '第九号'})
        self.app.store.put('listener_config', 'current', {'version': 3, 'group_ids': [-123], 'mode': 'preview'}, account_id=accounts[0])
        await self.app.action('rename_account', {'account_id': accounts[0], 'name': '新名字'})
        self.assertEqual(self.app.store.get('listener_config', 'current', account_id=accounts[0])['version'], 3)

    async def test_panel_lock_hides_data_and_rejects_mutations(self):
        await self.app.action('add_account', {'name': '私有账号'})
        await self.app.action('save_settings', {'panel_pin': '123456'})
        await self.app.action('lock_panel', {})
        self.assertNotIn('私有账号', json.dumps(self.app.snapshot()))
        with self.assertRaises(ValueError):
            await self.app.action('add_account', {'name': '不能添加'})
        with self.assertRaises(ValueError):
            await self.app.action('unlock_panel', {'pin': 'wrong'})
        await self.app.action('unlock_panel', {'pin': '123456'})
        self.assertEqual(len(self.app.snapshot()['accounts']), 1)

    async def test_global_pause_is_durable_but_resume_does_not_start_stopped_accounts(self):
        account_id = (await self.app.action('add_account', {'name': 'A'}))['account_id']
        await self.app.action('pause_all', {})
        self.assertTrue(self.app.store.setting('global_paused'))
        await self.app.action('resume_all', {})
        self.assertFalse(self.app.global_paused)
        self.assertEqual(self.app.listeners.snapshot(account_id)['state'], 'stopped')

    async def test_flood_wait_is_scoped_and_durable(self):
        a = (await self.app.action('add_account', {'name': 'A'}))['account_id']
        b = (await self.app.action('add_account', {'name': 'B'}))['account_id']
        self.app.record_wait(a, errors.FloodWaitError(request=None, capture=60))
        self.assertTrue(self.app.wait_state(a)['blocked'])
        self.assertFalse(self.app.wait_state(b).get('blocked', False))
        with self.assertRaises(ValueError):
            await self.app.action('acknowledge_wait', {'account_id': a, 'confirmed': True})

    async def test_shutdown_requires_one_use_preview_token(self):
        with self.assertRaises(ValueError):
            await self.app.action('shutdown', {'confirmation_token': 'wrong'})
        preview = await self.app.action('shutdown_preview', {})
        self.assertIn('affected', preview)
        await self.app.action('shutdown', {'confirmation_token': preview['confirmation_token']})
        with self.assertRaises(ValueError):
            await self.app.action('shutdown', {'confirmation_token': preview['confirmation_token']})

    async def test_ai_endpoint_disallows_embedded_keys_or_nonlocal_plain_http(self):
        for endpoint in ('https://user:secret@example.test/v1', 'https://example.test/v1?key=secret',
                         'http://example.test/v1'):
            with self.assertRaises(ValueError):
                await self.app.action('save_settings', {'ai': {'endpoint': endpoint}})

    async def test_shutdown_preview_invalidates_if_new_work_starts(self):
        preview = await self.app.action('shutdown_preview', {})
        self.app.tasks.snapshot = lambda *args: [{'id': 'new-task', 'account_id': 'A', 'status': 'running'}]
        with self.assertRaises(ValueError):
            await self.app.action('shutdown', {'confirmation_token': preview['confirmation_token']})

    async def test_successful_reconnect_clears_network_wait_but_not_platform_restriction(self):
        account_id = (await self.app.action('add_account', {'name': 'A'}))['account_id']
        self.app.listeners.ready = AsyncMock()
        self.app.chats.ready = AsyncMock()
        self.app.record_wait(account_id, ConnectionResetError('socket reset'))
        await self.app.on_account_ready(account_id)
        self.assertFalse(self.app.wait_state(account_id).get('blocked', False))
        self.app.record_wait(account_id, errors.PeerFloodError(request=None))
        await self.app.on_account_ready(account_id)
        self.assertTrue(self.app.wait_state(account_id)['blocked'])

    async def test_account_unread_and_last_sync_reflect_real_cached_updates(self):
        account_id = (await self.app.action('add_account', {'name': 'A'}))['account_id']
        self.app.store.put('dialogs', '10', {'id': 10, 'account_id': account_id, 'unread': 3}, account_id=account_id)
        self.app.mark_synced(account_id)
        row = self.app.snapshot()['accounts'][0]
        self.assertEqual(row['unread'], 3)
        self.assertTrue(row['last_sync'])

    async def test_invalid_settings_do_not_partially_save_shared_credentials(self):
        with self.assertRaises(ValueError):
            await self.app.action('save_settings', {
                'telegram_api': {'api_id': 123, 'api_hash': 'b' * 32},
                'ai': {'endpoint': 'http://unsafe.example/v1'}})
        self.assertIsNone(self.app.store.setting('telegram_api'))

    async def test_message_cache_clear_preserves_attachments_tombstones_and_unsent_drafts(self):
        account_id = (await self.app.action('add_account', {'name': 'A'}))['account_id']
        self.app.store.put('messages', '10:1', {'id': 1, 'chat_id': 10, 'text': '旧缓存'}, account_id=account_id)
        attachment = {'id': 'upload-id', 'record_type': 'attachment', 'name': '草稿附件.txt'}
        self.app.store.put('messages', 'attachment:upload-id', attachment, account_id=account_id)
        tombstone = {'id': 3, 'chat_id': 10, 'status': 'deleted'}
        self.app.store.put('messages', '10:3', tombstone, account_id=account_id)
        self.app.store.put('drafts', '10', {'text': '未发送草稿', 'attachments': ['upload-id']}, account_id=account_id)
        result = await self.app.action('clear_cache', {'confirmed': True})
        self.assertIsNone(self.app.store.get('messages', '10:1', account_id=account_id))
        self.assertEqual(self.app.store.get('messages', 'attachment:upload-id', account_id=account_id), attachment)
        self.assertEqual(self.app.store.get('messages', '10:3', account_id=account_id), tombstone)
        self.assertEqual(self.app.store.get('drafts', '10', account_id=account_id)['text'], '未发送草稿')
        self.assertEqual(result['removed']['messages'], 1)


if __name__ == '__main__':
    unittest.main()

"""Application composition; account focus is always explicit in every command."""

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sqlite3
import subprocess
import time
from urllib.parse import urlsplit
from uuid import uuid4
import zipfile

from telethon import errors

from accounts import AccountManager
from ai import AIService
from chats import ChatService
from contacts import ContactService
from listeners import ListenerService
from migration import MigrationService
from scheduler import AccountScheduler
from storage import Store, now
from tasks import TaskService


ACCOUNT_ACTIONS = {'add_account', 'rename_account', 'connect_account', 'send_code', 'sign_in',
                   'password', 'cancel_login', 'disconnect_account', 'logout_account', 'remove_account'}
LISTENER_ACTIONS = {'refresh_groups', 'save_listener', 'start_listener', 'stop_listener',
                    'pause_listener', 'resume_listener', 'reset_processed', 'inspect_group'}
CHAT_ACTIONS = {'list_chats', 'refresh_chats', 'messages', 'save_draft', 'get_draft',
                'send_message', 'check_message', 'retry_message', 'cancel_message',
                'edit_message', 'delete_message', 'takeover_chat'}
CONTACT_ACTIONS = {'refresh_contacts', 'list_contacts', 'update_contact', 'preview_import'}
TASK_ACTIONS = {'prepare_task', 'confirm_task', 'pause_task', 'check_task', 'resume_task',
                'cancel_task', 'retry_task', 'verify_task_item', 'task_detail', 'export_task',
                'prepare_clear', 'confirm_clear', 'cancel_clear', 'stop_clear', 'open_backup'}
AI_ACTIONS = {'ai_reply', 'ai_task', 'adopt_ai_draft'}


class Workbench:
    def __init__(self, root, client_factory=None):
        self.root = Path(root).resolve()
        self.store = Store(self.root)
        self.revision = 1
        self.global_paused = bool(self.store.setting('global_paused', False))
        self.locked = False
        self.panel_epoch = 0
        self._unlock_failures = 0
        self._unlock_after = 0
        self._shutdown_plan = None
        self._closing = False
        self._closed = False
        self._startup_task = None
        self._background = set()
        self.accounts = AccountManager(self, client_factory)
        self.scheduler = AccountScheduler(self)
        self.contacts = ContactService(self)
        self.tasks = TaskService(self)
        self.listeners = ListenerService(self)
        self.chats = ChatService(self)
        self.ai = AIService(self)
        self.migration = MigrationService(self)

    def bump(self):
        self.revision += 1

    def mark_synced(self, account_id):
        profile = self.store.get('accounts', account_id, {})
        if profile and not profile.get('deleted'):
            profile['last_sync'] = now()
            self.store.put('accounts', account_id, profile)
            self.bump()

    def log(self, account_id, action, detail, **fields):
        # No Telegram message body or credential is an operation log field.
        forbidden = ('password', 'code', 'phone_hash', 'api_hash', 'api_key', 'session', 'auth_key')
        safe = {key: value for key, value in fields.items() if not any(word in key.lower() for word in forbidden)}
        text = str(detail)
        for config in (self.store.setting('telegram_api', {}), self.store.setting('ai', {})):
            for key in ('api_hash', 'api_key'):
                secret = config.get(key)
                if secret:
                    text = text.replace(str(secret), '[已隐藏]')
        self.store.event(account_id, action, text, **safe)
        self.bump()

    def public_settings(self):
        ai = self.store.setting('ai', {})
        return {'api_configured': bool(self.store.setting('telegram_api')),
                'storage_path': str(self.root),
                'ai': {'enabled': bool(ai.get('enabled', False)), 'endpoint': ai.get('endpoint', ''),
                       'model': ai.get('model', ''), 'key_configured': bool(ai.get('api_key')),
                       'context_limit': min(20, ai.get('context_limit', 10)),
                       'data_scope': '仅你选择的当前会话消息，最多 20 条；凭据不进入 AI'},
                'enter_to_send': self.store.setting('enter_to_send', True),
                'notifications': self.store.setting('notifications', False),
                'log_retention_days': self.store.setting('log_retention_days', 30),
                'panel_pin_set': bool(self.store.setting('panel_pin'))}

    def snapshot(self):
        if self.locked:
            return {'version': '3.0.0', 'revision': self.revision, 'locked': True, 'panel_epoch': self.panel_epoch}
        accounts = []
        for account in self.accounts.public():
            account = dict(account)
            account['listener'] = self.listeners.snapshot(account['id'])
            account['wait'] = self.wait_state(account['id'])
            account['unread'] = sum(max(0, int(d.get('unread', 0))) for d in self.store.list('dialogs', account['id']))
            accounts.append(account)
        return {'version': '3.0.0', 'revision': self.revision, 'locked': False, 'panel_epoch': self.panel_epoch,
                'accounts': accounts, 'settings': self.public_settings(),
                'global_paused': self.global_paused, 'tasks': self.tasks.snapshot(),
                'events': self.store.events(limit=100), 'outbox': self.chats.snapshot()}

    async def start(self):
        self._startup_task = asyncio.current_task()
        try:
            self._prune_logs()
            await self.accounts.restore()
        finally:
            self._startup_task = None

    def _spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    async def on_account_ready(self, account_id):
        if self._closing:
            return
        wait = self.store.get('operation_wait', 'current', {}, account_id=account_id)
        if wait.get('category') in ('network', 'authorization') or wait.get('type') in ('ConnectionError', 'OSError', 'UnauthorizedError', 'AuthKeyUnregisteredError'):
            self.store.delete('operation_wait', 'current', account_id=account_id)
        await self.listeners.ready(account_id)
        await self.chats.ready(account_id)

        async def initial_sync():
            for service, name in ((self.chats, 'refresh_chats'), (self.listeners, 'refresh_groups')):
                if self._closing:
                    return
                try:
                    await asyncio.wait_for(service.action(name, {'account_id': account_id}), 25)
                    self.mark_synced(account_id)
                except Exception:
                    self.log(account_id, 'sync_failed', '首次读取未完成，可在对应页面手动刷新。')
        self._spawn(initial_sync())
        self.bump()

    async def on_account_offline(self, account_id):
        await self.listeners.offline(account_id)
        await self.chats.offline(account_id)
        await self.tasks.pause_account(account_id, reason='账号离线，执行结果不明的项目需先核实。')
        self.bump()

    async def pause_account(self, account_id):
        await self.listeners.stop(account_id, reason='本账号自动化已停止。', drain=True)
        await self.tasks.pause_account(account_id, reason='用户停止本账号自动化。')
        self.bump()

    def record_wait(self, account_id, error):
        seconds = getattr(error, 'seconds', None)
        until = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat() if isinstance(seconds, int) and seconds > 0 else None
        reason = ('Telegram 要求等待后再检查。' if until else
                  '当前操作受限或连接已断开，需要人工检查；结果未知的项目不会自动重试。')
        category = ('network' if isinstance(error, (ConnectionError, OSError)) else
                    'authorization' if isinstance(error, errors.UnauthorizedError) else 'platform')
        self.store.put('operation_wait', 'current', {'type': type(error).__name__, 'until': until,
                                                    'category': category, 'blocked': True, 'reason': reason, 'at': now()}, account_id=account_id)
        self.log(account_id, 'account_wait', reason, wait_until=until, error_type=type(error).__name__)

    def wait_state(self, account_id):
        wait = dict(self.store.get('operation_wait', 'current', {}, account_id=account_id))
        if wait.get('until'):
            try:
                wait['blocked'] = datetime.now(timezone.utc) < datetime.fromisoformat(wait['until'])
            except ValueError:
                wait['blocked'] = True
        profile = self.store.get('accounts', account_id, {})
        if profile.get('wait_restricted'):
            wait.update(blocked=True, reason='本账号受到无明确时长的平台限制，需要人工检查后确认。')
        account_until = profile.get('wait_until')
        if account_until:
            try:
                if datetime.now(timezone.utc) < datetime.fromisoformat(account_until):
                    wait.update(blocked=True, until=account_until, reason='本账号需等待 Telegram 允许后再操作。')
            except ValueError:
                wait.update(blocked=True, reason='账号等待时间无效，请重新连接检查。')
        wait['wait_until'] = wait.get('until')
        return wait

    def automation_allowed(self, account_id):
        if self._closing or self.global_paused or self.wait_state(account_id).get('blocked'):
            return False
        try:
            self.accounts.require(account_id)
            return True
        except ValueError:
            return False

    async def execute(self, account_id, operation, *, priority=10, automation=True):
        self.accounts.require(account_id)
        return await self.scheduler.execute(account_id, operation, priority=priority, automation=automation)

    async def _save_settings(self, data):
        pending = {}
        if 'telegram_api' in data:
            incoming = data['telegram_api']
            if not isinstance(incoming, dict):
                raise ValueError('应用 API 信息格式无效。')
            previous = self.store.setting('telegram_api', {})
            raw_id = incoming.get('api_id') or previous.get('api_id')
            if isinstance(raw_id, str) and raw_id.isascii() and raw_id.isdigit():
                raw_id = int(raw_id)
            secret = incoming.get('api_hash') or previous.get('api_hash', '')
            if type(raw_id) is not int or raw_id <= 0 or not isinstance(secret, str) or not re.fullmatch(r'[a-fA-F0-9]{32}', secret):
                raise ValueError('请填写正整数 API ID 和 32 位 API Hash。')
            changed = previous and (raw_id != previous.get('api_id') or secret != previous.get('api_hash'))
            if changed and any(row['connected'] for row in self.accounts.public()):
                raise ValueError('更改应用 API 信息前，请先断开所有账号。')
            pending['telegram_api'] = {'api_id': raw_id, 'api_hash': secret}
        if 'ai' in data:
            incoming = data['ai']
            if not isinstance(incoming, dict):
                raise ValueError('AI 服务配置格式无效。')
            previous = self.store.setting('ai', {})
            ai = {key: value for key, value in previous.items() if key in ('enabled', 'endpoint', 'model', 'api_key', 'context_limit')}
            for key in ('enabled', 'endpoint', 'model', 'context_limit'):
                if key in incoming:
                    ai[key] = incoming[key]
            if incoming.get('api_key'):
                ai['api_key'] = str(incoming['api_key'])
            endpoint = str(ai.get('endpoint', '')).strip().rstrip('/')
            if endpoint:
                parsed = urlsplit(endpoint)
                local = parsed.hostname in ('127.0.0.1', 'localhost', '::1')
                if parsed.username or parsed.password or parsed.query or parsed.fragment or not parsed.hostname or (parsed.scheme != 'https' and not (parsed.scheme == 'http' and local)):
                    raise ValueError('AI 服务地址需使用 HTTPS；本机服务可用 HTTP，地址中不能包含密码、密钥或查询参数。')
            ai['endpoint'] = endpoint
            if type(ai.get('enabled', False)) is not bool:
                raise ValueError('AI 开关格式无效。')
            limit = ai.get('context_limit', 10)
            if type(limit) is not int or not 1 <= limit <= 20:
                raise ValueError('AI 上下文条数需在 1 至 20 之间。')
            ai['context_limit'] = limit
            if ai.get('enabled') and (not endpoint or not ai.get('model')):
                raise ValueError('启用 AI 前请填写服务地址和模型名称。')
            pending['ai'] = ai
        for key in ('enter_to_send', 'notifications'):
            if key in data:
                if type(data[key]) is not bool:
                    raise ValueError('设置开关格式无效。')
                pending[key] = data[key]
        if 'log_retention_days' in data:
            days = data['log_retention_days']
            if type(days) is not int or not 1 <= days <= 365:
                raise ValueError('日志保留天数需在 1 至 365 之间。')
            pending['log_retention_days'] = days
        if data.get('panel_pin'):
            pin = str(data['panel_pin'])
            if not 4 <= len(pin) <= 128:
                raise ValueError('面板解锁密码需为 4 至 128 个字符。')
            salt = secrets.token_hex(16)
            digest = await asyncio.to_thread(hashlib.scrypt, pin.encode(), salt=bytes.fromhex(salt), n=16384, r=8, p=1)
            hashed = digest.hex()
            pending['panel_pin'] = {'salt': salt, 'hash': hashed}
        for key, value in pending.items():
            self.store.set_setting(key, value)
        self._prune_logs()
        self.bump()
        return self.public_settings()

    def _prune_logs(self):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self.store.setting('log_retention_days', 30))).isoformat()
        for event in self.store.list('events'):
            if event.get('time', '') < cutoff and event.get('id'):
                self.store.delete('events', event['id'], account_id=event.get('account_id', ''))

    def _backup(self):
        destination = self.root / 'backups'
        destination.mkdir(mode=0o700, exist_ok=True)
        stage = destination / ('.stage-' + uuid4().hex)
        stage.mkdir(mode=0o700)
        target = destination / ('workbench-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid4().hex[:8] + '.zip')
        try:
            self.store.backup_to(stage / 'workbench.sqlite3')
            for source in (self.root / 'accounts').glob('*/account.session') if (self.root / 'accounts').exists() else []:
                output = stage / 'accounts' / source.parent.name / 'account.session'
                output.parent.mkdir(mode=0o700, parents=True)
                before = sqlite3.connect(f'file:{source}?mode=ro', uri=True)
                after = sqlite3.connect(output)
                try:
                    before.backup(after)
                finally:
                    after.close()
                    before.close()
                output.chmod(0o600)
            with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as archive:
                for file in stage.rglob('*'):
                    if file.is_file():
                        archive.write(file, file.relative_to(stage))
            target.chmod(0o600)
            return {'path': str(target), 'contains_credentials': True,
                    'warning': '此应用备份包含账号会话和服务密钥，请勿分享；不提供一键恢复。'}
        finally:
            shutil.rmtree(stage)

    def _shutdown_scope(self):
        affected = [t for t in self.tasks.snapshot() if t.get('status') in ('queued', 'running', 'paused', 'waiting')]
        listeners = [a['id'] for a in self.accounts.public() if self.listeners.snapshot(a['id'])['state'] not in ('stopped', 'unconfigured')]
        signature = json.dumps({'tasks': sorted(t['id'] for t in affected), 'listeners': sorted(listeners)}, sort_keys=True)
        return affected, listeners, signature

    async def action(self, name, data):
        if not isinstance(data, dict):
            raise ValueError('请求格式无效。')
        if name == 'unlock_panel':
            if time.monotonic() < self._unlock_after:
                raise ValueError('解锁尝试过多，请稍后重试。')
            pin = self.store.setting('panel_pin')
            if not pin:
                raise ValueError('尚未设置面板解锁密码。')
            digest = await asyncio.to_thread(hashlib.scrypt, str(data.get('pin', '')).encode(), salt=bytes.fromhex(pin['salt']), n=16384, r=8, p=1)
            hashed = digest.hex()
            if not hmac.compare_digest(hashed, pin['hash']):
                self._unlock_failures += 1
                if self._unlock_failures >= 5:
                    self._unlock_after = time.monotonic() + 30
                raise ValueError('解锁密码不正确。')
            self.locked = False
            self.panel_epoch += 1
            self._unlock_failures = 0
            self.bump()
            return {'locked': False}
        if self.locked:
            raise ValueError('面板已锁定，请先解锁。')
        if name == 'save_settings':
            return await self._save_settings(data)
        if name == 'lock_panel':
            if not self.store.setting('panel_pin'):
                raise ValueError('请先在设置中设置面板解锁密码。')
            self.locked = True
            self.panel_epoch += 1
            self.bump()
            return {'locked': True}
        if name == 'pause_all':
            self.global_paused = True
            self.store.set_setting('global_paused', True)
            running = [account['id'] for account in self.accounts.public()
                       if self.listeners.snapshot(account['id'])['state'] in ('running', 'preview', 'live')]
            self.store.set_setting('global_listener_resume', running)
            for account in self.accounts.public():
                if account['id'] in running:
                    try:
                        await self.listeners.action('pause_listener', {'account_id': account['id']})
                    except ValueError:
                        pass
                await self.tasks.pause_account(account['id'], reason='全部自动化已暂停。')
            self.log('', 'global_pause', '新自动化写入与监听入队已暂停，人工发送仍可使用。')
            return {'paused': True}
        if name == 'resume_all':
            self.global_paused = False
            self.store.set_setting('global_paused', False)
            resumed = []
            for account_id in self.store.setting('global_listener_resume', []):
                try:
                    state = self.listeners.snapshot(account_id)
                    if state['state'] == 'paused':
                        await self.listeners.action('resume_listener', {'account_id': account_id})
                        resumed.append(account_id)
                except ValueError:
                    pass
            self.store.set_setting('global_listener_resume', [])
            self.log('', 'global_resume', '已解除全局暂停；邀请需检查后恢复，清空需重新准备并确认。')
            return {'paused': False, 'resumed_listeners': resumed}
        if name == 'shutdown_preview':
            affected, listeners, signature = self._shutdown_scope()
            self._shutdown_plan = {'token': secrets.token_urlsafe(24), 'deadline': time.monotonic() + 300, 'signature': signature}
            return {'confirmation_token': self._shutdown_plan['token'], 'affected': affected,
                    'listeners': listeners, 'warning': '将停止接收并保存任务边界；已提交请求仍可能完成。'}
        if name == 'shutdown':
            plan = self._shutdown_plan
            token = data.get('confirmation_token')
            if not plan or time.monotonic() >= plan['deadline'] or not isinstance(token, str) or not hmac.compare_digest(plan['token'], token):
                raise ValueError('退出确认无效或已过期，请重新查看运行任务。')
            if self._shutdown_scope()[2] != plan['signature']:
                self._shutdown_plan = None
                raise ValueError('运行任务范围已变化，请重新查看并确认退出。')
            self._shutdown_plan = None
            # HTTP sends success first, then closes the server/bridge at a safe boundary.
            self.global_paused = True
            self.bump()
            return {'shutdown': True}
        if name in ('check_legacy', 'import_legacy'):
            return await self.migration.action(name, data)
        if name == 'clear_cache':
            if data.get('confirmed') is not True:
                raise ValueError('请确认清理本地消息和聊天缓存；这会影响本地搜索，不删除 Telegram 云端消息、草稿或任务。')
            message_count = 0
            for profile in self.store.list('accounts', account_id=''):
                account_id = profile['id']
                for row in self.store.list('messages', account_id):
                    if row.get('record_type') or row.get('status') == 'deleted':
                        continue
                    if 'chat_id' not in row or 'id' not in row:
                        continue
                    self.store.delete('messages', f'{row["chat_id"]}:{row["id"]}', account_id=account_id)
                    message_count += 1
            removed = {'messages': message_count, 'dialogs': self.store.clear_kind('dialogs')}
            self.log('', 'clear_cache', '已清理本地消息与聊天缓存；草稿、任务和成功去重保留。')
            return {'removed': removed, 'scope': '本地消息/聊天与本地搜索；保留云端消息、附件文件与上传资料、草稿、任务、操作记录及撤回状态'}
        if name == 'backup_data':
            if data.get('confirmed') is not True:
                raise ValueError('应用备份包含账号会话和服务密钥，请确认在本机私密保存。')
            return self._backup()
        if name == 'open_data_folder':
            if sys_platform_is_mac():
                subprocess.run(['/usr/bin/open', str(self.root)], check=True)
            return {'path': str(self.root)}
        if name in ACCOUNT_ACTIONS:
            return await self.accounts.action(name, data)
        account_id = data.get('account_id')
        if name == 'list_chats' and not account_id:
            return await self.chats.action(name, data)
        if name in LISTENER_ACTIONS | CHAT_ACTIONS | CONTACT_ACTIONS | TASK_ACTIONS | AI_ACTIONS | {'pause_account', 'acknowledge_wait'}:
            self.accounts.get(account_id)  # Required even for offline cached reads.
        else:
            raise ValueError('不支持的操作。')
        if name == 'pause_account':
            await self.pause_account(account_id)
            return {'stopped': True}
        if name == 'acknowledge_wait':
            wait = self.wait_state(account_id)
            if data.get('confirmed') is not True or (wait.get('until') and wait.get('blocked')):
                raise ValueError('平台要求的等待时间尚未结束，或尚未确认人工检查。')
            self.accounts.clear_wait(account_id, confirmed=True)
            self.store.delete('operation_wait', 'current', account_id=account_id)
            self.log(account_id, 'wait_checked', '用户已人工检查；未自动恢复任何写入，不代表平台限制已解除。')
            return {'checked': True}
        if name in LISTENER_ACTIONS:
            result = await self.listeners.action(name, data)
        elif name in CHAT_ACTIONS:
            result = await self.chats.action(name, data)
        elif name in CONTACT_ACTIONS:
            result = await self.contacts.action(name, data)
        elif name in TASK_ACTIONS:
            result = await self.tasks.action(name, data)
        else:
            result = await self.ai.action(name, data)
        self.bump()
        if name in ('refresh_chats', 'refresh_contacts', 'refresh_groups', 'messages'):
            self.mark_synced(account_id)
        return result

    async def close(self):
        if self._closed:
            return
        self._closing = True
        self.global_paused = True
        startup = self._startup_task
        if startup and startup is not asyncio.current_task() and not startup.done():
            startup.cancel()
            await asyncio.gather(startup, return_exceptions=True)
        for task in list(self._background):
            task.cancel()
        if self._background:
            await asyncio.gather(*self._background, return_exceptions=True)
        await self.listeners.close()
        await self.tasks.close()
        await self.chats.close()
        await self.scheduler.close()
        await self.accounts.close()
        self.store.close()
        self._closed = True


def sys_platform_is_mac():
    import sys
    return sys.platform == 'darwin'

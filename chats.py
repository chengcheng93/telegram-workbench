"""Account-scoped manual chats, local history and crash-safe message outbox.

History and chat events never call the contact listener. An ambiguous Telegram
write remains unverified; reconnecting never replays it or queued manual sends.
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import inspect
import json
import mimetypes
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from telethon import events


MAX_ATTACHMENT = 25 * 1024 * 1024
PAGE_SIZE = 50


def _now():
    return datetime.now(timezone.utc).isoformat()


def _date(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat()
    return str(value) if value else None


def _integer(value, name, *, optional=False):
    if optional and value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f'{name}无效')
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError(f'{name}无效') from None
    if str(value).strip() != str(parsed) or not parsed or abs(parsed) > 9007199254740991:
        raise ValueError(f'{name}无效')
    return parsed


def _filename(value):
    value = str(value or '附件').replace('\\', '/').split('/')[-1]
    value = re.sub(r'[\x00-\x1f\x7f]', '', value).strip().strip('.')[:180]
    return value or '附件'


def _public_error(error):
    name = type(error).__name__
    if name in {'FloodWaitError', 'FloodPremiumWaitError', 'SlowModeWaitError'}:
        seconds = getattr(error, 'seconds', None)
        return f'平台要求等待{seconds}秒后再操作' if seconds is not None else '平台要求等待后再操作'
    if name in {'ChatWriteForbiddenError', 'ChatAdminRequiredError', 'MessageAuthorRequiredError', 'MessageEditTimeExpiredError', 'MessageDeleteForbiddenError'}:
        return '当前账号没有此操作权限，或消息已超过允许操作的时限'
    if name in {'UserPrivacyRestrictedError', 'UserIsBlockedError', 'YouBlockedUserError'}:
        return '对方的隐私设置或屏蔽状态不允许发送'
    if name in {'AuthKeyUnregisteredError', 'SessionRevokedError', 'UserDeactivatedError'}:
        return '账号授权失效，请重新登录后检查结果'
    if isinstance(error, (TimeoutError, ConnectionError, OSError, asyncio.CancelledError)):
        return '连接中断或请求超时，结果尚未核实，请先检查，勿重复发送'
    return '请求未完成，请检查账号连接和 Telegram 权限'


def _definite_rejection(error):
    # 4xx RPC rejections establish that Telegram did not accept this write.
    code = getattr(error, 'code', None)
    if isinstance(code, int) and 400 <= code < 500:
        return True
    return type(error).__name__ in {
        'ChatWriteForbiddenError', 'ChatAdminRequiredError', 'MessageAuthorRequiredError',
        'MessageEditTimeExpiredError', 'MessageDeleteForbiddenError', 'PeerIdInvalidError',
        'UserPrivacyRestrictedError', 'UserIsBlockedError', 'YouBlockedUserError',
        'MessageTooLongError', 'MessageEmptyError', 'ReplyMessageNotFoundError',
        'FloodWaitError', 'FloodPremiumWaitError', 'SlowModeWaitError',
        'AuthKeyUnregisteredError', 'SessionRevokedError', 'UserDeactivatedError',
    }


class _UploadCancelled(Exception):
    pass


class ChatService:
    def __init__(self, app):
        self.app = app
        self.store = app.store
        self._handlers = {}
        self._active = set()
        self._cancelled = set()
        self._event_clock = 0
        self._message_versions = {}
        self._dialog_versions = {}
        # A previous process may have died after Telegram accepted the request.
        for row in self.store.list('outbox'):
            if row.get('status') in {'inflight', 'uploading'}:
                row.update(status='unverified', error='程序曾在提交时中断，请先核实，勿重复发送')
                self.store.put('outbox', row['id'], row, account_id=row['account_id'])

    def _account(self, account_id):
        if not isinstance(account_id, str) or not account_id:
            raise ValueError('请明确选择所属账号')
        try:
            session = self.app.accounts.get(account_id)
        except (KeyError, ValueError):
            session = None
        if session is None:
            raise ValueError('所属账号不存在，请重新选择')
        return session

    def _online(self, account_id):
        self._account(account_id)
        try:
            return self.app.accounts.require(account_id)
        except (ValueError, ConnectionError):
            return None

    def _bump(self):
        self.app.bump()

    def _mark_synced(self, account_id):
        marker = getattr(self.app, 'mark_synced', None)
        if callable(marker):
            marker(account_id)

    def _put(self, kind, key, row, account_id):
        self.store.put(kind, str(key), row, account_id=account_id)
        self._bump()

    def _public_outbox(self, row):
        return {k: v for k, v in row.items() if k not in {'params_hash'}}

    def snapshot(self, account_id=None):
        return [self._public_outbox(row) for row in self.store.list('outbox', account_id)]

    def _message(self, account_id, chat_id, message):
        mid = _integer(getattr(message, 'id', None), '消息编号')
        actual_chat = getattr(message, 'chat_id', None)
        if actual_chat is not None and int(actual_chat) != chat_id:
            raise ValueError('消息不属于当前账号选择的会话')
        reply = getattr(message, 'reply_to', None)
        reply_to = getattr(reply, 'reply_to_msg_id', None) if reply else None
        sent_date = getattr(message, 'date', None)
        recent = isinstance(sent_date, datetime) and sent_date.replace(tzinfo=sent_date.tzinfo or timezone.utc) >= datetime.now(timezone.utc) - timedelta(hours=48)
        own_id = getattr(self._account(account_id), 'own_id', None)
        outgoing = bool(getattr(message, 'out', False))
        row = {
            'id': mid, 'account_id': account_id, 'chat_id': chat_id,
            'text': str(getattr(message, 'message', None) or getattr(message, 'text', None) or ''),
            'date': _date(getattr(message, 'date', None)),
            'out': outgoing, 'reply_to': reply_to,
            'status': 'sent', 'can_edit': outgoing and bool(recent or chat_id == own_id),
            'can_delete': outgoing or chat_id > 0,
        }
        file = getattr(message, 'file', None)
        if getattr(message, 'media', None) or file or getattr(message, 'photo', None) or getattr(message, 'document', None):
            name = _filename(getattr(file, 'name', None) or ('图片.jpg' if getattr(message, 'photo', None) else '附件'))
            row['media'] = {'name': name, 'size': getattr(file, 'size', None),
                            'mime': getattr(file, 'mime_type', None) or mimetypes.guess_type(name)[0] or 'application/octet-stream',
                            'type': 'image' if getattr(message, 'photo', None) else 'file', 'downloadable': True}
        return row

    def _cache_message(self, account_id, chat_id, message, *, live=False, edited=False, history_epoch=None):
        row = self._message(account_id, chat_id, message)
        key = f'{chat_id}:{row["id"]}'
        previous = self.store.get('messages', key, account_id=account_id)
        if chat_id >= -1000000000000 and self.store.get('messages', f'deleted:{row["id"]}', account_id=account_id):
            row.update(status='deleted', text='', media=None, can_edit=False, can_delete=False)
            self._put('messages', key, row, account_id)
            return row
        # A stale read response cannot undo a newer live update or a tombstone.
        if previous and (previous.get('status') == 'deleted' or
                         (history_epoch is not None and self._message_versions.get((account_id, key), 0) > history_epoch)):
            return previous
        if live or edited:
            self._event_clock += 1
            self._message_versions[account_id, key] = self._event_clock
            self._dialog_versions[account_id, chat_id] = self._event_clock
        self._put('messages', key, row, account_id)
        dialog = self.store.get('dialogs', str(chat_id), account_id=account_id) or {
            'account_id': account_id, 'id': chat_id, 'title': str(chat_id), 'unread': 0,
            'is_group': chat_id < 0, 'pinned': False, 'muted': False,
        }
        if live and not previous and not row['out']:
            dialog['unread'] = int(dialog.get('unread') or 0) + 1
        if (row.get('date') or '', row['id']) >= (dialog.get('date') or '', dialog.get('last_message_id') or 0) or dialog.get('last_message_id') == row['id']:
            dialog.update(last_text=row['text'], date=row['date'], last_message_id=row['id'])
        self._put('dialogs', str(chat_id), dialog, account_id)
        if live or edited:
            self._mark_synced(account_id)
        return row

    async def ready(self, account_id):
        session = self.app.accounts.require(account_id)
        client = session.client
        old = self._handlers.get(account_id)
        if old and old[0] is client:
            return
        if old:
            await self.offline(account_id)
        async def new(event):
            await self._event(account_id, event)

        async def edit(event):
            await self._event(account_id, event, edited=True)

        async def delete(event):
            chat_id = getattr(event, 'chat_id', None)
            ids = set(getattr(event, 'deleted_ids', []) or [])
            self._event_clock += 1
            matched = set()
            for row in self.store.list('messages', account_id):
                if row.get('record_type') in {'attachment', 'global_delete'} or row.get('id') not in ids:
                    continue
                if chat_id is None and row.get('chat_id', 0) < -1000000000000:
                    continue
                if chat_id is None or row.get('chat_id') == chat_id:
                    matched.add(row['id'])
                    self._message_versions[account_id, f'{row["chat_id"]}:{row["id"]}'] = self._event_clock
                    self._dialog_versions[account_id, row['chat_id']] = self._event_clock
                    row.update(status='deleted', text='', media=None, can_edit=False, can_delete=False)
                    self._put('messages', f'{row["chat_id"]}:{row["id"]}', row, account_id)
                    dialog = self.store.get('dialogs', str(row['chat_id']), account_id=account_id)
                    if dialog and dialog.get('last_message_id') == row['id']:
                        dialog['last_text'] = '消息已撤回'
                        self._put('dialogs', str(row['chat_id']), dialog, account_id)
            if chat_id is not None:
                self._dialog_versions[account_id, chat_id] = self._event_clock
                for mid in ids - matched:
                    key = f'{chat_id}:{mid}'
                    self._message_versions[account_id, key] = self._event_clock
                    tombstone = {'id': mid, 'account_id': account_id, 'chat_id': chat_id,
                                 'text': '', 'date': None, 'out': False, 'reply_to': None,
                                 'status': 'deleted', 'can_edit': False, 'can_delete': False}
                    self._put('messages', key, tombstone, account_id)
            else:
                # Telegram omits chat_id for ordinary private/basic-group deletes.
                # Their message IDs are account-global; channel IDs are separate.
                for mid in ids:
                    self._put('messages', f'deleted:{mid}', {'id': mid, 'account_id': account_id,
                              'record_type': 'global_delete', 'status': 'deleted'}, account_id)
            self._mark_synced(account_id)

        handlers = [(new, events.NewMessage()), (edit, events.MessageEdited()), (delete, events.MessageDeleted())]
        for callback, builder in handlers:
            client.add_event_handler(callback, builder)
        self._handlers[account_id] = (client, handlers)

    async def _event(self, account_id, event, edited=False):
        message = getattr(event, 'message', None)
        chat_id = getattr(event, 'chat_id', None) or getattr(message, 'chat_id', None)
        if message is None or chat_id is None:
            return
        try:
            self._cache_message(account_id, _integer(chat_id, '会话编号'), message, live=not edited, edited=edited)
        except (ValueError, TypeError):
            self.app.log(account_id, 'chat_event', '收到无法识别的聊天更新，已跳过')

    async def offline(self, account_id):
        current = self._handlers.pop(account_id, None)
        if current:
            client, handlers = current
            for callback, builder in handlers:
                client.remove_event_handler(callback, builder)

    async def close(self):
        for account_id in list(self._handlers):
            await self.offline(account_id)
        for outbox_id in list(self._active):
            self._cancelled.add(outbox_id)

    async def _refresh(self, account_id):
        client = self.app.accounts.require(account_id).client
        history_epoch = self._event_clock
        try:
            dialogs = await client.get_dialogs(limit=100)
        except Exception as error:
            raise ValueError(_public_error(error)) from None
        for dialog in dialogs:
            chat_id = _integer(dialog.id, '会话编号')
            message = getattr(dialog, 'message', None)
            settings = getattr(getattr(dialog, 'dialog', None), 'notify_settings', None)
            muted_until = getattr(settings, 'mute_until', None)
            muted = bool(muted_until and (not isinstance(muted_until, datetime) or muted_until.replace(tzinfo=muted_until.tzinfo or timezone.utc) > datetime.now(timezone.utc)))
            row = {'account_id': account_id, 'id': chat_id, 'title': str(getattr(dialog, 'title', None) or getattr(dialog, 'name', None) or chat_id),
                   'last_text': str(getattr(message, 'message', None) or getattr(message, 'text', None) or ''),
                   'date': _date(getattr(dialog, 'date', None) or getattr(message, 'date', None)),
                   'last_message_id': getattr(message, 'id', None), 'unread': int(getattr(dialog, 'unread_count', 0) or 0),
                   'is_group': bool(getattr(dialog, 'is_group', False) or getattr(dialog, 'is_channel', False)),
                   'pinned': bool(getattr(dialog, 'pinned', False)), 'muted': muted}
            existing = self.store.get('dialogs', str(chat_id), account_id=account_id)
            if existing and self._dialog_versions.get((account_id, chat_id), 0) > history_epoch:
                row = {**existing, 'title': row['title'], 'is_group': row['is_group'],
                       'pinned': row['pinned'], 'muted': row['muted']}
            self._put('dialogs', str(chat_id), row, account_id)
            if message and getattr(message, 'id', None):
                self._cache_message(account_id, chat_id, message, history_epoch=history_epoch)
        self._mark_synced(account_id)
        return self._list({'account_id': account_id})

    def _list(self, data):
        account_id = data.get('account_id') or None
        if account_id:
            self._account(account_id)
        query = str(data.get('query') or '').strip().casefold()
        mode = data.get('filter') or 'all'
        if mode not in {'all', 'unread', 'private', 'group'}:
            raise ValueError('聊天筛选条件无效')
        hits = set()
        if query:
            hits = {(r.get('account_id'), r.get('chat_id')) for r in self.store.list('messages', account_id)
                    if r.get('record_type') != 'attachment' and r.get('status') != 'deleted' and query in str(r.get('text', '')).casefold()}
        rows = []
        for row in self.store.list('dialogs', account_id):
            if mode == 'unread' and not row.get('unread'): continue
            if mode == 'private' and row.get('is_group'): continue
            if mode == 'group' and not row.get('is_group'): continue
            if query and query not in str(row.get('title', '')).casefold() and query not in str(row.get('last_text', '')).casefold() and (row.get('account_id'), row.get('id')) not in hits:
                continue
            rows.append(row)
        rows.sort(key=lambda r: (bool(r.get('pinned')), r.get('date') or ''), reverse=True)
        return {'dialogs': rows}

    async def _messages(self, account_id, chat_id, before_id):
        session = self._online(account_id)
        if session:
            history_epoch = self._event_clock
            try:
                messages = await session.client.get_messages(chat_id, limit=PAGE_SIZE + 1, max_id=before_id or 0)
            except Exception as error:
                raise ValueError(_public_error(error)) from None
            messages = list(messages or [])
            page = messages[:PAGE_SIZE]
            rows = [self._cache_message(account_id, chat_id, m, history_epoch=history_epoch) for m in page]
            if chat_id < 0 and hasattr(session.client, 'get_permissions'):
                try:
                    rights = await session.client.get_permissions(chat_id, 'me')
                    can_delete = bool(getattr(rights, 'delete_messages', False) or getattr(rights, 'is_creator', False))
                except Exception:
                    can_delete = False
                if can_delete:
                    for row in rows:
                        if row.get('status') != 'deleted': row['can_delete'] = True
            has_more = len(messages) > PAGE_SIZE
            self._mark_synced(account_id)
        else:
            rows = [r for r in self.store.list('messages', account_id) if r.get('record_type') != 'attachment' and r.get('chat_id') == chat_id and (not before_id or r.get('id', 0) < before_id)]
            rows.sort(key=lambda r: r['id'], reverse=True)
            has_more = len(rows) > PAGE_SIZE
            rows = rows[:PAGE_SIZE]
        rows.sort(key=lambda r: r['id'])
        return {'messages': rows, 'has_more': has_more, 'offline': session is None}

    def _draft(self, account_id, chat_id, data=None):
        if data is None:
            return self.store.get('drafts', str(chat_id), {'text': '', 'reply_to': None}, account_id=account_id)
        text = data.get('text', '')
        if not isinstance(text, str) or len(text) > 100000:
            raise ValueError('草稿内容无效或过长')
        row = {'account_id': account_id, 'chat_id': chat_id, 'text': text,
               'reply_to': _integer(data.get('reply_to'), '回复消息编号', optional=True), 'updated_at': _now()}
        self._put('drafts', str(chat_id), row, account_id)
        return row

    def _media_dir(self, account_id):
        self._account(account_id)
        root = Path(self.app.root).resolve()
        media_root = root / 'media'
        path = media_root / hashlib.sha256(account_id.encode()).hexdigest()
        try:
            for directory in (media_root, path):
                if directory.is_symlink():
                    raise ValueError('附件缓存目录无效，请检查本机存储')
                directory.mkdir(mode=0o700, exist_ok=True)
                os.chmod(directory, 0o700)
        except OSError:
            raise ValueError('附件缓存目录不可用，请检查本机存储与权限') from None
        if not path.resolve().is_relative_to(root):
            raise ValueError('附件缓存目录无效')
        return path

    def _attachment(self, account_id, attachment_id):
        if not isinstance(attachment_id, str) or not re.fullmatch(r'[a-f0-9]{32}', attachment_id):
            raise ValueError('附件编号无效')
        row = self.store.get('messages', 'attachment:' + attachment_id, account_id=account_id)
        if not row or row.get('account_id') != account_id or row.get('record_type') != 'attachment':
            raise ValueError('附件不属于当前账号，或已不存在')
        path = Path(row.get('path', ''))
        parent = self._media_dir(account_id)
        if path.is_symlink() or not path.is_file() or path.parent != parent or not path.resolve().is_relative_to(parent):
            raise ValueError('附件文件无效或已不在当前账号的缓存中')
        try:
            if path.stat().st_size != row['size'] or hashlib.sha256(path.read_bytes()).hexdigest() != row['sha256']:
                raise ValueError('附件已发生变化，请重新选择附件')
        except OSError:
            raise ValueError('附件无法读取，请重新选择附件') from None
        return row

    async def upload(self, account_id, filename, content):
        self._account(account_id)
        if not isinstance(content, bytes) or not content or len(content) > MAX_ATTACHMENT:
            raise ValueError('附件必须非空且不超过25MiB')
        attachment_id = uuid.uuid4().hex
        name = _filename(filename)
        # The original name is display-only. The filesystem name is generated.
        extension = Path(name).suffix[:16]
        extension = extension if re.fullmatch(r'\.[A-Za-z0-9]{1,15}', extension) else ''
        path = self._media_dir(account_id) / (attachment_id + extension)
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(content)
        except OSError:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            raise ValueError('附件保存失败，请检查磁盘可用空间与目录权限') from None
        row = {'id': attachment_id, 'account_id': account_id, 'record_type': 'attachment',
               'name': name, 'size': len(content), 'mime': mimetypes.guess_type(name)[0] or 'application/octet-stream',
               'path': str(path), 'sha256': hashlib.sha256(content).hexdigest(), 'progress': 100, 'status': 'ready', 'created_at': _now()}
        self._put('messages', 'attachment:' + attachment_id, row, account_id)
        return {k: v for k, v in row.items() if k not in {'path', 'sha256', 'record_type'}}

    async def media(self, account_id, chat_id, message_id):
        chat_id = _integer(chat_id, '会话编号'); message_id = _integer(message_id, '消息编号')
        client = self.app.accounts.require(account_id).client
        try:
            message = await client.get_messages(chat_id, ids=message_id)
        except Exception as error:
            raise ValueError(_public_error(error)) from None
        if not message:
            raise ValueError('当前账号无法访问此消息或附件')
        row = self._message(account_id, chat_id, message)
        info = row.get('media') or {'name': '附件', 'mime': 'application/octet-stream'}
        parent = self._media_dir(account_id)
        path = parent / (uuid.uuid4().hex + '.download')
        try:
            result = await client.download_media(message, file=str(path))
            if not result or Path(result) != path or path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(parent):
                raise ValueError('附件下载未完成，或缓存位置无效')
            os.chmod(path, 0o600)
        except Exception as error:
            if path.exists() or path.is_symlink(): path.unlink(missing_ok=True)
            if isinstance(error, ValueError): raise
            raise ValueError(_public_error(error)) from None
        return {'path': path, 'name': info['name'], 'mime': info['mime']}

    def _outbox(self, account_id, outbox_id):
        self._account(account_id)
        row = self.store.get('outbox', str(outbox_id), account_id=account_id)
        if not row or row.get('account_id') != account_id:
            raise ValueError('待发送消息不存在，或不属于当前账号')
        return row

    @staticmethod
    def _task_owned(row):
        return any(row.get(key) is not None for key in ('task_id', 'item_id', 'task_ownership_error'))

    @staticmethod
    def _validate_task_boundary(task_id, item_id, priority, automation, before_submit):
        if task_id is None and item_id is None:
            return False
        if any(not isinstance(value, str) or not value.strip() or len(value) > 128 for value in (task_id, item_id)):
            raise ValueError('任务消息归属无效，请到任务中心检查')
        if priority != 20 or automation is not True or not callable(before_submit):
            raise ValueError('任务消息必须通过任务中心的确认与自动化队列发送')
        return True

    async def send_confirmed(self, account_id, chat_id, text, *, request_id, reply_to=None, attachment_ids=None, before_submit=None, retry_failed=False, priority=0, automation=False, task_id=None, item_id=None):
        self._account(account_id)
        if before_submit is not None and not callable(before_submit):
            raise ValueError('任务提交回调无效')
        if not isinstance(retry_failed, bool):
            raise ValueError('任务重试确认参数无效')
        if type(priority) is not int or not 0 <= priority <= 100 or not isinstance(automation, bool):
            raise ValueError('发送队列参数无效')
        task_owned = self._validate_task_boundary(task_id, item_id, priority, automation, before_submit)
        chat_id = _integer(chat_id, '会话编号'); reply_to = _integer(reply_to, '回复消息编号', optional=True)
        try:
            request_id = str(uuid.UUID(str(request_id)))
        except (ValueError, TypeError, AttributeError):
            raise ValueError('发送请求编号无效，请重新创建发送请求') from None
        attachments = attachment_ids if attachment_ids is not None else []
        if not isinstance(attachments, list) or len(attachments) > 10:
            raise ValueError('一次最多选择10个附件')
        if not isinstance(text, str) or (not text.strip() and not attachments) or len(text) > (1024 if attachments else 4096):
            raise ValueError('请输入消息，且文字不超过4096字、附件说明不超过1024字')
        attachments = list(attachments)
        if any(not isinstance(a, str) or not re.fullmatch(r'[a-f0-9]{32}', a) for a in attachments):
            raise ValueError('附件编号无效')
        digest = hashlib.sha256(json.dumps({'chat_id': chat_id, 'text': text, 'reply_to': reply_to, 'attachment_ids': attachments}, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
        # Request key is independently scoped by account in the store.
        existing = next((r for r in self.store.list('outbox', account_id) if r.get('request_id') == request_id), None)
        if existing:
            if existing.get('task_ownership_error') is not None:
                raise ValueError('任务消息归属存在冲突，请到任务中心检查，禁止直接重发')
            if self._task_owned(existing) != task_owned or (task_owned and (existing.get('task_id'), existing.get('item_id')) != (task_id, item_id)):
                raise ValueError('此发送请求的任务归属不能更改，请到任务中心操作')
            if existing.get('params_hash') != digest:
                raise ValueError('同一发送请求不能更改账号、会话、正文、回复或附件')
            explicit_retry = retry_failed and existing['status'] == 'failed'
            if (existing['status'] != 'queued' and not explicit_retry) or existing['id'] in self._active:
                return self._public_outbox(existing)
            row = existing
            if explicit_retry:
                row.update(status='queued', updated_at=_now())
                self._put('outbox', row['id'], row, account_id)
        else:
            for attachment_id in attachments:
                self._attachment(account_id, attachment_id)
            row = {'id': uuid.uuid4().hex, 'account_id': account_id, 'chat_id': chat_id, 'text': text,
                   'request_id': request_id, 'reply_to': reply_to, 'attachment_ids': attachments,
                   'params_hash': digest, 'status': 'queued', 'progress': 0, 'error': None,
                   'created_at': _now(), 'updated_at': _now(), 'message_ids': []}
            if task_owned:
                row.update(task_id=task_id, item_id=item_id)
            self._put('outbox', row['id'], row, account_id)
        if not self._online(account_id):
            row.update(error='账号离线，已保存为待发送；恢复连接后请确认发送或取消')
            self._put('outbox', row['id'], row, account_id)
            return self._public_outbox(row)
        return await self._dispatch(row, before_submit=before_submit, priority=priority, automation=automation)

    async def _dispatch(self, row, *, before_submit=None, priority=0, automation=False):
        account_id, outbox_id = row['account_id'], row['id']
        if row.get('task_ownership_error') is not None:
            raise ValueError('任务消息归属存在冲突，请到任务中心检查，禁止直接重发')
        if self._task_owned(row):
            self._validate_task_boundary(row.get('task_id'), row.get('item_id'), priority, automation, before_submit)
        if outbox_id in self._active:
            return self._public_outbox(self._outbox(account_id, outbox_id))
        self._active.add(outbox_id)
        submitted = False

        async def operation():
            nonlocal submitted
            fresh = self._outbox(account_id, outbox_id)
            if fresh['status'] == 'cancelled' or outbox_id in self._cancelled:
                return None
            client = self.app.accounts.require(account_id).client
            try:
                attachments = [self._attachment(account_id, a) for a in row.get('attachment_ids', [])]
            except ValueError as error:
                row.update(status='failed', error=str(error), updated_at=_now())
                self._put('outbox', outbox_id, row, account_id)
                return None
            files = []
            for attachment in attachments:
                stream = io.BytesIO(Path(attachment['path']).read_bytes())
                stream.name = attachment['name']
                files.append(stream)
            if before_submit is not None:
                try:
                    callback_result = before_submit()
                    if inspect.isawaitable(callback_result):
                        await callback_result
                except BaseException:
                    for stream in files: stream.close()
                    raise
            if outbox_id in self._cancelled or self._outbox(account_id, outbox_id)['status'] == 'cancelled':
                for stream in files: stream.close()
                return None
            row.update(status='inflight', error=None, updated_at=_now(), submitted_at=_now())
            self._put('outbox', outbox_id, row, account_id)
            submitted = True

            async def progress(current, total):
                if outbox_id in self._cancelled:
                    raise _UploadCancelled()
                row.update(progress=min(99, int(current / total * 100)) if total else 0)
                self._put('outbox', outbox_id, row, account_id)

            if files:
                try:
                    return await client.send_file(row['chat_id'], files[0] if len(files) == 1 else files,
                                                  caption=row['text'], reply_to=row.get('reply_to'),
                                                  parse_mode=None, progress_callback=progress)
                finally:
                    for stream in files:
                        stream.close()
            return await client.send_message(row['chat_id'], row['text'], reply_to=row.get('reply_to'), parse_mode=None)

        try:
            result = await self.app.execute(account_id, operation, priority=priority, automation=automation)
            if result is None:
                row = self._outbox(account_id, outbox_id)
            else:
                messages = result if isinstance(result, (list, tuple)) else [result]
                rows = [self._cache_message(account_id, row['chat_id'], m) for m in messages]
                row.update(status='sent', progress=100, error=None, message_ids=[m['id'] for m in rows], sent_at=_now())
                self._put('outbox', outbox_id, row, account_id)
                if self._task_owned(row):
                    self.app.log(account_id, 'task_message', '已确认任务消息已发送', chat_id=row['chat_id'], outbox_id=outbox_id,
                                 task_id=row['task_id'], item_id=row['item_id'])
                else:
                    draft = self._draft(account_id, row['chat_id'])
                    if draft.get('text') == row['text'] and draft.get('reply_to') == row.get('reply_to'):
                        self._draft(account_id, row['chat_id'], {'text': ''})
                    self.app.log(account_id, 'manual_message', '人工消息已发送', chat_id=row['chat_id'], outbox_id=outbox_id)
        except (Exception, asyncio.CancelledError) as error:
            if not submitted:
                current = self._outbox(account_id, outbox_id)
                if current.get('status') == 'cancelled':
                    row = current
                else:
                    row.update(status='queued', error='尚未提交，账号离线或正在等待平台允许；可取消或稍后确认发送')
            else:
                known_failure = _definite_rejection(error) and len(row.get('attachment_ids', [])) <= 1
                row.update(status='failed' if known_failure else 'unverified', error=_public_error(error))
                if len(row.get('attachment_ids', [])) > 1:
                    row['error'] = '多附件发送未完整确认，请先逐项核实，勿重新发送整批附件'
                if isinstance(error, _UploadCancelled):
                    row['error'] = '附件上传已请求取消；提交结果尚未核实，请勿重复发送'
            row['updated_at'] = _now()
            self._put('outbox', outbox_id, row, account_id)
        finally:
            self._active.discard(outbox_id)
            self._cancelled.discard(outbox_id)
        return self._public_outbox(row)

    async def _check(self, account_id, outbox_id):
        row = self._outbox(account_id, outbox_id)
        if row.get('status') != 'unverified':
            return self._public_outbox(row)
        client = self.app.accounts.require(account_id).client
        ids = row.get('message_ids') or []
        verified = bool(ids)
        for mid in ids:
            try:
                message = await client.get_messages(row['chat_id'], ids=mid)
            except Exception:
                verified = False; break
            if not message or not getattr(message, 'out', False) or (getattr(message, 'chat_id', row['chat_id']) != row['chat_id']):
                verified = False; break
            if row.get('text') and str(getattr(message, 'message', None) or getattr(message, 'text', None) or '') != row['text']:
                verified = False; break
        if verified:
            row.update(status='sent', error=None, progress=100, verified_at=_now())
        else:
            row.update(error='缺少可唯一核对的 Telegram 消息编号，结果仍未核实；请在客户端核对，勿盲目重试', checked_at=_now())
        self._put('outbox', outbox_id, row, account_id)
        return self._public_outbox(row)

    async def _edit(self, account_id, chat_id, message_id, text):
        client = self.app.accounts.require(account_id).client
        if not isinstance(text, str) or not text.strip() or len(text) > 4096:
            raise ValueError('编辑消息内容无效')
        try:
            message = await client.get_messages(chat_id, ids=message_id)
            if not message or not getattr(message, 'out', False):
                raise ValueError('只能编辑当前账号自己发送且仍允许编辑的消息')
            self._message(account_id, chat_id, message)
            async def operation():
                return await client.edit_message(chat_id, message_id, text, parse_mode=None)
            edited = await self.app.execute(account_id, operation, priority=0, automation=False)
        except ValueError:
            raise
        except Exception as error:
            raise ValueError(_public_error(error)) from None
        return {'message': self._cache_message(account_id, chat_id, edited)}

    async def _delete(self, account_id, chat_id, message_id, confirmed):
        if confirmed is not True:
            raise ValueError('撤回消息前请明确确认')
        client = self.app.accounts.require(account_id).client
        try:
            message = await client.get_messages(chat_id, ids=message_id)
            if not message:
                raise ValueError('消息不存在，或当前账号无法访问')
            row = self._message(account_id, chat_id, message)
            if not row['out'] and chat_id < 0:
                rights = await client.get_permissions(chat_id, 'me')
                if not any(getattr(rights, field, False) for field in ('delete_messages', 'is_creator')):
                    raise ValueError('当前账号没有撤回此群消息的权限')
            async def operation():
                return await client.delete_messages(chat_id, [message_id], revoke=True)
            await self.app.execute(account_id, operation, priority=0, automation=False)
        except ValueError:
            raise
        except Exception as error:
            raise ValueError(_public_error(error)) from None
        row.update(status='deleted', text='', media=None, can_edit=False, can_delete=False)
        self._put('messages', f'{chat_id}:{message_id}', row, account_id)
        return {'deleted': True, 'message_id': message_id}

    async def action(self, name, data):
        if name == 'list_chats':
            return self._list(data)
        account_id = data.get('account_id')
        self._account(account_id)
        if name == 'refresh_chats':
            return await self._refresh(account_id)
        if name in {'check_message', 'retry_message', 'cancel_message'}:
            row = self._outbox(account_id, data.get('outbox_id'))
            if name == 'check_message': return await self._check(account_id, row['id'])
            if self._task_owned(row):
                raise ValueError('此消息属于任务，请到任务中心检查、重试或取消')
            if name == 'retry_message':
                if row['status'] != 'failed':
                    raise ValueError('只允许重试已确定失败的消息；结果未知必须先核实')
                if not self._online(account_id):
                    raise ValueError('账号离线，请恢复连接后重试')
                return await self._dispatch(row)
            if row['status'] == 'sent':
                raise ValueError('消息已发送，无法取消待发送项；请使用撤回操作')
            if row['status'] in {'inflight', 'uploading', 'unverified'}:
                self._cancelled.add(row['id'])
                row.update(status='unverified', cancel_requested=True, error='已请求停止上传；在途发送可能仍完成，请先核实')
            else:
                row.update(status='cancelled', error=None)
                self._cancelled.add(row['id'])
            self._put('outbox', row['id'], row, account_id)
            return self._public_outbox(row)
        chat_id = _integer(data.get('chat_id'), '会话编号')
        if name == 'messages':
            return await self._messages(account_id, chat_id, _integer(data.get('before_id'), '历史消息编号', optional=True))
        if name == 'save_draft': return self._draft(account_id, chat_id, data)
        if name == 'get_draft': return self._draft(account_id, chat_id)
        if name == 'send_message':
            return await self.send_confirmed(account_id, chat_id, data.get('text', ''), request_id=data.get('request_id'), reply_to=data.get('reply_to'), attachment_ids=data.get('attachment_ids'))
        if name == 'edit_message':
            return await self._edit(account_id, chat_id, _integer(data.get('message_id'), '消息编号'), data.get('text'))
        if name == 'delete_message':
            return await self._delete(account_id, chat_id, _integer(data.get('message_id'), '消息编号'), data.get('confirmed'))
        if name == 'takeover_chat':
            enabled = data.get('enabled')
            if not isinstance(enabled, bool): raise ValueError('人工接管状态无效')
            row = {'account_id': account_id, 'chat_id': chat_id, 'enabled': enabled, 'mode': 'manual' if enabled else 'assisted', 'updated_at': _now(), 'version': uuid.uuid4().hex}
            self._put('drafts', 'takeover:' + str(chat_id), row, account_id)
            if enabled:
                for draft in self.store.list('ai_drafts', account_id):
                    if draft.get('chat_id') == chat_id and draft.get('status') not in {'adopted', 'cancelled'}:
                        draft.update(status='cancelled', cancelled_at=_now())
                        self._put('ai_drafts', draft['id'], draft, account_id)
            return row
        raise ValueError('不支持的聊天操作')

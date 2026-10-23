"""Account-scoped, live-only Telegram listener with durable write boundaries."""
from __future__ import annotations

import asyncio
import copy
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

from telethon import errors, events, types, utils
from telethon.tl.custom import ParticipantPermissions

UTC = timezone.utc
_COUNT_KEYS = ('added', 'skipped', 'preview', 'failed', 'pending')


def _now():
    return datetime.now(UTC)


def _date(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except (TypeError, ValueError):
            return None
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _counts():
    return dict.fromkeys(_COUNT_KEYS, 0)


@dataclass
class _Runtime:
    state: str = 'stopped'
    run: dict | None = None
    gate: bool = False
    connected: bool = False
    epoch: datetime = field(default_factory=_now)
    generation: int = 0
    queue: deque = field(default_factory=deque)
    pending_users: set = field(default_factory=set)
    worker: asyncio.Task | None = None
    current: dict | None = None
    client: object = None
    handler: object = None
    resume_state: str | None = None
    error: str | None = None


class ListenerService:
    MAX_PENDING = 100
    DRAIN_TIMEOUT = 30

    def __init__(self, app):
        self.app = app
        self.store = app.store
        self._runtimes = {}
        self._closed = False
        # An interrupted request is evidence of uncertainty, never a retry ticket.
        for record in self.store.list('listener_processed'):
            if record.get('status') == 'inflight':
                record.update(status='unverified', detail='程序中断，须先核实联系人状态')
                self.store.put('listener_processed', str(record['user_id']), record,
                               account_id=record['account_id'])

    def _runtime(self, account_id):
        if not isinstance(account_id, str) or not account_id:
            raise ValueError('请明确选择账号')
        if account_id not in self._runtimes:
            run = self.store.get('listener_run', 'current', account_id=account_id)
            rt = _Runtime(run=run)
            self._runtimes[account_id] = rt
            if run:
                lost = run.setdefault('counts', _counts()).get('pending', 0)
                run['counts']['pending'] = 0
                if lost:
                    run['counts']['skipped'] += lost
                    run.setdefault('skip_reasons', {})['程序中断，未重放排队项'] = lost
                run['state'] = 'stopped'
                self._save_run(account_id, run)
            total = self._totals(account_id)
            if total.get('pending'):
                total['pending'] = 0
                self.store.put('listener_totals', 'current', total, account_id=account_id)
        return self._runtimes[account_id]

    def _config(self, account_id):
        return self.store.get('listener_config', 'current',
                              {'account_id': account_id, 'group_ids': [], 'mode': 'preview', 'version': 0,
                               'groups': [], 'groups_refreshed_at': None}, account_id=account_id)

    def _totals(self, account_id):
        return self.store.get('listener_totals', 'current',
                              {'account_id': account_id, **_counts()}, account_id=account_id)

    def _save_run(self, account_id, run):
        self.store.put('listener_run', run['id'], run, account_id=account_id)
        rt = self._runtimes.get(account_id)
        if rt is None or rt.run is run or (rt.run and rt.run['id'] == run['id']):
            self.store.put('listener_run', 'current', run, account_id=account_id)
        self.app.bump()

    def _increment(self, account_id, run, key, amount=1):
        run['counts'][key] = max(0, run['counts'].get(key, 0) + amount)
        total = self._totals(account_id)
        total[key] = max(0, total.get(key, 0) + amount)
        self.store.put('listener_totals', 'current', total, account_id=account_id)
        self._save_run(account_id, run)

    def _record(self, account_id, run, result, reason, *, group_id=None, user_id=None,
                message_id=None, version=None, mode=None):
        self.app.log(account_id, 'listener', reason, run_id=run['id'],
                     config_version=version if version is not None else run['version'],
                     group_id=group_id, user_id=user_id, message_id=message_id,
                     mode=mode or run['mode'], result=result)

    def _skip(self, account_id, run, reason, **fields):
        self._increment(account_id, run, 'skipped')
        reasons = run.setdefault('skip_reasons', {})
        reasons[reason] = reasons.get(reason, 0) + 1
        self._save_run(account_id, run)
        self._record(account_id, run, 'skipped', reason, **fields)

    def _wait_state(self, account_id):
        state = self.app.wait_state(account_id)
        return state if isinstance(state, dict) else {}

    def snapshot(self, account_id):
        rt = self._runtime(account_id)
        config = self._config(account_id)
        state = rt.state
        if state == 'running' and not self.app.automation_allowed(account_id) and not self.app.global_paused:
            state = 'waiting'
        return copy.deepcopy({'state': state, 'mode': rt.run['mode'] if rt.run else config.get('mode', 'preview'),
                              'config': {key: config.get(key) for key in ('group_ids', 'mode', 'version')},
                              'run': rt.run, 'totals': self._totals(account_id),
                              'groups': config.get('groups', []),
                              'groups_refreshed_at': config.get('groups_refreshed_at'),
                              'wait_until': self._wait_state(account_id).get('wait_until'), 'error': rt.error})

    async def ready(self, account_id):
        if self._closed:
            return
        session = self.app.accounts.require(account_id)
        rt = self._runtime(account_id)
        if rt.client is not session.client or rt.handler is None:
            if rt.client is not None and rt.handler is not None:
                rt.client.remove_event_handler(rt.handler)
            async def on_message(event):
                await self.handle_event(account_id, event)
            rt.client, rt.handler = session.client, on_message
            # Deliberately no catch-up, history, edit, or member-list subscription.
            session.client.add_event_handler(on_message, events.NewMessage(incoming=True))
        was_offline = not rt.connected
        rt.connected = True
        if was_offline:
            rt.epoch = max(_now(), _date(getattr(session, 'epoch', None)) or _now())
            rt.generation += 1
            if rt.state == 'offline' and rt.resume_state == 'running':
                rt.state, rt.gate = 'running', True
                rt.resume_state = None
        await self._reconcile(account_id)
        self.app.bump()

    async def offline(self, account_id):
        rt = self._runtime(account_id)
        rt.connected, rt.gate = False, False
        rt.generation += 1
        if rt.state in ('running', 'waiting'):
            rt.resume_state, rt.state = 'running', 'offline'
        await self._cancel_pending(account_id, '账号离线，取消未执行项')
        self.app.bump()

    async def _reconcile(self, account_id):
        unresolved = [r for r in self.store.list('listener_processed', account_id)
                      if r.get('status') == 'unverified']
        if not unresolved:
            return
        try:
            rows = await self.app.contacts.refresh(account_id)
        except Exception:
            # A failed/partial read supplies no proof of successful mutation.
            return
        existing = self._contact_ids(rows)
        for record in unresolved:
            latest = self.store.get('listener_processed', str(record['user_id']), account_id=account_id)
            if not latest or latest.get('status') != 'unverified':
                continue
            if record['user_id'] in existing:
                record.update(status='added', verified_at=_now().isoformat(),
                              detail='已通过当前账号联系人列表核实')
                self.store.put('listener_processed', str(record['user_id']), record, account_id=account_id)
                # Attribute verification to the original run, never today's active batch.
                run = self.store.get('listener_run', record.get('run_id', ''), account_id=account_id)
                if run:
                    rt = self._runtime(account_id)
                    if rt.run and rt.run['id'] == run['id']:
                        run = rt.run
                    self._increment(account_id, run, 'added')
                else:
                    total = self._totals(account_id)
                    total['added'] += 1
                    self.store.put('listener_totals', 'current', total, account_id=account_id)
                self.app.bump()

    @staticmethod
    def _contact_ids(rows):
        result = set()
        for row in rows or []:
            if isinstance(row, dict) and (row.get('current') is False or row.get('write_status') in ('inflight', 'unverified')):
                continue
            value = row.get('user_id', row.get('id')) if isinstance(row, dict) else getattr(row, 'id', None)
            if isinstance(value, int) and value > 0:
                result.add(value)
        return result

    @staticmethod
    def _group_type(entity):
        if isinstance(entity, (types.Chat, types.ChatForbidden, types.PeerChat)):
            return 'basic'
        if (isinstance(entity, (types.Channel, types.ChannelForbidden))
                and getattr(entity, 'megagroup', False) and not getattr(entity, 'broadcast', False)
                and not getattr(entity, 'monoforum', False)):
            return 'supergroup'
        return None

    @classmethod
    def _permission_hint(cls, entity, *, member=False):
        """Use only returned entity fields; this never performs an extra RPC."""
        if (isinstance(entity, (types.ChatForbidden, types.ChannelForbidden))
                or any(getattr(entity, key, False) for key in ('left', 'kicked', 'deactivated', 'migrated_to'))):
            return False, '当前账号已退出、无权访问或该群已停用迁移'
        kind = cls._group_type(entity)
        if kind is None or getattr(entity, 'min', False):
            return None, '群资料不完整，请显式核实权限'
        banned = getattr(entity, 'banned_rights', None)
        if isinstance(banned, types.ChatBannedRights) and (banned.invite_users or banned.view_messages):
            return False, '当前账号被限制邀请成员或查看该群'
        if not member:
            if getattr(entity, 'creator', False):
                return True, '群资料显示当前账号为群主，尚未显式核实'
            admin = getattr(entity, 'admin_rights', None)
            if isinstance(admin, types.ChatAdminRights):
                if kind == 'basic' or admin.invite_users:
                    return True, '群资料显示管理员可邀请，尚未显式核实'
                return False, '当前管理员权限未包含邀请成员'
        defaults = getattr(entity, 'default_banned_rights', None)
        if not isinstance(defaults, types.ChatBannedRights):
            return None, '缺少群默认权限资料，请显式核实'
        if defaults.invite_users or defaults.view_messages:
            return False, '群默认权限禁止普通成员邀请或查看'
        return True, '群默认权限允许普通成员邀请'

    @classmethod
    def _checked_permission(cls, entity, permissions, own_id):
        if (not isinstance(permissions, ParticipantPermissions)
                or permissions.is_chat != isinstance(entity, types.Chat)):
            return None, '返回的成员权限资料不完整，无法核实', None
        participant = permissions.participant
        basic = isinstance(entity, types.Chat)
        members = ((types.ChatParticipant, types.ChatParticipantAdmin, types.ChatParticipantCreator)
                   if basic else (types.ChannelParticipant, types.ChannelParticipantSelf,
                                  types.ChannelParticipantAdmin, types.ChannelParticipantCreator))
        if not basic and isinstance(participant, (types.ChannelParticipantBanned, types.ChannelParticipantLeft)):
            peer = participant.peer
            if not isinstance(peer, types.PeerUser) or peer.user_id != own_id:
                return None, '返回的成员身份与当前账号不一致', None
            if isinstance(participant, types.ChannelParticipantLeft) or participant.left:
                return False, '当前账号已不在该群中', False
            bans = participant.banned_rights
            if not isinstance(bans, types.ChatBannedRights):
                return None, '当前账号的成员限制资料不完整', None
            if bans.view_messages or bans.invite_users:
                return False, '当前账号的成员限制禁止查看或邀请', not bool(bans.view_messages)
            # A restricted participant may still be a member; unrelated send
            # restrictions alone are not proof that inviting is forbidden.
            allowed, reason = cls._permission_hint(entity, member=True)
            return allowed, reason, True
        if not isinstance(participant, members) or getattr(participant, 'user_id', None) != own_id:
            return None, '无法核实当前账号是该群的有效成员', None
        if permissions.is_creator:
            return True, '已核实当前账号为群主，可以邀请成员', True
        if permissions.is_admin:
            if basic:
                return True, '已核实当前账号为普通群管理员，可以邀请成员', True
            rights = getattr(participant, 'admin_rights', None)
            if not isinstance(rights, types.ChatAdminRights):
                return None, '管理员邀请权限资料不完整', True
            allowed = bool(rights.invite_users)
            return allowed, ('已核实管理员拥有邀请成员权限' if allowed else '已核实管理员没有邀请成员权限'), True
        allowed, reason = cls._permission_hint(entity, member=True)
        return allowed, reason, True

    def _save_group_inspection(self, account_id, group_id, fields, client):
        if self.app.accounts.require(account_id).client is not client:
            raise ValueError('账号连接已变化，请重新核实群权限')
        config = self._config(account_id)
        group = next((row for row in config.get('groups', []) if row['id'] == group_id), None)
        if group is None:
            raise ValueError('该群已不在当前账号群列表，请刷新后核实')
        group.update(fields)
        self.store.put('listener_config', 'current', config, account_id=account_id)
        self.app.bump()
        return {'group': copy.deepcopy(group), 'groups': copy.deepcopy(config['groups']),
                'groups_refreshed_at': config.get('groups_refreshed_at')}

    async def _inspect_group(self, account_id, group_id):
        session = self.app.accounts.require(account_id)
        if type(group_id) is not int or group_id >= 0:
            raise ValueError('请选择当前账号的标准群 ID')
        if not any(group['id'] == group_id for group in self._config(account_id).get('groups', [])):
            raise ValueError('只能核实当前账号已保存群列表中的群，请先刷新群列表')
        client, own_id = session.client, session.own_id
        if type(own_id) is not int or own_id <= 0:
            raise ValueError('当前账号身份尚未核实，请重新登录')
        fields = {'can_invite': None, 'permission_checked_at': None}
        try:
            entity = await client.get_entity(group_id)
            if utils.get_peer_id(entity) != group_id:
                raise ValueError('返回的目标群 ID 不一致，请刷新群列表')
            kind = self._group_type(entity)
            if kind is None:
                raise ValueError('目标必须是普通群或超级群，不能是广播频道')
            fields.update(group_type=kind, title=getattr(entity, 'title', None) or str(group_id),
                          refreshed_at=_now().isoformat())
            if (isinstance(entity, (types.ChatForbidden, types.ChannelForbidden))
                    or any(getattr(entity, key, False) for key in ('left', 'kicked', 'deactivated', 'migrated_to'))):
                fields.update(can_invite=False, available=False,
                              permission_reason='当前账号无权访问、已退出或该群已停用迁移',
                              permission_checked_at=_now().isoformat())
            else:
                permissions = await client.get_permissions(entity, 'me')
                allowed, reason, member = self._checked_permission(entity, permissions, own_id)
                fields.update(can_invite=allowed, permission_reason=reason,
                              permission_checked_at=_now().isoformat() if allowed is not None else None)
                if member is not None:
                    fields['available'] = member
        except (errors.UserNotParticipantError, errors.ChannelPrivateError, errors.ChatForbiddenError):
            fields.update(can_invite=False, available=False, permission_reason='已核实当前账号不是成员或无权访问该群',
                          permission_checked_at=_now().isoformat())
        except (errors.ChatAdminRequiredError, errors.ChatAdminInviteRequiredError, errors.ChatWriteForbiddenError):
            # A rejected permission read does not establish an invitation ban.
            fields.update(can_invite=None, permission_checked_at=None,
                          permission_reason='当前账号无权读取完整权限，无法核实是否可邀请')
        except ValueError:
            # Resolution/identity failures invalidate an older verified allow.
            # Preserve the original row identity and avoid exposing raw errors.
            self._save_group_inspection(account_id, group_id,
                {'can_invite': None, 'permission_checked_at': None,
                 'permission_reason': '群资料解析或身份核对失败，请刷新后重新核实'}, client)
            raise ValueError('群资料解析或身份核对失败，请刷新后重新核实') from None
        except Exception as error:
            wait = isinstance(error, (errors.FloodError, errors.PeerFloodError,
                                      errors.UnauthorizedError, ConnectionError, OSError))
            if wait and callable(getattr(self.app, 'record_wait', None)):
                self.app.record_wait(account_id, error)
            fields.update(can_invite=None, permission_checked_at=None,
                          permission_reason=('账号处于平台等待或连接异常，权限尚未核实' if wait
                                             else '读取群权限失败，无法核实是否可邀请'))
            self._save_group_inspection(account_id, group_id, fields, client)
            raise ValueError(fields['permission_reason']) from None
        return self._save_group_inspection(account_id, group_id, fields, client)

    async def _refresh_groups(self, account_id):
        session = self.app.accounts.require(account_id)
        config = self._config(account_id)
        old = {g['id']: g for g in config.get('groups', [])}
        groups = []
        async for dialog in session.client.iter_dialogs():
            entity = dialog.entity
            is_group = getattr(dialog, 'is_group', False) or isinstance(entity, types.Chat) or getattr(entity, 'megagroup', False)
            if (not is_group or self._group_type(entity) is None
                    or isinstance(entity, (types.ChatForbidden, types.ChannelForbidden))
                    or any(getattr(entity, key, False) for key in ('left', 'kicked', 'deactivated', 'migrated_to'))):
                continue
            # Dialog/entity IDs differ for channels; never manufacture -100 IDs.
            gid = utils.get_peer_id(entity)
            group = {'id': gid, 'title': getattr(dialog, 'name', None) or getattr(entity, 'title', None) or str(gid),
                     'available': True, 'group_type': self._group_type(entity),
                     'permission_checked_at': None, 'refreshed_at': _now().isoformat()}
            group['can_invite'], group['permission_reason'] = self._permission_hint(entity)
            previous = old.get(gid, {})
            if previous.get('can_invite') is group['can_invite'] and previous.get('permission_checked_at'):
                group['permission_checked_at'] = previous['permission_checked_at']
            count = getattr(entity, 'participants_count', None)
            if count is not None:
                group['member_count'] = count
            groups.append(group)
        # Network iteration yielded: preserve any concurrently saved configuration.
        config = self._config(account_id)
        old = {g['id']: g for g in config.get('groups', [])}
        found = {g['id'] for g in groups}
        for gid in config.get('group_ids', []):
            if gid not in found:
                groups.append({**old.get(gid, {'id': gid, 'title': str(gid), 'group_type': None}),
                               'available': False, 'can_invite': False, 'permission_checked_at': None,
                               'permission_reason': '该群当前不可访问，请刷新或显式核实'})
        config.update(groups=groups, groups_refreshed_at=_now().isoformat())
        self.store.put('listener_config', 'current', config, account_id=account_id)
        await self._reconcile(account_id)
        self.app.bump()
        return self.snapshot(account_id)

    async def action(self, name, data):
        account_id = data.get('account_id')
        rt = self._runtime(account_id)
        if name == 'refresh_groups':
            return await self._refresh_groups(account_id)
        if name == 'inspect_group':
            return await self._inspect_group(account_id, data.get('group_id'))
        if name == 'save_listener':
            self.app.accounts.require(account_id)
            config = self._config(account_id)
            if type(data.get('expected_version')) is not int or data['expected_version'] != config['version']:
                raise ValueError('监听配置已变化，请刷新后重新保存')
            mode = data.get('mode')
            ids = data.get('group_ids')
            if mode not in ('preview', 'live'):
                raise ValueError('请选择仅预览或实际添加模式')
            if not isinstance(ids, list) or any(type(gid) is not int or gid >= 0 for gid in ids):
                raise ValueError('群组必须使用当前账号的标准群 ID')
            ids = list(dict.fromkeys(ids))
            available = {g['id'] for g in config.get('groups', []) if g.get('available')}
            if any(gid not in available for gid in ids):
                raise ValueError('所选群已不可用，请刷新群列表并重新选择')
            active = rt.state in ('running', 'waiting', 'paused', 'offline') and rt.run is not None
            if active and data.get('apply_confirmed') is not True:
                raise ValueError('请确认保存并应用到后续新消息')
            rt.generation += 1
            rt.epoch = _now()
            await self._cancel_pending(account_id, '配置版本变化，取消旧配置未执行项')
            # Awaiting cancellation may yield: compare again before committing a version.
            latest = self._config(account_id)
            if latest['version'] != config['version']:
                raise ValueError('监听配置已变化，请刷新后重新保存')
            config.update(group_ids=ids, mode=mode, version=config['version'] + 1, saved_at=_now().isoformat())
            self.store.put('listener_config', 'current', config, account_id=account_id)
            if active:
                rt.run.update(version=config['version'], mode=mode, group_ids=ids.copy())
                rt.run.setdefault('config_history', []).append({'version': config['version'], 'mode': mode,
                                                               'group_ids': ids.copy(), 'applied_at': rt.epoch.isoformat()})
                self._save_run(account_id, rt.run)
            self.app.bump()
            return self.snapshot(account_id)
        if name == 'start_listener':
            if rt.state in ('running', 'waiting', 'stopping'):
                raise ValueError('监听已运行或正在停止')
            session = self.app.accounts.require(account_id)
            if self.app.global_paused:
                raise ValueError('全局已暂停，请先恢复')
            if self.app.tasks.contact_blocked(account_id):
                raise ValueError('联系人清理保护中，暂不能启动监听')
            config = self._config(account_id)
            if data.get('mode', config['mode']) != config['mode']:
                raise ValueError('启动模式必须与已保存配置一致')
            available = {g['id'] for g in config.get('groups', []) if g.get('available')}
            if not config['version'] or not config['group_ids'] or not any(g in available for g in config['group_ids']):
                raise ValueError('请先保存至少一个可用监听群')
            if rt.current is not None:
                raise ValueError('请先停止监听并等待在途请求完成')
            rt.generation += 1
            generation = rt.generation + (0 if rt.connected else 1)
            client = session.client
            if not rt.connected:
                await self.ready(account_id)
            else:
                await self._reconcile(account_id)
            if (rt.generation != generation or self._closed or not rt.connected
                    or self.app.accounts.require(account_id).client is not client
                    or self.app.global_paused or self.app.tasks.contact_blocked(account_id)
                    or self._config(account_id)['version'] != config['version']):
                raise ValueError('启动期间状态或配置已变化，请重新检查后启动')
            rt.epoch = max(_now(), _date(getattr(session, 'epoch', None)) or _now())
            rt.run = {'id': uuid4().hex, 'account_id': account_id, 'version': config['version'],
                      'mode': config['mode'], 'group_ids': config['group_ids'].copy(),
                      'started_at': rt.epoch.isoformat(), 'counts': _counts(), 'skip_reasons': {}, 'state': 'running'}
            rt.state, rt.gate, rt.error = 'running', True, None
            self._save_run(account_id, rt.run)
            return self.snapshot(account_id)
        if name == 'stop_listener':
            await self.stop(account_id)
            return self.snapshot(account_id)
        if name == 'pause_listener':
            await self.stop(account_id, reason='监听已暂停')
            rt.state = 'paused'
            if rt.run:
                rt.run['state'] = 'paused'
                self._save_run(account_id, rt.run)
            return self.snapshot(account_id)
        if name == 'resume_listener':
            self.app.accounts.require(account_id)
            if rt.state not in ('paused', 'waiting', 'error') or rt.run is None:
                raise ValueError('该账号没有可恢复的监听运行')
            if not self.app.automation_allowed(account_id):
                raise ValueError('账号仍在平台等待或全局暂停中')
            if self.app.tasks.contact_blocked(account_id):
                raise ValueError('联系人清理保护中，暂不能恢复监听')
            rt.generation += 1
            generation = rt.generation + (0 if rt.connected else 1)
            client = self.app.accounts.require(account_id).client
            if not rt.connected:
                await self.ready(account_id)
            else:
                await self._reconcile(account_id)
            if (rt.generation != generation or self._closed or not rt.connected
                    or self.app.accounts.require(account_id).client is not client
                    or not self.app.automation_allowed(account_id)
                    or self.app.tasks.contact_blocked(account_id)):
                raise ValueError('恢复期间状态或配置已变化，请重新检查后恢复')
            rt.epoch = _now()
            rt.state, rt.gate, rt.error = 'running', True, None
            rt.run['state'] = 'running'
            self._save_run(account_id, rt.run)
            return self.snapshot(account_id)
        if name == 'reset_processed':
            if data.get('confirmed') is not True:
                raise ValueError('重置会允许对象再次添加，请明确确认影响')
            if rt.pending_users:
                raise ValueError('请先停止监听并等待在途请求完成，再重置去重记录')
            all_ids = data.get('all') is True
            ids = data.get('user_ids')
            if not all_ids and (not isinstance(ids, list) or any(type(uid) is not int or uid <= 0 for uid in ids)):
                raise ValueError('请选择要重置的用户，或明确确认本账号全部记录')
            targets = {r['user_id'] for r in self.store.list('listener_processed', account_id)} if all_ids else set(ids)
            for record in self.store.list('listener_messages', account_id):
                if record.get('kind') == 'user_failed' and (all_ids or record.get('user_id') in targets):
                    targets.add(record['user_id'])
                    self.store.delete('listener_messages', f"failed:{record['user_id']}", account_id=account_id)
            for uid in targets:
                self.store.delete('listener_processed', str(uid), account_id=account_id)
            self.app.log(account_id, 'listener_reset', '已明确重置本账号处理记录，后续新消息可再次处理',
                         affected_count=len(targets), all=all_ids)
            return {'reset_count': len(targets), 'impact': '后续新消息可能再次添加这些对象'}
        raise ValueError('未知监听操作')

    def _is_active(self, account_id, rt):
        return (not self._closed and rt.gate and rt.connected and rt.state in ('running', 'waiting')
                and rt.run is not None and not self.app.global_paused)

    async def handle_event(self, account_id, event, *, source='live'):
        rt = self._runtime(account_id)
        if source != 'live' or not self._is_active(account_id, rt):
            return
        run = rt.run
        message = getattr(event, 'message', None)
        # MessageEdited.Event is a subclass of NewMessage.Event in Telethon.
        update = getattr(event, 'original_update', None)
        if (isinstance(event, events.MessageEdited.Event) or isinstance(message, types.MessageService)
                or getattr(message, 'action', None) is not None or getattr(event, 'is_edit', False)
                or isinstance(update, (types.UpdateEditMessage, types.UpdateEditChannelMessage))):
            self._skip(account_id, run, '编辑或服务消息不触发监听')
            return
        if not isinstance(message, types.Message):
            self._skip(account_id, run, '不是可处理的新消息')
            return
        if getattr(event, 'is_group', False) is not True:
            self._skip(account_id, run, '不是群消息')
            return
        session = self.app.accounts.require(account_id)
        event_client = getattr(event, 'client', None)
        if event_client is not None and event_client is not session.client:
            self._skip(account_id, run, '事件所属账号不匹配')
            return
        received = _date(getattr(message, 'date', None))
        boundary = max(_date(run['started_at']), rt.epoch, _date(getattr(session, 'epoch', None)) or rt.epoch)
        gid, mid = getattr(event, 'chat_id', None), getattr(message, 'id', None)
        fields = {'group_id': gid, 'message_id': mid, 'version': run['version']}
        available = {g['id'] for g in self._config(account_id).get('groups', []) if g.get('available')}
        if gid not in run['group_ids'] or gid not in available:
            self._skip(account_id, run, '群不在当前可用监听范围', **fields)
            return
        if received is None or received <= boundary:
            self._skip(account_id, run, '消息不晚于当前接收边界', **fields)
            return
        if type(mid) is not int or mid <= 0:
            self._skip(account_id, run, '消息身份无效', **fields)
            return
        if not self.app.automation_allowed(account_id):
            rt.state = 'waiting' if not self.app.global_paused else rt.state
            self._skip(account_id, run, '账号平台等待中，未加入队列', **fields)
            return
        if rt.state == 'waiting':
            # Elapsed waits establish another strict boundary; no backlog replay.
            rt.state, rt.epoch = 'running', _now()
            rt.generation += 1
            if received <= rt.epoch:
                self._skip(account_id, run, '平台等待恢复前消息不补处理', **fields)
                return
        message_key = f'{gid}:{mid}'
        if self.store.get('listener_messages', message_key, account_id=account_id):
            self._skip(account_id, run, '消息已处理', **fields)
            return
        generation, version = rt.generation, run['version']
        self.store.put('listener_messages', message_key,
                       {'account_id': account_id, 'group_id': gid, 'message_id': mid, 'run_id': run['id'],
                        'config_version': version, 'status': 'seen', 'at': _now().isoformat()}, account_id=account_id)
        uid = getattr(event, 'sender_id', None)
        from_id = getattr(message, 'from_id', None)
        if type(uid) is not int or uid <= 0 or not isinstance(from_id, types.PeerUser) or from_id.user_id != uid:
            self._skip(account_id, run, '匿名或非用户身份', **fields)
            return
        fields['user_id'] = uid
        try:
            person = await event.get_sender()
        except Exception:
            self._skip(account_id, run, '无法解析当前账号发言者', **fields)
            return
        if generation != rt.generation or run is not rt.run or not self._is_active(account_id, rt):
            self._skip(account_id, run, '接收边界已变化，取消旧候选', **fields)
            return
        if getattr(person, 'id', None) != uid or isinstance(person, (types.Channel, types.Chat)):
            self._skip(account_id, run, '无法识别真实用户', **fields)
            return
        reason = None
        if uid == getattr(session, 'own_id', None) or getattr(person, 'is_self', False):
            reason = '自己不处理'
        elif getattr(person, 'bot', False):
            reason = '机器人不处理'
        elif getattr(person, 'deleted', False):
            reason = '已删除用户不处理'
        processed = self.store.get('listener_processed', str(uid), account_id=account_id)
        if not reason and processed:
            reason = '已处理或结果待核实'
        if not reason and (getattr(person, 'contact', False) or uid in self._contact_ids(self.app.contacts.cached(account_id))):
            if run['mode'] == 'live':
                self.store.put('listener_processed', str(uid), {'account_id': account_id, 'user_id': uid, 'status': 'existing',
                               'run_id': run['id'], 'config_version': version, 'at': _now().isoformat()}, account_id=account_id)
            reason = '已是联系人'
        if not reason and self.store.get('listener_messages', f'failed:{uid}', account_id=account_id):
            reason = '此前明确失败，需人工检查后重置'
        if not reason and uid in rt.pending_users:
            reason = '该用户已在本账号队列中'
        if reason:
            self._skip(account_id, run, reason, **fields)
            return
        if self.app.tasks.contact_blocked(account_id):
            self._skip(account_id, run, '联系人清理保护中', **fields)
            return
        if run['mode'] == 'preview':
            self._increment(account_id, run, 'preview')
            self._record(account_id, run, 'preview', '仅预览候选，未修改联系人', **fields)
            return
        if len(rt.pending_users) >= self.MAX_PENDING:
            self._skip(account_id, run, '本账号待处理队列已满', **fields)
            return
        item = {'user': person, 'user_id': uid, 'group_id': gid, 'message_id': mid,
                'version': version, 'generation': generation, 'mode': run['mode'], 'run': run, 'submitted': False}
        rt.pending_users.add(uid)
        rt.queue.append(item)
        self._increment(account_id, run, 'pending')
        if rt.worker is None or rt.worker.done():
            rt.worker = asyncio.create_task(self._work(account_id), name=f'listener:{account_id}')

    def _eligible_item(self, account_id, rt, item):
        return (self._is_active(account_id, rt) and item['generation'] == rt.generation
                and item['version'] == rt.run['version'] and self.app.automation_allowed(account_id)
                and not self.app.tasks.contact_blocked(account_id))

    def allow_submission(self, account_id, user_id):
        """RPC-dispatch gate used synchronously by ContactService, after queuing."""
        rt = self._runtime(account_id)
        item = rt.current
        return bool(item and item['user_id'] == user_id and not item['submitted']
                    and self._eligible_item(account_id, rt, item))

    def mark_submitted(self, account_id, user_id):
        """Persist uncertainty immediately before the shared client submits RPC."""
        if not self.allow_submission(account_id, user_id):
            raise ValueError('监听候选已失效，未提交联系人添加')
        item = self._runtime(account_id).current
        record = item['record']
        record['submitted_at'] = _now().isoformat()
        self.store.put('listener_processed', str(user_id), record, account_id=account_id)
        item['submitted'] = True

    def _finish_item(self, account_id, item):
        rt = self._runtime(account_id)
        rt.pending_users.discard(item['user_id'])
        self._increment(account_id, item['run'], 'pending', -1)

    async def _work(self, account_id):
        rt = self._runtime(account_id)
        try:
            while rt.queue:
                item = rt.queue.popleft()
                rt.current = item
                run, uid = item['run'], item['user_id']
                fields = {key: item[key] for key in ('user_id', 'group_id', 'message_id', 'version', 'mode')}
                record = None
                try:
                    async with self.app.tasks.contact_lock(account_id):
                        if not self._eligible_item(account_id, rt, item):
                            self._skip(account_id, run, '状态或联系人保护已变化，取消未执行项', **fields)
                            continue
                        if uid in self._contact_ids(self.app.contacts.cached(account_id)):
                            self.store.put('listener_processed', str(uid), {'account_id': account_id, 'user_id': uid,
                                           'status': 'existing', 'run_id': run['id'], 'config_version': item['version'],
                                           'at': _now().isoformat()}, account_id=account_id)
                            self._skip(account_id, run, '等待期间已成为联系人', **fields)
                            continue
                        record = {'account_id': account_id, 'user_id': uid, 'run_id': run['id'],
                                  'config_version': item['version'], 'group_id': item['group_id'],
                                  'message_id': item['message_id'], 'mode': item['mode'], 'status': 'inflight'}
                        item['record'] = record
                        # add_person itself schedules through app.execute; nesting it deadlocks.
                        await self.app.contacts.add_person(account_id, item['user'], source='listener')
                        if not item['submitted']:
                            raise ValueError('联系人服务未确认请求提交边界')
                        record.update(status='added', finished_at=_now().isoformat())
                        self.store.put('listener_processed', str(uid), record, account_id=account_id)
                        self._increment(account_id, run, 'added')
                        self._record(account_id, run, 'added', '已添加联系人', **fields)
                except asyncio.CancelledError:
                    if record and item['submitted']:
                        record.update(status='unverified', detail='请求中断，须先核实联系人状态')
                        self.store.put('listener_processed', str(uid), record, account_id=account_id)
                        self._increment(account_id, run, 'failed')
                        self._record(account_id, run, 'unverified', '请求中断，结果待核实', **fields)
                    else:
                        self._skip(account_id, run, '取消未提交项', **fields)
                    raise
                except Exception as exc:
                    await self._write_error(account_id, item, record, exc)
                finally:
                    self._finish_item(account_id, item)
                    rt.current = None
        finally:
            if rt.state == 'stopping':
                rt.state = 'stopped'
                if rt.run:
                    rt.run.update(state='stopped', stopped_at=_now().isoformat())
                    self._save_run(account_id, rt.run)

    async def _write_error(self, account_id, item, record, exc):
        rt, run = self._runtime(account_id), item['run']
        name = type(exc).__name__
        if not item['submitted']:
            self._skip(account_id, run, '提交前状态变化，未执行添加',
                       **{key: item[key] for key in ('user_id', 'group_id', 'message_id', 'version', 'mode')})
            return
        definite = name in {'ValueError', 'UserPrivacyRestrictedError', 'UserIdInvalidError', 'InputUserDeactivatedError',
                            'ContactIdInvalidError', 'FloodWaitError', 'FloodPremiumWaitError', 'PeerFloodError',
                            'UserRestrictedError', 'ChatWriteForbiddenError'}
        fields = {key: item[key] for key in ('user_id', 'group_id', 'message_id', 'version', 'mode')}
        if record:
            if definite:
                self.store.delete('listener_processed', str(item['user_id']), account_id=account_id)
                self.store.put('listener_messages', f"failed:{item['user_id']}",
                               {**record, 'kind': 'user_failed', 'status': 'failed', 'error_type': name}, account_id=account_id)
            else:
                record.update(status='unverified', detail='请求结果不明，须先核实联系人状态', error_type=name)
                self.store.put('listener_processed', str(item['user_id']), record, account_id=account_id)
        self._increment(account_id, run, 'failed')
        self._record(account_id, run, 'failed' if definite else 'unverified',
                     '添加明确失败，需人工检查' if definite else '添加结果待核实，不会自动重试', **fields)
        if name in {'FloodWaitError', 'FloodPremiumWaitError', 'PeerFloodError', 'UserRestrictedError'} or not self.app.automation_allowed(account_id):
            rt.state, rt.error = 'waiting', '账号平台限制，请等待并人工检查后恢复'
            rt.gate = False
        elif not definite:
            rt.state, rt.gate, rt.error = 'error', False, '连接或请求异常，结果待核实，请检查后恢复'
        if not rt.gate:
            # This callback runs inside the worker: do not cancel or await itself.
            self._drop_queue(account_id, '账号暂停，取消未执行项')
        self.app.bump()

    def _drop_queue(self, account_id, reason):
        rt = self._runtime(account_id)
        while rt.queue:
            item = rt.queue.popleft()
            self._skip(account_id, item['run'], reason,
                       **{key: item[key] for key in ('user_id', 'group_id', 'message_id', 'version', 'mode')})
            self._finish_item(account_id, item)

    async def _cancel_pending(self, account_id, reason):
        rt = self._runtime(account_id)
        self._drop_queue(account_id, reason)
        if rt.current is not None and not rt.current['submitted'] and rt.worker is not asyncio.current_task():
            rt.worker.cancel()
            try:
                await rt.worker
            except asyncio.CancelledError:
                pass

    async def stop(self, account_id, reason='', drain=True):
        rt = self._runtime(account_id)
        rt.gate = False
        rt.generation += 1
        rt.resume_state = None
        rt.state = 'stopping'
        self.app.bump()
        await self._cancel_pending(account_id, reason or '监听停止，取消未执行项')
        worker = rt.worker
        if worker is not None and not worker.done():
            if not drain:
                return
            try:
                await asyncio.wait_for(asyncio.shield(worker), self.DRAIN_TIMEOUT)
            except asyncio.TimeoutError:
                # The scheduler shields dispatched RPCs. Cancelling their caller
                # would release the contact lock while Telegram may still write.
                item = rt.current
                if item and item['submitted']:
                    record = item['record']
                    record.update(status='unverified', detail='在途请求尚未结束，结果待核实')
                    self.store.put('listener_processed', str(item['user_id']), record, account_id=account_id)
                rt.error = '在途添加尚未结束，已保留联系人写入保护，请等待后重试停止'
                self.app.bump()
                raise ValueError(rt.error) from None
            except asyncio.CancelledError:
                # Caller cancellation must not turn an ongoing request into success.
                raise
        rt.state = 'stopped'
        if rt.run:
            rt.run.update(state='stopped', stopped_at=_now().isoformat())
            self._save_run(account_id, rt.run)
        self.app.bump()

    async def close(self):
        self._closed = True
        await asyncio.gather(*(self.stop(aid, reason='后台退出，取消未执行项') for aid in list(self._runtimes)))
        for rt in self._runtimes.values():
            if rt.client is not None and rt.handler is not None:
                rt.client.remove_event_handler(rt.handler)
                rt.handler = None

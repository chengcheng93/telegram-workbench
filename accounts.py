"""Independent Telegram authorization and validated local account bindings.

Importing this module neither opens sessions nor connects Telegram. Login codes,
passwords and phone-code hashes are passed to Telegram only in memory.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import fcntl
import os
from pathlib import Path
import re
from typing import Any

from storage import new_id, now

_RECONNECT_DELAYS = (1, 2, 4)
_AUTH_ERRORS = frozenset(('AuthKeyUnregisteredError', 'SessionRevokedError',
                          'SessionExpiredError', 'UnauthorizedError',
                          'AuthKeyDuplicatedError', 'AuthKeyNotFound',
                          'UserDeactivatedError', 'UserDeactivatedBanError'))


class _PublicAccountError(ValueError):
    """An error whose message is authored here and safe for the local UI."""



def _default_client_factory(session_path: Path, api_id: int, api_hash: str):
    from telethon import TelegramClient
    return TelegramClient(str(session_path), api_id, api_hash,
                          sequential_updates=True, catch_up=False,
                          flood_sleep_threshold=0, request_retries=0, auto_reconnect=False,
                          raise_last_call_error=True)


def _phone_mask(phone: Any) -> str:
    digits = re.sub(r'\D', '', str(phone or ''))
    if not digits:
        return ''
    if len(digits) < 7:
        return '*' * len(digits)
    return '+' + digits[:2] + '*' * (len(digits) - 6) + digits[-4:]


@dataclass
class AccountSession:
    id: str
    profile: dict
    client: Any = None
    own_id: int | None = None
    logged_in: bool = False
    stage: str = 'phone'
    status: str = 'offline'
    wait_until: str | None = None
    epoch: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    error: str = ''
    login_channel: str = ''
    _phone: str | None = field(default=None, repr=False)
    _phone_hash: str | None = field(default=None, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    _lock_fd: int | None = field(default=None, repr=False)
    _auth_task: asyncio.Task | None = field(default=None, repr=False)
    _generation: int = field(default=0, repr=False)
    _watch_task: asyncio.Task | None = field(default=None, repr=False)

    @property
    def connected(self) -> bool:
        return bool(self.client is not None and self.client.is_connected())


class AccountManager:
    def __init__(self, app, client_factory=None):
        self.app = app
        self.store = app.store
        self.root = Path(app.root)
        self.client_factory = client_factory or _default_client_factory
        self.sessions: dict[str, AccountSession] = {}
        for profile in self.store.list('accounts', account_id=''):
            account_id = profile.get('id', '') if isinstance(profile, dict) else ''
            if re.fullmatch(r'[0-9a-f]{32}', account_id) and not profile.get('deleted'):
                self.sessions[account_id] = AccountSession(account_id, profile,
                                                         wait_until=profile.get('wait_until'))

    def get(self, account_id) -> AccountSession:
        session = self.sessions.get(account_id)
        if session is None:
            raise _PublicAccountError('未找到该账号，请重新选择账号')
        return session

    def require(self, account_id) -> AccountSession:
        session = self.get(account_id)
        if not session.connected or not session.logged_in or session.own_id is None:
            raise _PublicAccountError('该账号尚未连接并完成登录')
        return session

    def _profile(self, session):
        profile = self.store.get('accounts', session.id, session.profile)
        if isinstance(profile, dict) and not profile.get('deleted'):
            session.profile = profile
        return session.profile

    def public(self) -> list[dict]:
        result = []
        for session in self.sessions.values():
            profile = self._profile(session)
            result.append({
                'id': session.id, 'name': profile.get('name', ''),
                'telegram_id': profile.get('telegram_id'),
                'username': profile.get('username', ''),
                'phone_masked': profile.get('phone_masked', ''),
                'status': session.status, 'stage': session.stage,
                'connected': session.connected,
                'logged_in': bool(session.logged_in and session.connected),
                'last_sync': profile.get('last_sync'),
                'wait_until': session.wait_until or profile.get('wait_until'),
                'login_channel': session.login_channel, 'error': session.error,
            })
        return result

    def _persist(self, session):
        profile = dict(self._profile(session))
        profile.update(status=session.status, stage=session.stage,
                       wait_until=session.wait_until)
        session.profile = profile
        self.store.put('accounts', session.id, profile)
        self.app.bump()

    def _event(self, session, action, detail):
        self.app.log(session.id, action, detail)

    def _directory(self, session):
        directory = self.root / 'accounts' / session.id
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory.chmod(0o700)
        directory.parent.chmod(0o700)
        return directory

    def _path(self, session):
        return self._directory(session) / 'account.session'

    def _acquire(self, session):
        if session._lock_fd is not None:
            return
        lock_path = self._directory(session) / 'account.lock'
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        os.fchmod(fd, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise _PublicAccountError('该账号会话正由另一个后台持有，请先断开另一连接') from None
        session._lock_fd = fd

    def _release(self, session):
        if session._lock_fd is not None:
            fcntl.flock(session._lock_fd, fcntl.LOCK_UN)
            os.close(session._lock_fd)
            session._lock_fd = None

    def _private_session_files(self, session):
        path = self._path(session)
        for suffix in ('', '-journal', '-wal', '-shm'):
            candidate = Path(str(path) + suffix)
            if candidate.exists():
                candidate.chmod(0o600)

    def _erase_session_files(self, session):
        path = self._path(session)
        for suffix in ('', '-journal', '-wal', '-shm'):
            Path(str(path) + suffix).unlink(missing_ok=True)

    def _credentials(self):
        credentials = self.store.setting('telegram_api', {})
        try:
            api_id = int(credentials.get('api_id', 0))
            api_hash = credentials.get('api_hash', '')
        except (TypeError, ValueError, AttributeError):
            raise _PublicAccountError('请先在设置中填写有效的 Telegram API 凭据') from None
        if api_id <= 0 or not isinstance(api_hash, str) or not api_hash.strip():
            raise _PublicAccountError('请先在设置中填写有效的 Telegram API 凭据')
        return api_id, api_hash.strip()

    async def _connected(self, session):
        if session.connected:
            return
        api_id, api_hash = self._credentials()
        self._acquire(session)
        try:
            if session.client is None:
                session.client = self.client_factory(self._path(session), api_id, api_hash)
            session.status = 'connecting'
            self._persist(session)
            await session.client.connect()
            session.epoch = datetime.now(timezone.utc)
            self._private_session_files(session)
        except BaseException:
            await self._disconnect(session)
            raise

    async def _disconnect(self, session):
        watcher = session._watch_task
        if watcher is not None and watcher is not asyncio.current_task():
            watcher.cancel()
            session._watch_task = None
        try:
            if session.client is not None:
                try:
                    await session.client.disconnect()
                finally:
                    client_session = getattr(session.client, 'session', None)
                    close = getattr(client_session, 'close', None)
                    if close:
                        close()
        finally:
            session.client = None
            session.logged_in = False
            session.own_id = None
            session._phone = None
            session._phone_hash = None
            session.login_channel = ''
            self._release(session)

    async def _identity(self, session):
        me = await session.client.get_me()
        telegram_id = getattr(me, 'id', None)
        if isinstance(telegram_id, bool) or not isinstance(telegram_id, int) or telegram_id <= 0:
            await self._reject_identity(session, '无法核实登录账号的真实身份，请重新登录')
        profile = self._profile(session)
        expected = profile.get('telegram_id')
        if expected is not None and expected != telegram_id:
            await self._reject_identity(session, '登录身份与此账号原身份不符，请为该身份新增账号')
        for existing in self.store.list('accounts', account_id=''):
            if (not existing.get('deleted') and existing.get('id') != session.id
                    and existing.get('telegram_id') == telegram_id):
                await self._reject_identity(session, '该 Telegram 身份已绑定到另一个工作台账号')
        # No network await between duplicate validation and saving the binding.
        profile = dict(profile)
        profile.update(telegram_id=telegram_id, username=getattr(me, 'username', '') or '',
                       phone_masked=_phone_mask(getattr(me, 'phone', None)), last_sync=now())
        session.profile = profile
        self.store.put('accounts', session.id, profile)
        session.own_id = telegram_id
        session.logged_in = True
        session.stage = 'ready'
        session.status = 'online'
        session.error = ''
        session._phone = session._phone_hash = None
        session.login_channel = ''
        self._persist(session)
        await self.app.on_account_ready(session.id)
        self._event(session, 'account_ready', '账号已连接，监听需手动启动')

    async def _reject_identity(self, session, message):
        try:
            await self.app.on_account_offline(session.id)
        except Exception:
            # A service hook cannot keep an untrusted identity connected.
            pass
        try:
            await self._disconnect(session)
        finally:
            self._erase_session_files(session)
        session.status = 'needs_login'
        session.stage = 'phone'
        session.error = message
        self._persist(session)
        self._event(session, 'account_identity_rejected', message)
        raise _PublicAccountError(message)

    def clear_wait(self, account_id, *, confirmed=False):
        """Clear an explicitly checked restriction, never an unelapsed wait."""
        if confirmed is not True:
            raise _PublicAccountError('请确认已人工检查此账号的平台限制')
        session = self.get(account_id)
        profile = dict(self._profile(session))
        wait_until = session.wait_until or profile.get('wait_until')
        if wait_until:
            try:
                still_waiting = datetime.fromisoformat(wait_until) > datetime.now(timezone.utc)
            except (TypeError, ValueError):
                still_waiting = True
            if still_waiting:
                raise _PublicAccountError('平台等待时间尚未结束，不能提前解除')
        profile['wait_restricted'] = False
        profile['wait_until'] = None
        session.wait_until = None
        session.error = ''
        session.status = 'online' if session.logged_in and session.connected else 'needs_login'
        session.profile = profile
        self.store.put('accounts', session.id, profile)
        self._persist(session)
        self._event(session, 'account_wait_acknowledged', '已确认人工检查此账号的平台限制')

    def _check_wait(self, session):
        if self._profile(session).get('wait_restricted'):
            raise _PublicAccountError('平台限制此账号，请人工检查并明确解除后重试')
        wait_until = session.wait_until or self._profile(session).get('wait_until')
        if wait_until:
            try:
                waiting = datetime.fromisoformat(wait_until) > datetime.now(timezone.utc)
            except (TypeError, ValueError):
                waiting = True
            if waiting:
                raise _PublicAccountError('平台要求等待，请在显示的恢复时间后重试')
            session.wait_until = None
            self._persist(session)

    async def _connect_account(self, session, data):
        self._profile(session)
        await self._connected(session)
        if await session.client.is_user_authorized():
            await self._identity(session)
        else:
            session.logged_in = False
            session.stage = 'phone'
            session.status = 'needs_login'
            session.error = ''
            self._persist(session)

    async def _login_rpc(self, operation):
        # Telegram has explicitly rejected these authentication requests with
        # a new DC. Telethon switches the DC, but request_retries=0 then raises
        # that rejection. Retry this known-safe case only; ambiguous connection
        # outcomes and all normal Telegram writes are never replayed here.
        for attempt in range(3):
            try:
                return await operation()
            except Exception as error:
                if (type(error).__name__ not in ('PhoneMigrateError', 'NetworkMigrateError', 'UserMigrateError')
                        or attempt == 2):
                    raise

    async def _send_code(self, session, data):
        self._check_wait(session)
        phone = re.sub(r'[\s()\-]', '', str(data.get('phone', '')))
        if not re.fullmatch(r'\+[1-9]\d{5,14}', phone):
            raise _PublicAccountError('请输入带国家区号的有效手机号，例如 +8613800000000')
        await self._connected(session)
        if session.logged_in:
            raise _PublicAccountError('该账号已登录，无需再次发送验证码')
        result = await self._login_rpc(lambda: session.client.send_code_request(phone))
        phone_hash = getattr(result, 'phone_code_hash', None)
        if not isinstance(phone_hash, str) or not phone_hash:
            raise _PublicAccountError('平台未返回有效验证信息，请重新请求验证码')
        session._phone, session._phone_hash = phone, phone_hash
        channel = type(getattr(result, 'type', None)).__name__
        session.login_channel = {
            'SentCodeTypeApp': 'Telegram 应用内消息', 'SentCodeTypeSms': '短信',
            'SentCodeTypeCall': '语音电话', 'SentCodeTypeFlashCall': '电话验证',
            'SentCodeTypeMissedCall': '未接来电验证',
            'SentCodeTypeFragmentSms': 'Fragment 短信',
            'SentCodeTypeEmailCode': '邮箱验证码',
        }.get(channel, 'Telegram 返回的验证渠道')
        session.stage, session.status, session.error = 'code', 'authorizing', ''
        self._persist(session)
        self._event(session, 'account_code_requested', '已向平台请求登录验证码')

    async def _sign_in(self, session, data):
        self._check_wait(session)
        if session.stage != 'code' or not session._phone or not session._phone_hash or not session.connected:
            raise _PublicAccountError('请先重新请求验证码')
        code = str(data.get('code', '')).strip()
        if not re.fullmatch(r'\d{3,10}', code):
            raise _PublicAccountError('请输入有效的数字验证码')
        try:
            await self._login_rpc(lambda: session.client.sign_in(
                phone=session._phone, code=code, phone_code_hash=session._phone_hash))
        except Exception as error:
            if type(error).__name__ == 'SessionPasswordNeededError':
                session.stage, session.status, session.error = 'password', 'authorizing', ''
                self._persist(session)
                return
            raise
        await self._identity(session)

    async def _password(self, session, data):
        self._check_wait(session)
        if session.stage != 'password' or not session.connected:
            raise _PublicAccountError('请先完成手机号和验证码验证')
        password = data.get('password', '')
        if not isinstance(password, str) or not password or len(password) > 1024:
            raise _PublicAccountError('请输入有效的两步验证密码')
        await self._login_rpc(lambda: session.client.sign_in(password=password))
        await self._identity(session)

    def _failure(self, session, error):
        name = type(error).__name__
        if isinstance(error, _PublicAccountError):
            message = str(error)
        elif name in ('FloodWaitError', 'FloodPremiumWaitError', 'PhoneNumberFloodError'):
            seconds = getattr(error, 'seconds', None)
            known_duration = isinstance(seconds, (int, float)) and seconds >= 0
            profile = dict(self._profile(session))
            profile['wait_restricted'] = not known_duration
            if known_duration:
                session.wait_until = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
            else:
                session.wait_until = None
            profile['wait_until'] = session.wait_until
            self.store.put('accounts', session.id, profile)
            session.profile = profile
            session.status = 'waiting'
            message = '平台请求过频，请等待恢复时间后重试' if seconds is not None else '平台限制此账号，请人工检查后重试'
        elif name == 'PhoneCodeExpiredError':
            session.stage = 'phone'
            session._phone = session._phone_hash = None
            message = '验证码已过期，请重新获取验证码'
        elif name in ('PhoneCodeInvalidError', 'PhoneCodeEmptyError'):
            message = '验证码不正确，请核对后重试'
        elif name in ('PasswordHashInvalidError', 'PasswordEmptyError'):
            message = '两步验证密码不正确，请核对后重试'
        elif name in ('PhoneNumberInvalidError', 'PhoneNumberBannedError'):
            message = '手机号无效或受到平台限制，请核对账号状态'
        elif name in _AUTH_ERRORS or getattr(error, 'code', None) == 401:
            session.status, session.stage, session.logged_in = 'needs_login', 'phone', False
            message = '会话已失效，请重新登录此账号'
        elif isinstance(error, (OSError, ConnectionError, asyncio.TimeoutError)):
            session.status = 'offline'
            message = '连接失败，请检查网络后重试'
        else:
            message = '账号操作未完成，请检查连接与账号状态后重试'
        session.error = message
        self._persist(session)
        self._event(session, 'account_error', message)
        return ValueError(message)

    async def _authorized_action(self, name, session, data):
        generation = session._generation
        async with session._lock:
            if generation != session._generation:
                raise _PublicAccountError('登录操作已取消')
            task = asyncio.current_task()
            session._auth_task = task
            try:
                await getattr(self, '_' + name)(session, data)
            except asyncio.CancelledError:
                if generation != session._generation:
                    raise _PublicAccountError('登录操作已取消') from None
                raise
            except Exception as error:
                raise self._failure(session, error) from None
            finally:
                if session._auth_task is task:
                    session._auth_task = None
                self._private_session_files(session)
                if generation == session._generation:
                    self._start_watch(session)
        return {'account_id': session.id}

    def _start_watch(self, session):
        if not session.logged_in or not session.connected:
            return
        if session._watch_task is not None and not session._watch_task.done():
            return
        if getattr(session.client, 'disconnected', None) is None:
            return
        session._watch_task = asyncio.create_task(
            self._watch_connection(session, session.client, session._generation))

    async def _watch_connection(self, session, client, generation):
        try:
            while generation == session._generation and session.client is client:
                dropped_error = None
                try:
                    # Shield Telethon's own lifecycle future from cancellation
                    # of the workbench watcher during an explicit disconnect.
                    await asyncio.shield(client.disconnected)
                except asyncio.CancelledError:
                    return
                except Exception as error:
                    dropped_error = error
                async with session._lock:
                    if generation != session._generation or session.client is not client:
                        return
                    session.epoch = datetime.now(timezone.utc)
                    session.logged_in = False
                    session.status = 'reconnecting'
                    self._persist(session)
                    await self.app.on_account_offline(session.id)
                    if dropped_error is not None and (
                            type(dropped_error).__name__ in _AUTH_ERRORS
                            or getattr(dropped_error, 'code', None) == 401):
                        await self._disconnect(session)
                        self._failure(session, dropped_error)
                        return
                    for delay in _RECONNECT_DELAYS:
                        await asyncio.sleep(delay)
                        if generation != session._generation:
                            return
                        try:
                            await client.connect()
                            session.epoch = datetime.now(timezone.utc)
                            self._private_session_files(session)
                            if not await client.is_user_authorized():
                                await self._disconnect(session)
                                session.status, session.stage = 'needs_login', 'phone'
                                session.error = '会话已失效，请重新登录此账号'
                                self._persist(session)
                                return
                            await self._identity(session)
                            break
                        except asyncio.CancelledError:
                            raise
                        except Exception as error:
                            self._failure(session, error)
                            if (type(error).__name__ in _AUTH_ERRORS
                                    or getattr(error, 'code', None) == 401
                                    or session.client is None):
                                await self._disconnect(session)
                                return
                    else:
                        await self._disconnect(session)
                        session.status = 'offline'
                        session.error = '连接重试未成功，请检查网络后手动重新连接'
                        self._persist(session)
                        return
        except asyncio.CancelledError:
            return
        finally:
            if session._watch_task is asyncio.current_task():
                session._watch_task = None

    def _cancel_pending(self, session):
        session._generation += 1
        watcher = session._watch_task
        if watcher is not None and watcher is not asyncio.current_task():
            watcher.cancel()
            session._watch_task = None
        task = session._auth_task
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()

    async def action(self, name, data):
        if name == 'add_account':
            if len(self.sessions) >= 8:
                raise _PublicAccountError('最多支持八个账号，请先移除不再使用的账号')
            display_name = str(data.get('name') or f'账号 {len(self.sessions) + 1}').strip()
            if not display_name or len(display_name) > 80:
                raise _PublicAccountError('账号名称应为一至八十个字符')
            account_id = new_id()
            profile = {'id': account_id, 'account_id': account_id, 'name': display_name,
                       'telegram_id': None, 'username': '', 'phone_masked': '',
                       'created_at': now(), 'last_sync': None, 'wait_until': None,
                       'status': 'offline', 'stage': 'phone', 'deleted': False,
                       'wait_restricted': False}
            session = AccountSession(account_id, profile)
            self._directory(session)
            self.sessions[account_id] = session
            try:
                self._persist(session)
                self._event(session, name, '已新增本地账号，尚未登录')
            except Exception:
                self.sessions.pop(account_id, None)
                try:
                    self.store.delete('accounts', account_id)
                    self._directory(session).rmdir()
                except Exception:
                    pass
                raise _PublicAccountError('本地账号保存失败，请检查存储空间后重试') from None
            return {'account_id': account_id}
        session = self.get(data.get('account_id'))
        if name in ('connect_account', 'send_code', 'sign_in', 'password'):
            return await self._authorized_action(name, session, data)
        if name == 'rename_account':
            display_name = str(data.get('name') or '').strip()
            if not display_name or len(display_name) > 80:
                raise _PublicAccountError('账号名称应为一至八十个字符')
            profile = dict(self._profile(session))
            profile['name'] = display_name
            self.store.put('accounts', session.id, profile)
            session.profile = profile
            self.app.bump()
            return {'account_id': session.id}
        if name not in ('cancel_login', 'disconnect_account', 'logout_account', 'remove_account'):
            raise _PublicAccountError('不支持的账号操作')
        if name in ('logout_account', 'remove_account') and data.get('confirmed') is not True:
            raise _PublicAccountError('请明确确认此账号操作')
        if name == 'cancel_login' and (session.stage == 'ready' or session.logged_in):
            # Dismissing a completed login flow must preserve its connection,
            # session and automation. Disconnect/logout are explicit actions.
            return {'account_id': session.id}
        self._cancel_pending(session)
        async with session._lock:
            if name in ('remove_account', 'cancel_login'):
                self._acquire(session)
            pause_account = getattr(self.app, 'pause_account', None)
            if pause_account is not None:
                await pause_account(session.id)
            await self.app.on_account_offline(session.id)
            if name == 'logout_account':
                try:
                    await self._connected(session)
                    if not await session.client.log_out():
                        raise _PublicAccountError('平台尚未确认退出登录，请检查后重试')
                except Exception as error:
                    raise self._failure(session, error) from None
            await self._disconnect(session)
            session.stage = 'phone'
            session.status = 'offline' if name == 'disconnect_account' else 'needs_login'
            session.error = ''
            if name in ('logout_account', 'remove_account', 'cancel_login'):
                self._erase_session_files(session)
            if name == 'remove_account':
                profile = dict(self._profile(session))
                profile.update(deleted=True, deleted_at=now(), status='removed', stage='phone')
                self.store.put('accounts', session.id, profile)
                self.sessions.pop(session.id)
                self.app.bump()
            else:
                self._persist(session)
            self._event(session, name, {
                'cancel_login': '已取消登录', 'disconnect_account': '已断开连接，会话保留',
                'logout_account': '已退出并撤销此会话',
                'remove_account': '已移除本地账号，任务记录保留',
            }[name])
        return {'account_id': session.id}

    async def restore(self):
        async def restore_one(account_id):
            try:
                await self.action('connect_account', {'account_id': account_id})
            except ValueError:
                # Each account exposes its own safe error. One failure cannot
                # block restoration of the other independent sessions.
                pass
        await asyncio.gather(*(restore_one(account_id) for account_id in tuple(self.sessions)))

    async def close(self):
        async def close_one(session):
            self._cancel_pending(session)
            async with session._lock:
                await self.app.on_account_offline(session.id)
                await self._disconnect(session)
                session.status = 'offline'
                session.stage = 'phone'
                self._persist(session)
        await asyncio.gather(*(close_one(session) for session in tuple(self.sessions.values())))

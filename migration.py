"""Explicit, copied-session migration from the old single-account application.

Importing this module never discovers legacy data or contacts Telegram. A user-selected
source is inspected only by check_legacy; only its private candidate is connected.
"""

import asyncio
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import time
import uuid


_FILES = ("config.json", "account.session", "account.lock")
_NON_MIGRATABLE = ["旧版运行统计（未持久保存）", "旧版处理去重记录（未持久保存）",
                   "旧版运行状态、任务和历史日志"]


def _private_dir(path):
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)


def _write_private(path, content):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def _read_regular(path):
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "rb") as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ValueError("旧版来源必须包含普通文件，不能使用符号链接。")
            content = handle.read()
            after = os.fstat(handle.fileno())
            current = path.stat(follow_symlinks=False)
            if (before.st_ino, before.st_dev, before.st_size, before.st_mtime_ns) != (
                    after.st_ino, after.st_dev, after.st_size, after.st_mtime_ns):
                raise ValueError("旧版来源在读取时发生变化，请停止旧程序后重新检查。")
            if (after.st_ino, after.st_dev) != (current.st_ino, current.st_dev):
                raise ValueError("旧版来源在读取时发生变化，请重新检查。")
            return content
    except (OSError, RuntimeError) as error:
        raise ValueError("旧版来源缺少普通文件，或包含不可读取的符号链接。") from None


def _digest(files):
    hashes = {name: hashlib.sha256(content).hexdigest() for name, content in files.items()}
    return hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()


def _config(raw):
    try:
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeError):
        raise ValueError("旧版 config.json 格式错误。") from None
    if not isinstance(data, dict):
        raise ValueError("旧版 config.json 必须是配置对象。")
    api_id, api_hash, groups = data.get("api_id"), data.get("api_hash"), data.get("group_ids", [])
    if type(api_id) is not int or api_id <= 0:
        raise ValueError("旧版 API ID 必须是正整数。")
    if not isinstance(api_hash, str) or not re.fullmatch(r"[0-9a-fA-F]{32}", api_hash):
        raise ValueError("旧版 API Hash 必须为 32 位十六进制字符串。")
    if not isinstance(groups, list) or any(type(group) is not int or group >= 0
                                           or abs(group) > 9007199254740991 for group in groups):
        raise ValueError("旧版群 ID 必须是负整数列表。")
    return {"api_id": api_id, "api_hash": api_hash, "group_ids": list(dict.fromkeys(groups))}


@contextmanager
def _readonly_db(path):
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
    try:
        db.execute("PRAGMA trusted_schema = OFF")
        db.execute("PRAGMA query_only = ON")
        yield db
    finally:
        db.close()


def _validate_session(path):
    try:
        with _readonly_db(path) as db:
            if db.execute("PRAGMA quick_check").fetchone() != ("ok",):
                raise ValueError("旧版 SQLite 会话完整性校验失败。")
            required = {"version", "sessions", "entities", "sent_files", "update_state"}
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not required.issubset(tables):
                raise ValueError("旧版文件不是受支持的 Telethon SQLite 会话。")
            versions = db.execute("SELECT version FROM version").fetchall()
            if len(versions) != 1 or versions[0][0] not in (7, 8):
                raise ValueError("旧版 SQLite 会话版本不受支持，请使用旧程序重新保存会话。")
            columns = {row[1] for row in db.execute("PRAGMA table_info(sessions)")}
            if not {"dc_id", "server_address", "port", "auth_key", "takeout_id"}.issubset(columns):
                raise ValueError("旧版 SQLite 会话字段不完整。")
            rows = db.execute("SELECT dc_id, server_address, port, auth_key FROM sessions").fetchall()
            if len(rows) != 1:
                raise ValueError("旧版 SQLite 会话缺少唯一连接信息。")
            dc_id, server, port, key = rows[0]
            if (type(dc_id) is not int or dc_id <= 0 or not isinstance(server, str) or not server
                    or type(port) is not int or not 1 <= port <= 65535
                    or (key is not None and (not isinstance(key, bytes) or len(key) not in (0, 256)))):
                raise ValueError("旧版 SQLite 会话连接信息格式错误。")
    except sqlite3.Error:
        raise ValueError("旧版 account.session 不是有效的 SQLite 会话。") from None


def _require_closed_journal(path, *, label="候选"):
    for suffix in ("-wal", "-journal"):
        sidecar = path.with_name(path.name + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise ValueError(f"{label}会话仍有未合并的 SQLite 日志，请正常退出旧程序后重新检查。")


def _snapshot_session(original, candidate):
    _validate_session(original)
    _write_private(candidate, b"")
    try:
        with _readonly_db(original) as source, closing(sqlite3.connect(candidate)) as destination, destination:
            source.backup(destination)
    except sqlite3.Error:
        raise ValueError("无法建立旧版会话的 SQLite 迁移快照。") from None
    candidate.chmod(0o600)
    _validate_session(candidate)


def _mask(value, *, tail=4):
    text = str(value or "")
    if not text:
        return ""
    return "***" if len(text) <= tail else "***" + text[-tail:]


def _identity(me):
    user_id = getattr(me, "id", None)
    if type(user_id) is not int or user_id <= 0 or getattr(me, "bot", False):
        raise ValueError("旧版会话未能核实真实用户身份，不能导入机器人或未知身份。")
    first_name = str(getattr(me, "first_name", "") or "")
    last_name = str(getattr(me, "last_name", "") or "")
    return {"telegram_id": user_id, "username": str(getattr(me, "username", "") or ""),
            "phone_masked": _mask(getattr(me, "phone", "")),
            "display_name_masked": (first_name[:1] + "***") if first_name else
                                   ((last_name[:1] + "***") if last_name else "用户 " + _mask(user_id))}


class MigrationService:
    def __init__(self, app):
        self.app = app
        self._previews = {}
        self._import_lock = asyncio.Lock()

    async def action(self, name, data):
        if name == "check_legacy":
            return await self._check(data)
        if name == "import_legacy":
            async with self._import_lock:
                return await self._import(data)
        raise ValueError("未知的旧版迁移操作。")

    @contextmanager
    def _source(self, selected):
        if not isinstance(selected, str) or not selected.strip():
            raise ValueError("请主动选择旧版数据目录后再检查。")
        try:
            source = Path(selected).expanduser().resolve(strict=True)
        except (OSError, RuntimeError):
            raise ValueError("选择的旧版来源目录不存在或不可读取。") from None
        if not source.is_dir():
            raise ValueError("请选择包含旧版配置和会话的目录。")
        lock_path = source / "account.lock"
        try:
            descriptor = os.open(lock_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except OSError:
            raise ValueError("旧版来源缺少普通 account.lock 文件，或使用了符号链接。") from None
        with os.fdopen(descriptor, "rb") as lock:
            if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
                raise ValueError("旧版 account.lock 必须是普通文件。")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (BlockingIOError, OSError):
                raise ValueError("旧版账号仍被运行中的程序占用，请先停止旧程序。") from None
            try:
                locked = os.fstat(lock.fileno())
                current = lock_path.stat(follow_symlinks=False)
                if (locked.st_ino, locked.st_dev) != (current.st_ino, current.st_dev):
                    raise ValueError("旧版来源锁文件发生变化，请重新检查。")
                # A raw rollback backup cannot safely represent an active journal.
                _require_closed_journal(source / "account.session", label="旧版来源")
                files = {name: _read_regular(source / name) for name in _FILES}
                yield source, files
                _require_closed_journal(source / "account.session", label="旧版来源")
                current = lock_path.stat(follow_symlinks=False)
                if (locked.st_ino, locked.st_dev) != (current.st_ino, current.st_dev):
                    raise ValueError("旧版来源锁文件发生变化，请重新检查。")
                if _digest({name: _read_regular(source / name) for name in _FILES}) != _digest(files):
                    raise ValueError("旧版来源发生变化，本次确认已失效，请重新检查。")
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _eligibility(self, identity, config, *, ignore_id=None):
        profiles = [profile for profile in self.app.store.list("accounts")
                    if not profile.get("deleted") and profile.get("id") != ignore_id]
        if any(profile.get("telegram_id") == identity["telegram_id"] for profile in profiles):
            return "此真实 Telegram 账号已存在，不能重复导入。"
        if len(profiles) >= 8:
            return "已达到 8 个账号上限，请先移除不需要的本地账号。"
        current = self.app.store.setting("telegram_api")
        if current and (current.get("api_id") != config["api_id"] or
                        not hmac.compare_digest(str(current.get("api_hash", "")), config["api_hash"])):
            return "旧版 API 凭据与本工作台的共享 API 不同；请先核对设置，不会覆盖现有凭据。"
        return ""

    async def _check(self, data):
        with self._source(data.get("source_path")) as (source, files):
            config = _config(files["config.json"])
            directory = self.app.root / "migrations" / uuid.uuid4().hex
            backup, candidate_dir = directory / "original", directory / "candidate"
            _private_dir(self.app.root / "migrations")
            _private_dir(directory)
            _private_dir(backup)
            _private_dir(candidate_dir)
            for name, content in files.items():
                _write_private(backup / name, content)
            candidate = candidate_dir / "account.session"
            _snapshot_session(backup / "account.session", candidate)
            identity, reason, client = None, "", None
            try:
                client = self.app.accounts.client_factory(candidate, config["api_id"], config["api_hash"])
                await client.connect()
                if not await client.is_user_authorized():
                    reason = "旧版会话未授权或已失效，不能恢复；请使用新增账号登录。"
                else:
                    identity = _identity(await client.get_me())
            except asyncio.CancelledError:
                raise
            except ValueError:
                reason = "旧版会话未能核实真实用户身份，不能导入机器人或未知身份。"
            except Exception:
                reason = "无法连接并核实旧版会话，尚不能确认恢复；请检查网络后重新检查。"
            finally:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        identity, reason = None, "旧版候选会话未能安全断开，请重新检查后导入。"
                for path in candidate_dir.iterdir():
                    if path.is_file():
                        path.chmod(0o600)
            can_restore = identity is not None
            if can_restore:
                try:
                    _require_closed_journal(candidate)
                    _validate_session(candidate)
                except ValueError:
                    can_restore = False
                    reason = "候选会话未通过 SQLite 会话或日志校验，请重新检查后导入。"
                else:
                    reason = self._eligibility(identity, config)
            token = secrets.token_urlsafe(32) if can_restore and not reason else None
            expires = time.time() + 300
            if token:
                self._previews[token] = {"source_path": str(source), "source_digest": _digest(files),
                    "candidate": candidate, "candidate_digest": _digest({"session": _read_regular(candidate)}),
                    "config": config, "identity": identity, "expires": expires, "backup_path": str(backup)}
            public_identity = None if not identity else {
                "telegram_id_masked": _mask(identity["telegram_id"]),
                "name": identity["display_name_masked"],
                "username_masked": _mask(identity["username"]),
                "phone_masked": identity["phone_masked"]}
            return {"source_path": str(source), "backup_path": str(backup),
                    "identity": public_identity, "group_ids": config["group_ids"],
                    "migratable_fields": ["已验证的本机会话", "群 ID 配置", "兼容的应用 API 凭据"],
                    "non_migratable_fields": list(_NON_MIGRATABLE),
                    "risks": ["迁移备份包含会话密钥，仅保存在本机私有目录，请勿分享。",
                              "旧去重无法迁移，重新启动监听前请核对群配置；现有联系人仍由当前账号重新核实。",
                              "确认后会创建独立账号，旧版文件保留；本预览不代表会话永远有效。"],
                    "listener_after_import": "停止，保存为预览模式；核对配置后需手动启动。",
                    "can_restore": can_restore, "can_import": bool(token), "reason": reason,
                    "confirmation_token": token,
                    "expires_at": datetime.fromtimestamp(expires, timezone.utc).isoformat() if token else None}

    async def _import(self, data):
        token = data.get("confirmation_token")
        preview = self._previews.pop(token, None) if isinstance(token, str) else None
        if not preview or time.time() >= preview["expires"]:
            raise ValueError("迁移确认已过期或失效，请重新检查旧版来源。")
        name = data.get("name", "")
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 80:
            raise ValueError("请输入 1 至 80 字的账号名称，再重新检查迁移。")
        account_id = None
        previous_api = self.app.store.setting("telegram_api")
        applied_api = None
        try:
            with self._source(preview["source_path"]) as (_, files):
                if not hmac.compare_digest(_digest(files), preview["source_digest"]):
                    raise ValueError("旧版来源发生变化，本次确认已失效，请重新检查。")
                backup_files = {name: _read_regular(Path(preview["backup_path"]) / name) for name in _FILES}
                if not hmac.compare_digest(_digest(backup_files), preview["source_digest"]):
                    raise ValueError("旧版原始迁移备份发生变化，校验失败，请重新检查。")
                candidate = preview["candidate"]
                _require_closed_journal(candidate)
                candidate_bytes = _read_regular(candidate)
                if not hmac.compare_digest(_digest({"session": candidate_bytes}), preview["candidate_digest"]):
                    raise ValueError("已核实身份的候选会话发生变化，校验失败，请重新检查。")
                _validate_session(candidate)
                reason = self._eligibility(preview["identity"], preview["config"])
                if reason:
                    raise ValueError(reason)
                # The manager allocates an isolated id and enforces its capacity limit.
                added = await self.app.accounts.action("add_account", {"name": name.strip()})
                account_id = added["account_id"]
                reason = self._eligibility(preview["identity"], preview["config"], ignore_id=account_id)
                if reason:
                    raise ValueError(reason)
                target = self.app.root / "accounts" / account_id / "account.session"
                _write_private(target, candidate_bytes)
                profile = self.app.store.get("accounts", account_id)
                profile.update({"telegram_id": preview["identity"]["telegram_id"],
                    "username": preview["identity"]["username"],
                    "phone_masked": preview["identity"]["phone_masked"],
                    "status": "offline", "stage": "phone", "migration_backup": preview["backup_path"]})
                self.app.store.put("accounts", account_id, profile)
                self.app.store.put("listener_config", "current", {"account_id": account_id,
                    "group_ids": preview["config"]["group_ids"], "mode": "preview", "version": 1},
                    account_id=account_id)
                if not self.app.store.setting("telegram_api"):
                    applied_api = {key: preview["config"][key] for key in ("api_id", "api_hash")}
                    self.app.store.set_setting("telegram_api", applied_api)
            self.app.log(account_id, "import_legacy", "旧版账号已复制导入；监听停止，统计和去重未迁移。")
        except BaseException:
            if account_id is not None:
                try:
                    await self.app.accounts.action("remove_account", {"account_id": account_id, "confirmed": True})
                finally:
                    self.app.store.delete("listener_config", "current", account_id=account_id)
                    if applied_api is not None and self.app.store.setting("telegram_api") == applied_api:
                        if previous_api is None:
                            self.app.store.delete("settings", "telegram_api")
                        else:
                            self.app.store.set_setting("telegram_api", previous_api)
            raise
        return {"account_id": account_id, "listener_state": "stopped", "mode": "preview",
                "non_migratable_fields": list(_NON_MIGRATABLE), "backup_path": preview["backup_path"]}

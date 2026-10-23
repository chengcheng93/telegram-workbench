"""Read-only cleanup scope evidence and private registered-contact backups.

Importing this module never constructs a client or accesses account sessions.
SavedPhoneContact has no registration flag: unmatched phones remain unknown.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import stat
from uuid import uuid4

from telethon.tl import functions, types


@dataclass(frozen=True, slots=True)
class FullSnapshot:
    account_id: str
    own_id: int
    users: tuple
    saved: tuple
    scope_info: dict
    fingerprint: str


def _valid_id(value):
    return type(value) is int and 0 < value <= 2 ** 53 - 1


def _identity(account_id, own_id):
    if not isinstance(account_id, str) or not account_id or not _valid_id(own_id):
        raise ValueError("账号身份无效，无法核实联系人范围")


def input_user(user):
    """Reject partial references before creating a real TL deletion input."""
    if (type(user) is not types.User or not _valid_id(user.id)
            or user.deleted or user.min or user.is_self
            or type(user.access_hash) is not int
            or not -(2 ** 63) <= user.access_hash < 2 ** 63
            or user.access_hash == 0):
        raise ValueError("联系人身份不可解析，不能执行删除")
    return types.InputUser(user_id=user.id, access_hash=user.access_hash)


def _phone(value):
    if not isinstance(value, str) or not value:
        return None
    compact = re.sub(r"[\s().\-]", "", value)
    if compact.startswith("+"):
        compact = compact[1:]
    return compact if compact and compact.isascii() and compact.isdecimal() else None


def _export_user(user):
    if type(user) not in (types.User, types.UserEmpty) or not _valid_id(user.id):
        raise ValueError("完整联系人数据包含无效身份")
    first = getattr(user, "first_name", None) or ""
    last = getattr(user, "last_name", None) or ""
    return {
        "user_id": user.id,
        "first_name": first,
        "last_name": last,
        "name": " ".join(part for part in (first, last) if part) or str(user.id),
        "username": getattr(user, "username", None) or "",
        "phone": getattr(user, "phone", None) or "",
    }


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


async def read_full_snapshot(client, account_id, own_id):
    """Read registered contacts and takeout saved phones without any writes.

    Registered reads fail closed. A saved-phone capability failure preserves
    registered references, but cannot authorize a full-clear confirmation.
    """
    _identity(account_id, own_id)
    response = await client(functions.contacts.GetContactsRequest(hash=0))
    if type(response) is not types.contacts.Contacts:
        raise ValueError("未获得完整联系人快照，不能准备清空")
    if not isinstance(response.contacts, (list, tuple)) or not isinstance(response.users, (list, tuple)):
        raise ValueError("完整联系人快照格式无效")
    by_id = {}
    for user in response.users:
        if type(user) not in (types.User, types.UserEmpty) or not _valid_id(user.id):
            raise ValueError("完整联系人快照包含无效身份")
        if user.id in by_id:
            raise ValueError("完整联系人快照包含重复身份")
        by_id[user.id] = user
    contact_ids = []
    for contact in response.contacts:
        if type(contact) is not types.Contact or not _valid_id(contact.user_id):
            raise ValueError("完整联系人快照包含无效联系人")
        if contact.user_id in contact_ids:
            raise ValueError("完整联系人快照包含重复联系人")
        contact_ids.append(contact.user_id)
    users = tuple(by_id.get(user_id, types.UserEmpty(user_id)) for user_id in contact_ids)
    saved = ()
    saved_available = False
    saved_error = ""
    try:
        async with client.takeout(contacts=True) as takeout:
            result = await takeout(functions.contacts.GetSavedRequest())
        if (not isinstance(result, (list, tuple))
                or any(type(entry) is not types.SavedPhoneContact for entry in result)):
            raise ValueError("保存号码响应格式无效")
        saved = tuple(result)
        saved_available = True
    except Exception as error:
        # Library exception text can contain private account/phone data.
        saved_error = "无法读取已保存号码（" + type(error).__name__ + "）"

    verified_phones = {
        phone for user in users
        if type(user) is types.User and not user.deleted and not user.min
        and user.id != own_id and (phone := _phone(user.phone)) is not None
    }
    matched = saved_available and all(
        _phone(entry.phone) is not None and _phone(entry.phone) in verified_phones
        for entry in saved
    )
    complete = bool(matched)
    reason = ""
    if not complete:
        reason = "不支持完整清空：" + (
            saved_error or "保存号码的注册状态无法完整核实"
        )
    scope = {
        "registered_count": len(users),
        "saved_numbers_count": len(saved) if saved_available else None,
        "saved_count": len(saved) if saved_available else None,
        "unregistered_count": 0 if complete else None,
        "complete": complete,
        "error": reason,
        "reason": reason,
        "capabilities": {
            "get_contacts": True,
            "saved_contacts_read": saved_available,
            "saved_registration_verified": complete,
            "full_clear": complete,
        },
    }
    fingerprint_data = {
        "account_id": account_id,
        "own_id": own_id,
        "users": sorted([
            {**_export_user(user), "deleted": bool(getattr(user, "deleted", False)),
             "min": bool(getattr(user, "min", False)),
             "resolvable": type(user) is types.User and type(user.access_hash) is int
             and user.access_hash != 0}
            for user in users
        ], key=lambda row: row["user_id"]),
        "saved": sorted([
            {"phone": entry.phone, "first_name": entry.first_name,
             "last_name": entry.last_name,
             "date": entry.date.isoformat() if entry.date is not None else None}
            for entry in saved
        ], key=lambda row: _canonical(row)),
        "scope": scope,
    }
    fingerprint = hashlib.sha256(_canonical(fingerprint_data)).hexdigest()
    return FullSnapshot(account_id, own_id, users, saved, scope, fingerprint)


def _directory(root, *, create=False):
    root = Path(root).resolve()
    if create:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory = root / "backups"
    if directory.is_symlink():
        raise ValueError("备份目录不能是符号链接")
    if create:
        directory.mkdir(mode=0o700, exist_ok=True)
    if not directory.is_dir():
        raise ValueError("备份目录不存在")
    if create:
        directory.chmod(0o700)
    return directory


def _read_private(directory, name):
    dir_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=dir_fd)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
                raise ValueError("备份文件权限不安全或文件类型无效")
            if info.st_size > 64 * 1024 * 1024:
                raise ValueError("备份文件过大，无法验证")
            return stream.read()
    finally:
        os.close(dir_fd)


def save_backup(root, snapshot):
    """Atomically persist registered contacts only, then verify actual bytes."""
    if type(snapshot) is not FullSnapshot:
        raise ValueError("联系人快照无效")
    _identity(snapshot.account_id, snapshot.own_id)
    payload = {
        "version": 1,
        "account_id": snapshot.account_id,
        "own_id": snapshot.own_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "fingerprint": snapshot.fingerprint,
        "contacts": [_export_user(user) for user in snapshot.users],
    }
    content = _canonical(payload)
    temporary = ".contacts-" + uuid4().hex + ".tmp"
    name = "contacts-" + uuid4().hex + ".json"
    descriptor = None
    dir_fd = None
    try:
        directory = _directory(root, create=True)
        dir_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW,
                             0o600, dir_fd=dir_fd)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            os.fchmod(stream.fileno(), 0o600)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        os.fsync(dir_fd)
        metadata = {
            "path": str(directory / name), "name": name,
            "sha256": hashlib.sha256(content).hexdigest(),
            "count": len(payload["contacts"]),
        }
        validate_backup(root, metadata, snapshot.account_id)
        return metadata
    except (OSError, TypeError, UnicodeError, ValueError) as error:
        raise ValueError("备份保存或校验失败，禁止进入清空确认") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if dir_fd is not None:
            try:
                os.unlink(temporary, dir_fd=dir_fd)
            except FileNotFoundError:
                pass
            os.close(dir_fd)


def validate_backup(root, metadata, account_id):
    """Validate trusted task metadata against a private, account-bound file."""
    try:
        if not isinstance(metadata, dict) or not isinstance(account_id, str) or not account_id:
            raise ValueError("备份元数据无效")
        name = metadata.get("name")
        digest = metadata.get("sha256")
        count = metadata.get("count")
        if (not isinstance(name, str) or re.fullmatch(r"contacts-[a-f0-9]{32}\.json", name) is None
                or not isinstance(digest, str) or re.fullmatch(r"[a-f0-9]{64}", digest) is None
                or type(count) is not int or count < 0):
            raise ValueError("备份元数据无效")
        directory = _directory(root)
        if not isinstance(metadata.get("path"), str) or Path(metadata["path"]) != directory / name:
            raise ValueError("备份路径超出当前备份目录")
        content = _read_private(directory, name)
        if not hmac.compare_digest(hashlib.sha256(content).hexdigest(), digest):
            raise ValueError("备份摘要不一致")
        payload = json.loads(content)
        if (not isinstance(payload, dict)
                or set(payload) != {"version", "account_id", "own_id", "created_at", "fingerprint", "contacts"}
                or payload["version"] != 1 or payload["account_id"] != account_id
                or not _valid_id(payload["own_id"])
                or not isinstance(payload["created_at"], str)
                or not isinstance(payload["fingerprint"], str)
                or re.fullmatch(r"[a-f0-9]{64}", payload["fingerprint"]) is None
                or not isinstance(payload["contacts"], list) or len(payload["contacts"]) != count):
            raise ValueError("备份账号或条数校验失败")
        ids = set()
        for row in payload["contacts"]:
            if (not isinstance(row, dict)
                    or set(row) != {"user_id", "first_name", "last_name", "name", "username", "phone"}
                    or not _valid_id(row["user_id"]) or row["user_id"] in ids
                    or any(not isinstance(row[key], str) for key in row if key != "user_id")):
                raise ValueError("备份联系人内容无效")
            ids.add(row["user_id"])
        return payload
    except (OSError, KeyError, TypeError, UnicodeError, ValueError) as error:
        raise ValueError("备份无法通过读取、账号、条数和摘要校验") from error

"""Private local persistence for TG Workbench."""

import json
from datetime import datetime, timezone
import os
from pathlib import Path
import sqlite3
import tempfile
from uuid import uuid4


def new_id() -> str:
    """Return an opaque identifier suitable for a persisted record key."""
    return uuid4().hex


def now() -> str:
    """Return the current time as an aware UTC ISO timestamp."""
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.root.chmod(0o700)
        database = self.root / "workbench.sqlite3"
        descriptor = os.open(database, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        self._db = sqlite3.connect(database, check_same_thread=False)
        with self._db:
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS records ("
                "kind TEXT NOT NULL, key TEXT NOT NULL, account_id TEXT NOT NULL, "
                "value TEXT NOT NULL, PRIMARY KEY (kind, key, account_id))"
            )

    def get(self, kind: str, key: str, default=None, *, account_id: str = ""):
        row = self._db.execute(
            "SELECT value FROM records WHERE kind = ? AND key = ? AND account_id = ?",
            (kind, key, account_id),
        ).fetchone()
        return default if row is None else json.loads(row[0])

    def put(self, kind: str, key: str, value, *, account_id: str = "") -> None:
        payload = json.dumps(value, ensure_ascii=False, allow_nan=False)
        with self._db:
            self._db.execute(
                "INSERT INTO records(kind, key, account_id, value) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(kind, key, account_id) DO UPDATE SET value = excluded.value",
                (kind, key, account_id, payload),
            )

    def delete(self, kind: str, key: str, *, account_id: str = "") -> None:
        with self._db:
            self._db.execute(
                "DELETE FROM records WHERE kind = ? AND key = ? AND account_id = ?",
                (kind, key, account_id),
            )

    def clear_kind(self, kind: str, account_id: str | None = None) -> int:
        """Delete one category in one transaction, optionally within an account."""
        query = "DELETE FROM records WHERE kind = ?"
        parameters = [kind]
        if account_id is not None:
            query += " AND account_id = ?"
            parameters.append(account_id)
        with self._db:
            deleted = self._db.execute(query, parameters).rowcount
        return deleted

    def backup_to(self, path: Path) -> None:
        """Atomically publish a private, consistent SQLite snapshot."""
        destination = Path(path)
        source = (self.root / "workbench.sqlite3").resolve()
        protected = [source, *(Path(f"{source}{suffix}") for suffix in ("-wal", "-shm", "-journal"))]
        if any(
            destination.resolve() == reserved
            or (destination.exists() and reserved.exists() and destination.samefile(reserved))
            for reserved in protected
        ):
            raise ValueError("备份路径不能覆盖当前数据库或其日志")

        missing = []
        parent = destination.parent
        while not parent.exists():
            missing.append(parent)
            parent = parent.parent
        for directory in reversed(missing):
            directory.mkdir(mode=0o700)
            directory.chmod(0o700)

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
        )
        temporary = Path(temporary_name)
        try:
            try:
                os.fchmod(descriptor, 0o600)
            finally:
                os.close(descriptor)
            connection = sqlite3.connect(temporary)
            try:
                self._db.backup(connection)
            finally:
                connection.close()
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)

    def list(self, kind: str, account_id: str | None = None) -> list:
        query = "SELECT value FROM records WHERE kind = ?"
        parameters = [kind]
        if account_id is not None:
            query += " AND account_id = ?"
            parameters.append(account_id)
        query += " ORDER BY key, account_id"
        return [json.loads(row[0]) for row in self._db.execute(query, parameters)]

    def setting(self, key: str, default=None):
        return self.get("settings", key, default)

    def set_setting(self, key: str, value) -> None:
        self.put("settings", key, value)

    def event(self, account_id: str, action: str, detail: str, **fields) -> dict:
        entry = {
            **fields,
            "id": new_id(),
            "time": now(),
            "account_id": account_id,
            "action": action,
            "detail": detail,
        }
        self.put("events", entry["id"], entry, account_id=account_id)
        return entry

    def events(self, account_id: str | None = None, limit: int = 100) -> list[dict]:
        query = "SELECT value FROM records WHERE kind = 'events'"
        parameters = []
        if account_id is not None:
            query += " AND account_id = ?"
            parameters.append(account_id)
        query += " ORDER BY json_extract(value, '$.time') DESC, rowid DESC LIMIT ?"
        parameters.append(max(0, limit))
        return [json.loads(row[0]) for row in self._db.execute(query, parameters)]

    def close(self) -> None:
        self._db.close()

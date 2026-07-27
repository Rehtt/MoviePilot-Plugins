import threading
from copy import deepcopy
from typing import Any, Callable, Dict, Iterable, List, Optional

from .models import TASK_SCHEMA_VERSION, TaskRecord


LoadCallback = Callable[[], Any]
SaveCallback = Callable[[Dict[str, Any]], None]
MigrationCallback = Callable[[Dict[str, Any], str, str, str], None]


class TaskStore:
    def __init__(
        self,
        load_callback: LoadCallback,
        save_callback: SaveCallback,
        default_downloader: str,
        migration_callback: Optional[MigrationCallback] = None,
    ):
        self._load_callback = load_callback
        self._save_callback = save_callback
        self._default_downloader = default_downloader
        self._migration_callback = migration_callback
        self._lock = threading.RLock()
        self._records: Dict[str, TaskRecord] = {}
        self._load()

    def _load(self) -> None:
        raw = self._load_callback() or {}
        records: Dict[str, TaskRecord] = {}
        if (
            isinstance(raw, dict)
            and raw.get("schema_version") == TASK_SCHEMA_VERSION
            and isinstance(raw.get("records"), dict)
        ):
            for public_id, value in raw["records"].items():
                if not isinstance(value, dict):
                    continue
                value = dict(value)
                value.setdefault("public_id", str(public_id))
                try:
                    record = TaskRecord.from_dict(value)
                except (TypeError, ValueError):
                    continue
                records[record.public_id] = record
        elif isinstance(raw, dict):
            for gid, value in raw.items():
                if not isinstance(value, dict):
                    continue
                record = TaskRecord.from_legacy(
                    str(gid), value, self._default_downloader
                )
                records[record.public_id] = record
        self._records = records

    def _payload(
        self, records: Optional[Dict[str, TaskRecord]] = None
    ) -> Dict[str, Any]:
        source = records if records is not None else self._records
        return {
            "schema_version": TASK_SCHEMA_VERSION,
            "records": {
                public_id: record.to_dict()
                for public_id, record in source.items()
            },
        }

    def _persist(self) -> None:
        self._save_callback(self._payload())

    def records(self, owned_only: bool = False) -> List[TaskRecord]:
        with self._lock:
            return [
                deepcopy(record)
                for record in self._records.values()
                if not owned_only or record.owned
            ]

    def find(self, identity: Any) -> Optional[TaskRecord]:
        text = str(identity or "")
        if not text:
            return None
        with self._lock:
            direct = self._records.get(text)
            if direct:
                return deepcopy(direct)
            for record in self._records.values():
                if record.matches(text):
                    return deepcopy(record)
        return None

    def upsert(self, record: TaskRecord) -> TaskRecord:
        record = deepcopy(record).normalize()
        with self._lock:
            current = self._records.get(record.public_id)
            if current:
                updated = deepcopy(current).merge(record)
            else:
                updated = record
            if current and current.to_dict() == updated.to_dict():
                return deepcopy(current)
            updated.touch()
            self._records[updated.public_id] = updated
            self._persist()
            return deepcopy(updated)

    def update(self, identity: Any, **changes) -> Optional[TaskRecord]:
        with self._lock:
            current = self.find(identity)
            if not current:
                return None
            stored = self._records[current.public_id]
            for key, value in changes.items():
                if hasattr(stored, key):
                    setattr(stored, key, value)
            stored.normalize()
            stored.touch()
            self._persist()
            return deepcopy(stored)

    def remove(self, identity: Any) -> bool:
        with self._lock:
            current = self.find(identity)
            if not current:
                return False
            self._records.pop(current.public_id, None)
            self._persist()
            return True

    def migrate(
        self,
        identity: Any,
        new_public_id: str,
        info_hash: Optional[str],
        related_gids: Iterable[str],
        effective_gid: str,
    ) -> TaskRecord:
        with self._lock:
            current = self.find(identity)
            if not current:
                raise KeyError(str(identity))
            if not current.migration_pending and current.public_id == new_public_id:
                return current
            old_public_id = current.public_id
            migrated = deepcopy(current)
            migrated.public_id = new_public_id
            migrated.info_hash = info_hash
            migrated.effective_gid = effective_gid or migrated.effective_gid
            migrated.related_gids = [
                *migrated.related_gids,
                *list(related_gids or []),
            ]
            migrated.aliases = [*migrated.aliases, old_public_id]
            migrated.migration_pending = False
            migrated.normalize()
            migrated.touch()

            next_records = deepcopy(self._records)
            next_records.pop(old_public_id, None)
            collision = next_records.get(new_public_id)
            if collision:
                migrated = collision.merge(migrated)
                migrated.touch()
            next_records[new_public_id] = migrated
            payload = self._payload(next_records)
            if self._migration_callback:
                self._migration_callback(
                    payload,
                    old_public_id,
                    new_public_id,
                    migrated.downloader,
                )
            else:
                self._save_callback(payload)
            self._records = next_records
            return deepcopy(migrated)


def moviepilot_atomic_migration(
    plugin_id: str,
    data_key: str,
    payload: Dict[str, Any],
    old_hash: str,
    new_hash: str,
    downloader: str,
) -> None:
    """
    Update plugin task data and matching MoviePilot history rows in one DB transaction.
    Imports stay local so pure task-store tests do not require MoviePilot.
    """
    from app.db import SessionFactory
    from app.db.models.downloadhistory import DownloadFiles, DownloadHistory
    from app.db.models.plugindata import PluginData
    from app.db.models.transferhistory import TransferHistory

    with SessionFactory.begin() as database:
        plugin_data = (
            database.query(PluginData)
            .filter(
                PluginData.plugin_id == plugin_id,
                PluginData.key == data_key,
            )
            .first()
        )
        if plugin_data:
            plugin_data.value = payload
        else:
            database.add(
                PluginData(plugin_id=plugin_id, key=data_key, value=payload)
            )

        history_filter = [DownloadHistory.download_hash == old_hash]
        file_filter = [DownloadFiles.download_hash == old_hash]
        transfer_filter = [TransferHistory.download_hash == old_hash]
        if downloader:
            history_filter.append(DownloadHistory.downloader == downloader)
            file_filter.append(DownloadFiles.downloader == downloader)
            transfer_filter.append(TransferHistory.downloader == downloader)
        database.query(DownloadHistory).filter(*history_filter).update(
            {"download_hash": new_hash}, synchronize_session=False
        )
        database.query(DownloadFiles).filter(*file_filter).update(
            {"download_hash": new_hash}, synchronize_session=False
        )
        database.query(TransferHistory).filter(*transfer_filter).update(
            {"download_hash": new_hash}, synchronize_session=False
        )

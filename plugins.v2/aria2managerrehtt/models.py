import base64
import binascii
import hashlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse


TASK_SCHEMA_VERSION = 2


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def unique_text(values: Any) -> List[str]:
    result: List[str] = []
    for value in as_list(values):
        text = str(value or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def normalize_info_hash(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    if len(text) == 40 and all(char in "0123456789abcdef" for char in text):
        return text
    if len(text) == 32:
        try:
            decoded = base64.b32decode(text.upper())
            if len(decoded) == 20:
                return decoded.hex()
        except (ValueError, binascii.Error):
            return None
    return None


def magnet_info_hash(content: Any) -> Optional[str]:
    if isinstance(content, bytes):
        try:
            content = content.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(content, str) or not content.lower().startswith("magnet:"):
        return None
    query = parse_qs(urlparse(content).query)
    for exact_topic in query.get("xt", []):
        prefix = "urn:btih:"
        if exact_topic.lower().startswith(prefix):
            return normalize_info_hash(exact_topic[len(prefix):])
    return None


def proxy_public_id(plugin_id: str, downloader: str, root_gid: str) -> str:
    payload = "\0".join(
        ["aria2managerrehtt", plugin_id or "", downloader or "", root_gid or ""]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


class Aria2File(dict):
    """MoviePilot file-list compatible mapping with attribute access."""

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as err:
            raise AttributeError(item) from err


@dataclass
class TaskRecord:
    public_id: str
    root_gid: str
    effective_gid: str
    related_gids: List[str] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)
    owned: bool = True
    downloader: str = ""
    info_hash: Optional[str] = None
    bt_expected: bool = False
    tags: List[str] = field(default_factory=list)
    category: Optional[str] = None
    episodes: List[int] = field(default_factory=list)
    selected_episodes: List[int] = field(default_factory=list)
    done: bool = False
    file_paths: List[str] = field(default_factory=list)
    migration_pending: bool = False
    remove_pending: bool = False
    created_at: str = field(default_factory=utc_now_text)
    updated_at: str = field(default_factory=utc_now_text)

    def normalize(self) -> "TaskRecord":
        self.public_id = str(self.public_id or "")
        self.root_gid = str(self.root_gid or "")
        self.effective_gid = str(self.effective_gid or self.root_gid)
        self.related_gids = unique_text(
            [
                self.root_gid,
                self.effective_gid,
                *as_list(self.related_gids),
            ]
        )
        self.aliases = unique_text(
            [
                self.public_id,
                self.root_gid,
                self.effective_gid,
                *as_list(self.aliases),
            ]
        )
        self.tags = unique_text(self.tags)
        self.file_paths = unique_text(self.file_paths)
        self.episodes = sorted(
            {to_int(item) for item in as_list(self.episodes) if item is not None}
        )
        self.selected_episodes = sorted(
            {
                to_int(item)
                for item in as_list(self.selected_episodes)
                if item is not None
            }
        )
        self.info_hash = normalize_info_hash(self.info_hash)
        return self

    def touch(self) -> None:
        self.updated_at = utc_now_text()

    def matches(self, value: Any) -> bool:
        text = str(value or "")
        return text == self.public_id or text in self.aliases or text in self.related_gids

    def merge(self, other: "TaskRecord") -> "TaskRecord":
        self.root_gid = other.root_gid or self.root_gid
        self.effective_gid = other.effective_gid or self.effective_gid
        self.related_gids = unique_text([*self.related_gids, *other.related_gids])
        self.aliases = unique_text([*self.aliases, *other.aliases])
        self.owned = self.owned or other.owned
        self.downloader = other.downloader or self.downloader
        self.info_hash = other.info_hash or self.info_hash
        self.bt_expected = self.bt_expected or other.bt_expected
        self.tags = unique_text([*self.tags, *other.tags])
        self.category = other.category if other.category is not None else self.category
        self.episodes = sorted(set(self.episodes).union(other.episodes))
        self.selected_episodes = sorted(
            set(self.selected_episodes).union(other.selected_episodes)
        )
        self.done = self.done or other.done
        if other.file_paths:
            self.file_paths = unique_text(other.file_paths)
        self.migration_pending = self.migration_pending and other.migration_pending
        self.remove_pending = self.remove_pending or other.remove_pending
        return self.normalize()

    def to_dict(self) -> Dict[str, Any]:
        self.normalize()
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskRecord":
        values = dict(data or {})
        allowed = cls.__dataclass_fields__.keys()
        return cls(
            **{key: value for key, value in values.items() if key in allowed}
        ).normalize()

    @classmethod
    def from_legacy(
        cls, gid: str, data: Dict[str, Any], default_downloader: str
    ) -> "TaskRecord":
        data = data or {}
        return cls(
            public_id=str(gid),
            root_gid=str(gid),
            effective_gid=str(gid),
            related_gids=[str(gid)],
            aliases=[str(gid)],
            owned=True,
            downloader=str(data.get("downloader") or default_downloader or ""),
            tags=unique_text(data.get("tags") or []),
            category=data.get("category"),
            episodes=as_list(data.get("episodes")),
            selected_episodes=as_list(data.get("selected_episodes")),
            done=to_bool(data.get("done")),
            migration_pending=True,
        ).normalize()


@dataclass
class ResolvedTask:
    public_id: str
    task: Optional[Dict[str, Any]]
    gids: List[str]
    record: Optional[TaskRecord] = None

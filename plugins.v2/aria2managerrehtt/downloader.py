import base64
import os
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from app.core.cache import FileCache
from app.core.config import settings
from app.core.metainfo import MetaInfo
from app.log import logger
from app.schemas import (
    DownloaderInfo,
    DownloaderTorrent,
    DownloadingTorrent,
    TransferTorrent,
)
from app.schemas.types import DownloadTaskState, TorrentQueryStatus, TorrentStatus
from app.utils.string import StringUtils

from .aria2_client import Aria2Client, Aria2RpcError
from .config import DownloaderBinding, PluginConfig
from .models import (
    Aria2File,
    ResolvedTask,
    TaskRecord,
    magnet_info_hash,
    normalize_info_hash,
    proxy_public_id,
    to_bool,
    to_int,
    unique_text,
)
from .pathing import PathMapper, PreparedDeletion, SafeFileDeleter
from .task_store import TaskStore


class Aria2Downloader:
    """MoviePilot downloader module adapter backed by a single Aria2 RPC."""

    def __init__(
        self,
        plugin_id: str,
        config: PluginConfig,
        binding: DownloaderBinding,
        client: Aria2Client,
        store: TaskStore,
        path_mapper: PathMapper,
        allowed_roots_provider,
    ):
        self.plugin_id = plugin_id
        self.config = config
        self.binding = binding
        self.client = client
        self.store = store
        self.path_mapper = path_mapper
        self.allowed_roots_provider = allowed_roots_provider
        self.last_error = ""
        self._truncation_warned = False

    @property
    def ready(self) -> bool:
        return self.config.enabled and self.config.valid and bool(self.binding.name)

    @staticmethod
    def _as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            return list(value)
        return [value]

    def _accept_list(self, downloader: Optional[str]) -> bool:
        return not downloader or downloader == self.binding.name

    def _accept_download(self, downloader: Optional[str]) -> bool:
        if downloader:
            return downloader == self.binding.name
        return self.binding.default

    def _accept_control(self, identities: Any, downloader: Optional[str]) -> bool:
        if downloader:
            return downloader == self.binding.name
        return any(self.store.find(value) for value in self._as_list(identities))

    @staticmethod
    def _task_info_hash(task: Optional[Dict[str, Any]]) -> Optional[str]:
        return normalize_info_hash((task or {}).get("infoHash"))

    @staticmethod
    def _is_data_complete(task: Optional[Dict[str, Any]]) -> bool:
        task = task or {}
        if str(task.get("status") or "") == "complete":
            return True
        if to_bool(task.get("seeder")):
            return True
        total = to_int(task.get("totalLength"))
        completed = to_int(task.get("completedLength"))
        return total > 0 and completed >= total

    @classmethod
    def _normalized_state(cls, task: Dict[str, Any]) -> str:
        status = str(task.get("status") or "")
        if status == "paused":
            return DownloadTaskState.PAUSED.value
        if cls._is_data_complete(task):
            return DownloadTaskState.COMPLETED.value
        if status in {"active", "waiting"}:
            return DownloadTaskState.DOWNLOADING.value
        return DownloadTaskState.COMPLETED.value

    @staticmethod
    def _normalize_query_status(status: Any) -> str:
        value = getattr(status, "value", status)
        text = str(value or "").strip().lower()
        if not text or text in {"all", "全部"}:
            return TorrentQueryStatus.ALL.value
        if text in {
            TorrentStatus.TRANSFER.value.lower(),
            TorrentQueryStatus.TRANSFER.value,
            "transfer",
        }:
            return TorrentQueryStatus.TRANSFER.value
        if text in {
            TorrentStatus.DOWNLOADING.value.lower(),
            TorrentQueryStatus.DOWNLOADING.value,
            "downloading",
        }:
            return TorrentQueryStatus.DOWNLOADING.value
        if text in {
            TorrentQueryStatus.COMPLETED.value,
            "complete",
            "seeding",
            "完成",
            "已完成",
        }:
            return TorrentQueryStatus.COMPLETED.value
        if text in {
            TorrentQueryStatus.PAUSED.value,
            "pause",
            "暂停",
            "已暂停",
        }:
            return TorrentQueryStatus.PAUSED.value
        return TorrentQueryStatus.ALL.value

    def _all_tasks(self) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        tasks, truncated = self.client.all_tasks()
        if truncated and not self._truncation_warned:
            logger.warning("Aria2任务超过10000条，列表已按安全上限截断")
            self._truncation_warned = True
        task_map = {
            str(task.get("gid")): task
            for task in tasks
            if task and task.get("gid")
        }
        return tasks, task_map

    @staticmethod
    def _root_gid(
        task: Dict[str, Any], task_map: Dict[str, Dict[str, Any]]
    ) -> str:
        current = task
        visited: Set[str] = set()
        root = str(task.get("gid") or "")
        for _ in range(8):
            following = str(current.get("following") or "")
            if not following or following in visited:
                break
            visited.add(following)
            root = following
            parent = task_map.get(following)
            if not parent:
                break
            current = parent
        return root

    def _external_public_id(
        self, task: Dict[str, Any], task_map: Dict[str, Dict[str, Any]]
    ) -> str:
        return self._task_info_hash(task) or proxy_public_id(
            self.plugin_id,
            self.binding.name,
            self._root_gid(task, task_map),
        )

    def _record_file_paths(self, task: Optional[Dict[str, Any]]) -> List[str]:
        result: List[str] = []
        for file_info in (task or {}).get("files") or []:
            raw_path = str((file_info or {}).get("path") or "")
            if raw_path:
                result.append(self.path_mapper.to_moviepilot(raw_path))
        return unique_text(result)

    def _sync_record(
        self,
        record: TaskRecord,
        task_map: Dict[str, Dict[str, Any]],
    ) -> Tuple[TaskRecord, Optional[Dict[str, Any]]]:
        related = set(record.related_gids or [])
        related.update({record.root_gid, record.effective_gid})
        related.discard("")
        queue = list(related)
        found: Dict[str, Dict[str, Any]] = {}

        for _ in range(8):
            next_queue: List[str] = []
            while queue:
                gid = queue.pop(0)
                if gid in found:
                    continue
                task = task_map.get(gid)
                if task is None:
                    task = self.client.tell_status_optional(gid)
                    if task:
                        task_map[gid] = task
                if not task:
                    continue
                found[gid] = task
                for child_gid in task.get("followedBy") or []:
                    child_gid = str(child_gid or "")
                    if child_gid and child_gid not in related:
                        related.add(child_gid)
                        next_queue.append(child_gid)

            for task in list(task_map.values()):
                gid = str(task.get("gid") or "")
                following = str(task.get("following") or "")
                if gid and following in related and gid not in related:
                    related.add(gid)
                    next_queue.append(gid)
            if not next_queue:
                break
            queue = next_queue

        candidates = list(found.values())
        info_hash = record.info_hash
        for task in candidates:
            info_hash = info_hash or self._task_info_hash(task)
        leaf_candidates = [
            task for task in candidates if not (task.get("followedBy") or [])
        ] or candidates
        if info_hash:
            matching = [
                task
                for task in leaf_candidates
                if self._task_info_hash(task) == info_hash
            ]
            if matching:
                leaf_candidates = matching
        effective = next(
            (
                task
                for task in leaf_candidates
                if str(task.get("gid") or "") == record.effective_gid
            ),
            leaf_candidates[-1] if leaf_candidates else None,
        )
        effective_gid = str((effective or {}).get("gid") or record.effective_gid)

        should_migrate = bool(
            record.migration_pending
            and effective
            and (
                info_hash
                or len(record.public_id) != 40
                or normalize_info_hash(record.public_id) is None
            )
        )
        if should_migrate:
            new_public_id = info_hash or proxy_public_id(
                self.plugin_id, self.binding.name, record.root_gid
            )
            record = self.store.migrate(
                record.public_id,
                new_public_id,
                info_hash,
                related,
                effective_gid,
            )

        new_related = unique_text([*record.related_gids, *related])
        new_aliases = unique_text(
            [*record.aliases, record.public_id, record.root_gid, effective_gid]
        )
        current_paths = self._record_file_paths(effective)
        new_paths = current_paths or record.file_paths
        changed = (
            record.effective_gid != effective_gid
            or record.related_gids != new_related
            or record.aliases != new_aliases
            or record.info_hash != info_hash
            or record.file_paths != new_paths
        )
        if changed:
            record.effective_gid = effective_gid
            record.related_gids = new_related
            record.aliases = new_aliases
            record.info_hash = info_hash
            record.file_paths = new_paths
            record = self.store.upsert(record)
        return record, effective

    def sync_owned_relations(self) -> None:
        _, task_map = self._all_tasks()
        for record in self.store.records(owned_only=True):
            try:
                self._sync_record(record, task_map)
            except Exception as err:
                logger.debug(f"同步Aria2任务映射失败 {record.public_id}: {err}")

    def _resolved_from_record(
        self,
        record: TaskRecord,
        task_map: Dict[str, Dict[str, Any]],
    ) -> ResolvedTask:
        record, task = self._sync_record(record, task_map)
        return ResolvedTask(
            public_id=record.public_id,
            task=task,
            gids=unique_text(record.related_gids),
            record=record,
        )

    def _external_display_tasks(
        self,
        tasks: List[Dict[str, Any]],
        task_map: Dict[str, Dict[str, Any]],
        claimed_gids: Set[str],
    ) -> List[ResolvedTask]:
        results: List[ResolvedTask] = []
        seen_public: Set[str] = set()
        for task in tasks:
            gid = str(task.get("gid") or "")
            if not gid or gid in claimed_gids or task.get("followedBy"):
                continue
            public_id = self._external_public_id(task, task_map)
            if public_id in seen_public:
                continue
            seen_public.add(public_id)
            related = [gid]
            following = str(task.get("following") or "")
            if following:
                related.append(following)
            results.append(
                ResolvedTask(
                    public_id=public_id,
                    task=task,
                    gids=unique_text(related),
                )
            )
        return results

    def _resolve(
        self,
        identity: Any,
        task_map: Optional[Dict[str, Dict[str, Any]]] = None,
        allow_external: bool = False,
    ) -> Optional[ResolvedTask]:
        text = str(identity or "")
        if not text:
            return None
        task_map = task_map if task_map is not None else {}
        record = self.store.find(text)
        if record:
            return self._resolved_from_record(record, task_map)
        if not allow_external:
            return None

        tasks: List[Dict[str, Any]]
        if not task_map:
            tasks, task_map = self._all_tasks()
        else:
            tasks = list(task_map.values())
        direct = task_map.get(text)
        if direct:
            public_id = self._external_public_id(direct, task_map)
            return ResolvedTask(public_id=public_id, task=direct, gids=[text])
        for resolved in self._external_display_tasks(tasks, task_map, set()):
            if resolved.public_id == text or text in resolved.gids:
                return resolved
        return None

    def _task_path(self, task: Dict[str, Any]) -> Path:
        raw_dir = str(task.get("dir") or "")
        bittorrent = task.get("bittorrent") or {}
        info = bittorrent.get("info") or {}
        info_name = str(info.get("name") or "")
        files = task.get("files") or []
        if raw_dir and info_name:
            return Path(
                self.path_mapper.to_moviepilot(Path(raw_dir) / info_name)
            )
        raw_paths = [
            str((file_info or {}).get("path") or "")
            for file_info in files
            if (file_info or {}).get("path")
        ]
        mapped_paths = [
            Path(self.path_mapper.to_moviepilot(raw_path))
            for raw_path in raw_paths
        ]
        if len(mapped_paths) == 1:
            return mapped_paths[0]
        if len(mapped_paths) > 1:
            try:
                return Path(os.path.commonpath([path.as_posix() for path in mapped_paths]))
            except ValueError:
                return mapped_paths[0].parent
        if raw_dir:
            return Path(self.path_mapper.to_moviepilot(raw_dir))
        return Path("/")

    @staticmethod
    def _task_title(task: Dict[str, Any]) -> str:
        bittorrent = task.get("bittorrent") or {}
        info = bittorrent.get("info") or {}
        if info.get("name"):
            return str(info["name"])
        files = task.get("files") or []
        if files:
            first = files[0] or {}
            if first.get("path"):
                return Path(str(first["path"])).name
            uris = first.get("uris") or []
            if uris and (uris[0] or {}).get("uri"):
                uri = str(uris[0]["uri"])
                parsed = urllib.parse.urlparse(uri)
                return urllib.parse.unquote(Path(parsed.path or uri).name)
        return str(task.get("gid") or "")

    def _to_file_entries(self, task: Dict[str, Any]) -> List[Aria2File]:
        raw_dir = Path(str(task.get("dir") or "/"))
        result: List[Aria2File] = []
        for fallback_index, file_info in enumerate(task.get("files") or [], start=1):
            if not file_info or not file_info.get("path"):
                continue
            raw_path = Path(str(file_info["path"]))
            try:
                name = raw_path.relative_to(raw_dir).as_posix()
            except ValueError:
                name = raw_path.name
            size = to_int(file_info.get("length"))
            completed = to_int(file_info.get("completedLength"))
            selected = to_bool(file_info.get("selected", True))
            result.append(
                Aria2File(
                    {
                        "id": to_int(file_info.get("index"), fallback_index),
                        "index": to_int(file_info.get("index"), fallback_index),
                        "name": name,
                        "path": self.path_mapper.to_moviepilot(raw_path),
                        "size": size,
                        "completed": completed,
                        "priority": 1 if selected else 0,
                        "progress": round(completed * 100 / size, 2) if size else 0,
                        "selected": selected,
                    }
                )
            )
        return result

    @staticmethod
    def _progress(task: Dict[str, Any]) -> float:
        total = to_int(task.get("totalLength"))
        completed = to_int(task.get("completedLength"))
        return round(completed * 100 / total, 2) if total > 0 else 0.0

    @staticmethod
    def _left_time(task: Dict[str, Any]) -> str:
        total = to_int(task.get("totalLength"))
        completed = to_int(task.get("completedLength"))
        speed = to_int(task.get("downloadSpeed"))
        if speed <= 0 or completed >= total:
            return ""
        seconds = int((total - completed) / speed)
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f"{hours}h{minutes}m{seconds}s"
        if minutes:
            return f"{minutes}m{seconds}s"
        return f"{seconds}s"

    def _build_torrent(
        self,
        resolved: ResolvedTask,
        query_status: str,
    ) -> DownloaderTorrent:
        task = resolved.task or {}
        record = resolved.record
        title = self._task_title(task)
        meta = MetaInfo(title)
        path = self._task_path(task)
        save_path = (
            self.path_mapper.to_moviepilot(str(task.get("dir")))
            if task.get("dir")
            else None
        )
        model_class = DownloaderTorrent
        if query_status == TorrentQueryStatus.DOWNLOADING.value:
            model_class = DownloadingTorrent
        elif query_status == TorrentQueryStatus.TRANSFER.value:
            model_class = TransferTorrent
        return model_class(
            downloader=self.binding.name,
            hash=resolved.public_id,
            title=title,
            name=meta.name or title,
            year=meta.year,
            season_episode=meta.season_episode,
            path=path,
            save_path=save_path,
            content_path=path.as_posix(),
            size=to_int(task.get("totalLength")),
            progress=self._progress(task),
            state=self._normalized_state(task),
            dlspeed=StringUtils.str_filesize(to_int(task.get("downloadSpeed"))),
            upspeed=StringUtils.str_filesize(to_int(task.get("uploadSpeed"))),
            tags=",".join(record.tags) if record else "",
            category=record.category if record else None,
            left_time=self._left_time(task),
        )

    def _matches_status(
        self,
        resolved: ResolvedTask,
        query_status: str,
    ) -> bool:
        task = resolved.task or {}
        state = self._normalized_state(task)
        if query_status == TorrentQueryStatus.ALL.value:
            return True
        if query_status == TorrentQueryStatus.TRANSFER.value:
            return (
                self._is_data_complete(task)
                and bool(resolved.record and resolved.record.owned)
                and not bool(resolved.record and resolved.record.done)
                and not bool(
                    resolved.record
                    and resolved.record.bt_expected
                    and not self._task_info_hash(task)
                )
            )
        if query_status == TorrentQueryStatus.DOWNLOADING.value:
            return state == "downloading"
        if query_status == TorrentQueryStatus.COMPLETED.value:
            return state == "completed"
        if query_status == TorrentQueryStatus.PAUSED.value:
            return state == "paused"
        return True

    def _build_add_options(
        self, download_dir: Path, cookie: str = "", pause: bool = False
    ) -> Dict[str, Any]:
        options: Dict[str, Any] = {
            "dir": self.path_mapper.to_downloader(download_dir)
        }
        if pause:
            options["pause"] = "true"
        if cookie:
            options["header"] = [f"Cookie: {cookie}"]
        return options

    @staticmethod
    def _path_content(path: Path) -> Optional[bytes]:
        if path.exists():
            return path.read_bytes()
        return FileCache().get(path.as_posix(), region="torrents")

    def _select_episode_files(
        self, gid: str, episodes: Set[int]
    ) -> Tuple[bool, str, List[int]]:
        task = self.client.tell_status(gid)
        selected_indices: List[str] = []
        selected_episodes: Set[int] = set()
        for file_info in self._to_file_entries(task):
            meta = MetaInfo(Path(file_info.name).stem)
            episode_list = set(meta.episode_list or [])
            if episode_list and episode_list.issubset(episodes):
                selected_indices.append(str(file_info.index))
                selected_episodes.update(episode_list)
        if not selected_indices:
            return False, "未匹配到需要下载的集数文件", []
        if not self.client.change_option(
            gid, {"select-file": ",".join(selected_indices)}
        ):
            return False, "Aria2未接受选集文件设置", []
        return (
            True,
            f"已选择集数：{sorted(selected_episodes)}",
            sorted(selected_episodes),
        )

    def download(
        self,
        content: Union[Path, str, bytes],
        download_dir: Path,
        cookie: str,
        episodes: set = None,
        category: Optional[str] = None,
        label: Optional[str] = None,
        downloader: Optional[str] = None,
    ) -> Optional[Tuple[Optional[str], Optional[str], Optional[str], str]]:
        if not self.ready or not self._accept_download(downloader):
            return None
        if not content:
            return self.binding.name, None, "Original", "下载内容为空"
        gid = ""
        try:
            direct_content: Optional[bytes] = None
            magnet_content: Optional[Union[str, bytes]] = None
            remote_uri: Optional[str] = None
            if isinstance(content, Path):
                direct_content = self._path_content(content)
                if direct_content is None:
                    return (
                        self.binding.name,
                        None,
                        "Original",
                        "种子文件不存在",
                    )
            elif isinstance(content, bytes):
                if content.startswith(b"magnet:"):
                    magnet_content = content
                else:
                    direct_content = content
            elif isinstance(content, str):
                if content.startswith("magnet:"):
                    magnet_content = content
                elif content.startswith(("http://", "https://")):
                    remote_uri = content
                else:
                    return (
                        self.binding.name,
                        None,
                        "Original",
                        "不支持的下载内容格式",
                    )
            else:
                return self.binding.name, None, "Original", "不支持的下载内容类型"

            select_episodes = bool(episodes and direct_content)
            options = self._build_add_options(
                download_dir=download_dir,
                cookie=cookie if remote_uri else "",
                pause=select_episodes,
            )
            if direct_content is not None:
                gid = self.client.add_torrent(
                    base64.b64encode(direct_content).decode("utf-8"), options
                )
            else:
                uri = (
                    magnet_content.decode("utf-8")
                    if isinstance(magnet_content, bytes)
                    else magnet_content or remote_uri
                )
                gid = self.client.add_uri(str(uri), options)
            if not gid:
                return self.binding.name, None, "Original", "添加下载失败"

            selected_episodes: List[int] = []
            message = "添加下载任务成功"
            if select_episodes:
                selected, select_message, selected_episodes = (
                    self._select_episode_files(gid, set(episodes))
                )
                if not selected:
                    self.client.remove_many([gid])
                    return self.binding.name, None, "Original", select_message
                if not self.client.unpause(gid):
                    self.client.remove_many([gid])
                    return (
                        self.binding.name,
                        None,
                        "Original",
                        "选集完成但恢复下载失败，任务已回滚",
                    )
                message = f"添加下载任务成功，{select_message}"
            elif episodes and (magnet_content or remote_uri):
                message = "添加下载任务成功；链接任务暂不支持预选文件，已按全集下载"

            initial_task = self.client.tell_status_optional(gid) or {"gid": gid}
            info_hash = magnet_info_hash(magnet_content) or self._task_info_hash(
                initial_task
            )
            public_id = info_hash or proxy_public_id(
                self.plugin_id, self.binding.name, gid
            )
            tags = unique_text(
                [
                    *(str(label or "").split(",") if label else []),
                    getattr(settings, "TORRENT_TAG", ""),
                ]
            )
            remote_path = (
                urllib.parse.urlparse(remote_uri).path.lower()
                if remote_uri
                else ""
            )
            bt_expected = bool(
                direct_content is not None
                or magnet_content is not None
                or remote_path.endswith(".torrent")
                or initial_task.get("bittorrent")
                or initial_task.get("followedBy")
            )
            record = TaskRecord(
                public_id=public_id,
                root_gid=gid,
                effective_gid=gid,
                related_gids=[gid],
                aliases=[gid, public_id],
                owned=True,
                downloader=self.binding.name,
                info_hash=info_hash,
                bt_expected=bt_expected,
                tags=tags,
                category=category,
                episodes=sorted(set(episodes or [])),
                selected_episodes=selected_episodes,
                file_paths=self._record_file_paths(initial_task),
                migration_pending=info_hash is None,
            )
            try:
                self.store.upsert(record)
            except Exception:
                self.client.remove_many([gid])
                raise
            self.last_error = ""
            return self.binding.name, public_id, "Original", message
        except Exception as err:
            self.last_error = str(err)
            logger.error(f"Aria2添加下载失败：{self.last_error}")
            if gid:
                self.client.remove_many([gid])
            return (
                self.binding.name,
                None,
                "Original",
                f"添加下载失败：{self.last_error}",
            )

    def list_torrents(
        self,
        status: Any = None,
        hashs: Union[list, str] = None,
        downloader: Optional[str] = None,
        include_all_tags: bool = False,
    ) -> Optional[List[DownloaderTorrent]]:
        if not self.ready or not self._accept_list(downloader):
            return None
        query_status = self._normalize_query_status(status)
        try:
            tasks, task_map = self._all_tasks()
            resolved_tasks: List[ResolvedTask] = []
            if hashs:
                for identity in self._as_list(hashs):
                    resolved = self._resolve(
                        identity,
                        task_map=task_map,
                        allow_external=include_all_tags,
                    )
                    if resolved:
                        resolved_tasks.append(resolved)
            else:
                claimed: Set[str] = set()
                records = self.store.records(owned_only=not include_all_tags)
                for record in records:
                    resolved = self._resolved_from_record(record, task_map)
                    claimed.update(resolved.gids)
                    if resolved.task:
                        resolved_tasks.append(resolved)
                if include_all_tags:
                    resolved_tasks.extend(
                        self._external_display_tasks(
                            tasks, task_map, claimed_gids=claimed
                        )
                    )

            results: List[DownloaderTorrent] = []
            seen: Set[str] = set()
            for resolved in resolved_tasks:
                if (
                    not resolved.task
                    or resolved.public_id in seen
                    or not self._matches_status(resolved, query_status)
                ):
                    continue
                seen.add(resolved.public_id)
                results.append(self._build_torrent(resolved, query_status))
            self.last_error = ""
            return results
        except Exception as err:
            self.last_error = str(err)
            logger.error(f"Aria2查询任务失败：{self.last_error}")
            return None

    def transfer_completed(
        self, hashs: Union[list, str], downloader: Optional[str] = None
    ) -> None:
        if not self.ready or not self._accept_control(hashs, downloader):
            return None
        for identity in self._as_list(hashs):
            record = self.store.find(identity)
            if not record:
                continue
            self.store.update(
                record.public_id,
                done=True,
                tags=unique_text([*record.tags, "已整理"]),
            )
        return None

    def _resolved_many(
        self, identities: Any, allow_external: bool
    ) -> List[ResolvedTask]:
        _, task_map = self._all_tasks()
        results: List[ResolvedTask] = []
        seen: Set[str] = set()
        for identity in self._as_list(identities):
            resolved = self._resolve(
                identity, task_map=task_map, allow_external=allow_external
            )
            if resolved and resolved.public_id not in seen:
                seen.add(resolved.public_id)
                results.append(resolved)
        return results

    def remove_torrents(
        self,
        hashs: Union[list, str],
        delete_file: bool = True,
        downloader: Optional[str] = None,
    ) -> Optional[bool]:
        if not self.ready or not self._accept_control(hashs, downloader):
            return None
        try:
            resolved_tasks = self._resolved_many(
                hashs, allow_external=bool(downloader)
            )
            if not resolved_tasks:
                return False
            overall = True
            for resolved in resolved_tasks:
                current_paths = self._record_file_paths(resolved.task)
                paths = unique_text(
                    current_paths
                    or (
                        resolved.record.file_paths
                        if resolved.record
                        else []
                    )
                )
                if (
                    delete_file
                    and resolved.task
                    and to_int(resolved.task.get("totalLength")) > 0
                    and not paths
                ):
                    logger.error(
                        f"拒绝删除 {resolved.public_id}："
                        "Aria2任务有数据但未返回可校验的文件路径"
                    )
                    overall = False
                    continue
                prepared = PreparedDeletion()
                deleter: Optional[SafeFileDeleter] = None
                if delete_file and paths:
                    sidecars = [f"{path}.aria2" for path in paths]
                    if resolved.task:
                        task_path = self._task_path(resolved.task)
                        if task_path.as_posix() != "/":
                            sidecars.append(f"{task_path}.aria2")
                    candidates = unique_text(
                        [*paths, *sidecars]
                    )
                    deleter = SafeFileDeleter(self.allowed_roots_provider())
                    prepared = deleter.prepare(candidates)
                    if not prepared.valid:
                        logger.error("；".join(prepared.errors))
                        overall = False
                        continue

                rpc_ok = self.client.remove_many(resolved.gids)
                if not rpc_ok:
                    overall = False
                    if resolved.record:
                        self.store.update(
                            resolved.record.public_id,
                            remove_pending=True,
                            file_paths=paths,
                        )
                    continue
                files_ok = True
                if delete_file and deleter:
                    delete_result = deleter.execute(prepared)
                    files_ok = delete_result.success
                    if delete_result.errors:
                        logger.error("；".join(delete_result.errors))
                if rpc_ok and files_ok:
                    if resolved.record:
                        self.store.remove(resolved.record.public_id)
                else:
                    overall = False
                    if resolved.record:
                        self.store.update(
                            resolved.record.public_id,
                            remove_pending=True,
                            file_paths=paths,
                        )
            return overall
        except Exception as err:
            self.last_error = str(err)
            logger.error(f"Aria2删除任务失败：{self.last_error}")
            return False

    def set_torrents_tag(
        self,
        hashs: Union[list, str],
        tags: list,
        downloader: Optional[str] = None,
    ) -> Optional[bool]:
        if not self.ready or not self._accept_control(hashs, downloader):
            return None
        try:
            resolved_tasks = self._resolved_many(
                hashs, allow_external=bool(downloader)
            )
            if not resolved_tasks:
                return False
            for resolved in resolved_tasks:
                record = resolved.record
                if not record:
                    task = resolved.task or {}
                    record = TaskRecord(
                        public_id=resolved.public_id,
                        root_gid=self._root_gid(task, {str(task.get("gid")): task}),
                        effective_gid=str(task.get("gid") or ""),
                        related_gids=resolved.gids,
                        aliases=resolved.gids,
                        owned=False,
                        downloader=self.binding.name,
                        info_hash=self._task_info_hash(task),
                    )
                record.tags = unique_text([*record.tags, *self._as_list(tags)])
                self.store.upsert(record)
            return True
        except Exception as err:
            self.last_error = str(err)
            return False

    def _operate(
        self,
        hashs: Any,
        downloader: Optional[str],
        action: str,
    ) -> Optional[bool]:
        if not self.ready or not self._accept_control(hashs, downloader):
            return None
        try:
            resolved_tasks = self._resolved_many(
                hashs, allow_external=bool(downloader)
            )
            if not resolved_tasks:
                return False
            ok = True
            for resolved in resolved_tasks:
                gid = str((resolved.task or {}).get("gid") or "")
                if not gid:
                    ok = False
                    continue
                status = str((resolved.task or {}).get("status") or "")
                if action == "start":
                    if status == "paused":
                        ok = self.client.unpause(gid) and ok
                    elif status not in {"active", "waiting"}:
                        ok = False
                elif action == "stop":
                    if status in {"active", "waiting"}:
                        ok = self.client.pause(gid) and ok
                    elif status != "paused":
                        ok = False
            return ok
        except Exception as err:
            self.last_error = str(err)
            logger.error(f"Aria2任务{action}失败：{self.last_error}")
            return False

    def start_torrents(
        self, hashs: Union[list, str], downloader: Optional[str] = None
    ) -> Optional[bool]:
        return self._operate(hashs, downloader, "start")

    def stop_torrents(
        self, hashs: Union[list, str], downloader: Optional[str] = None
    ) -> Optional[bool]:
        return self._operate(hashs, downloader, "stop")

    def torrent_files(
        self, tid: str, downloader: Optional[str] = None
    ) -> Optional[List[Aria2File]]:
        if not self.ready or not self._accept_control(tid, downloader):
            return None
        try:
            resolved = self._resolved_many(
                [tid], allow_external=bool(downloader)
            )
            if not resolved or not resolved[0].task:
                return None
            return self._to_file_entries(resolved[0].task)
        except Exception as err:
            self.last_error = str(err)
            return None

    @staticmethod
    def _speed_option(value: float) -> str:
        return str(max(0, int(float(value) * 1024)))

    def update_torrent(
        self,
        hash_string: str,
        downloader: Optional[str] = None,
        download_limit: Optional[float] = None,
        upload_limit: Optional[float] = None,
        tracker_list: Optional[list] = None,
        save_path: Optional[str] = None,
        category: Optional[str] = None,
        ratio_limit: Optional[float] = None,
        seeding_time_limit: Optional[int] = None,
    ) -> Optional[Dict[str, bool]]:
        if not self.ready or not self._accept_control(hash_string, downloader):
            return None
        try:
            resolved_tasks = self._resolved_many(
                [hash_string], allow_external=bool(downloader)
            )
            if not resolved_tasks or not resolved_tasks[0].task:
                return {}
            resolved = resolved_tasks[0]
            task = resolved.task or {}
            gid = str(task.get("gid") or "")
            results: Dict[str, bool] = {}
            errors: List[str] = []
            limit_options: Dict[str, str] = {}
            try:
                if download_limit is not None:
                    limit_options["max-download-limit"] = self._speed_option(
                        download_limit
                    )
                if upload_limit is not None:
                    limit_options["max-upload-limit"] = self._speed_option(
                        upload_limit
                    )
                if ratio_limit is not None:
                    limit_options["seed-ratio"] = str(
                        max(0, float(ratio_limit))
                    )
                if seeding_time_limit is not None:
                    limit_options["seed-time"] = str(
                        max(0, int(seeding_time_limit))
                    )
                if limit_options:
                    results["limits"] = self.client.change_option(
                        gid, limit_options
                    )
            except (ValueError, TypeError, Aria2RpcError) as err:
                results["limits"] = False
                errors.append(f"限速/做种设置失败：{err}")
            if tracker_list is not None:
                try:
                    trackers = unique_text(tracker_list)
                    results["trackers"] = self.client.change_option(
                        gid,
                        {
                            "bt-exclude-tracker": "*",
                            "bt-tracker": ",".join(trackers),
                        },
                    )
                except Aria2RpcError as err:
                    results["trackers"] = False
                    errors.append(f"Tracker设置失败：{err}")
            if save_path is not None:
                status = str(task.get("status") or "")
                can_move = (
                    status in {"waiting", "paused"}
                    and to_int(task.get("completedLength")) == 0
                )
                if not can_move:
                    results["save_path"] = False
                else:
                    try:
                        results["save_path"] = self.client.change_option(
                            gid,
                            {
                                "dir": self.path_mapper.to_downloader(
                                    Path(save_path)
                                )
                            },
                        )
                    except Aria2RpcError as err:
                        results["save_path"] = False
                        errors.append(f"保存目录设置失败：{err}")
            if category is not None:
                try:
                    record = resolved.record
                    if not record:
                        record = TaskRecord(
                            public_id=resolved.public_id,
                            root_gid=self._root_gid(task, {gid: task}),
                            effective_gid=gid,
                            related_gids=resolved.gids,
                            aliases=resolved.gids,
                            owned=False,
                            downloader=self.binding.name,
                            info_hash=self._task_info_hash(task),
                        )
                    record.category = category
                    self.store.upsert(record)
                    results["category"] = True
                except Exception as err:
                    results["category"] = False
                    errors.append(f"分类设置失败：{err}")
            self.last_error = "；".join(errors)
            return results
        except (ValueError, TypeError, Aria2RpcError) as err:
            self.last_error = str(err)
            logger.error(f"Aria2更新任务失败：{self.last_error}")
            return {}

    def get_torrent_trackers(
        self, hash_string: str, downloader: Optional[str] = None
    ) -> Optional[Dict[str, List[str]]]:
        if not self.ready or not self._accept_control(hash_string, downloader):
            return None
        try:
            resolved_tasks = self._resolved_many(
                [hash_string], allow_external=bool(downloader)
            )
            if not resolved_tasks or not resolved_tasks[0].task:
                return {}
            announce_list = (
                (resolved_tasks[0].task.get("bittorrent") or {}).get(
                    "announceList"
                )
                or []
            )
            trackers = unique_text(
                tracker
                for tier in announce_list
                for tracker in (tier if isinstance(tier, list) else [tier])
            )
            return {self.binding.name: trackers}
        except Exception as err:
            self.last_error = str(err)
            return {}

    def downloader_info(
        self, downloader: Optional[str] = None
    ) -> Optional[List[DownloaderInfo]]:
        if not self.ready or not self._accept_list(downloader):
            return None
        try:
            stat = self.client.get_global_stat()
            return [
                DownloaderInfo(
                    download_speed=to_int(stat.get("downloadSpeed")),
                    upload_speed=to_int(stat.get("uploadSpeed")),
                    download_size=0,
                    upload_size=0,
                )
            ]
        except Exception as err:
            self.last_error = str(err)
            return None

    def refresh_status(self) -> Dict[str, Any]:
        try:
            stat = self.client.get_global_stat()
            tasks, task_map = self._all_tasks()
            for record in self.store.records(owned_only=True):
                try:
                    self._sync_record(record, task_map)
                except Exception as err:
                    logger.debug(f"同步Aria2任务关系失败：{err}")
            stopped_complete = sum(
                1
                for task in tasks
                if task.get("status") == "complete"
            )
            stopped_error = sum(
                1 for task in tasks if task.get("status") == "error"
            )
            self.last_error = ""
            return {
                "connection": "ok",
                "active": to_int(stat.get("numActive")),
                "waiting": to_int(stat.get("numWaiting")),
                "stopped": to_int(stat.get("numStopped")),
                "stopped_complete": stopped_complete,
                "stopped_error": stopped_error,
                "download_speed": to_int(stat.get("downloadSpeed")),
                "upload_speed": to_int(stat.get("uploadSpeed")),
            }
        except Exception as err:
            self.last_error = str(err)
            logger.error(f"Aria2状态刷新失败：{self.last_error}")
            return {
                "connection": "error",
                "active": 0,
                "waiting": 0,
                "stopped": 0,
                "stopped_complete": 0,
                "stopped_error": 0,
                "download_speed": 0,
                "upload_speed": 0,
            }

    def pause_all(self) -> bool:
        try:
            return self.client.pause_all()
        except Exception as err:
            self.last_error = str(err)
            return False

    def unpause_all(self) -> bool:
        try:
            return self.client.unpause_all()
        except Exception as err:
            self.last_error = str(err)
            return False

    def purge_done(self) -> bool:
        try:
            _, task_map = self._all_tasks()
            ok = True
            for record in self.store.records(owned_only=True):
                record, task = self._sync_record(record, task_map)
                if not task:
                    if record.done:
                        self.store.remove(record.public_id)
                    continue
                status = str(task.get("status") or "")
                if status not in {"complete", "error", "removed"}:
                    continue
                if status == "complete" and not record.done:
                    continue
                task_ok = self.client.remove_many(record.related_gids)
                ok = task_ok and ok
                if task_ok:
                    self.store.remove(record.public_id)
            return ok
        except Exception as err:
            self.last_error = str(err)
            return False

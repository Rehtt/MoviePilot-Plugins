import json
import urllib.request
import urllib.parse
import base64
import os
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

from app.core.event import eventmanager, Event
from app.core.metainfo import MetaInfo
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType, TransferTorrent, DownloadingTorrent, DownloaderInfo
from app.schemas.types import EventType, TorrentStatus
from app.utils.string import StringUtils


class Aria2File(dict):
    """
    Dict with attribute access, matching the access patterns MoviePilot uses for downloader files.
    """

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as err:
            raise AttributeError(item) from err


class Aria2ManagerRehtt(_PluginBase):
    # 插件名称
    plugin_name = "Aria2 下载管理"
    # 插件描述
    plugin_desc = "在下载管理中控制和监控 Aria2 任务状态。"
    # 插件图标（使用在线图标，避免仓库内额外资源依赖）
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/download.png"
    # 插件版本
    plugin_version = "1.2"
    # 插件作者
    plugin_author = "Rehtt"
    # 作者主页
    author_url = "https://github.com/Rehtt"
    # 插件配置项ID前缀
    plugin_config_prefix = "aria2managerrehtt_"
    # 加载顺序
    plugin_order = 20
    # 可使用的用户级别
    auth_level = 1

    _enabled: bool = False
    _notify: bool = False
    _rpc_url: str = "http://127.0.0.1:6800/jsonrpc"
    _rpc_secret: str = ""
    _timeout: int = 8
    _monitor_interval: int = 60
    _downloader_type: str = "aria2managerrehtt"
    _last_status: Dict[str, Any] = {}
    _last_error: str = ""
    _task_data_key: str = "tasks"
    _done_tag: str = "已整理"
    _status_fields: List[str] = [
        "gid",
        "status",
        "totalLength",
        "completedLength",
        "downloadSpeed",
        "uploadSpeed",
        "dir",
        "files",
        "bittorrent",
        "errorMessage",
    ]

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify"))
            self._rpc_url = (config.get("rpc_url") or self._rpc_url).strip()
            self._rpc_secret = (config.get("rpc_secret") or "").strip()
            self._timeout = int(config.get("timeout") or 8)
            self._monitor_interval = int(config.get("monitor_interval") or 60)
            self._downloader_type = (config.get("downloader_type") or "aria2managerrehtt").strip().lower()
        if self._enabled:
            self.refresh_status()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/aria2_status",
                "event": EventType.PluginAction,
                "desc": "查看 Aria2 状态",
                "category": "下载管理",
                "data": {"action": "aria2_status"},
            },
            {
                "cmd": "/aria2_pause_all",
                "event": EventType.PluginAction,
                "desc": "暂停 Aria2 全部任务",
                "category": "下载管理",
                "data": {"action": "aria2_pause_all"},
            },
            {
                "cmd": "/aria2_unpause_all",
                "event": EventType.PluginAction,
                "desc": "恢复 Aria2 全部任务",
                "category": "下载管理",
                "data": {"action": "aria2_unpause_all"},
            },
            {
                "cmd": "/aria2_purge_done",
                "event": EventType.PluginAction,
                "desc": "清理 Aria2 已完成/错误任务",
                "category": "下载管理",
                "data": {"action": "aria2_purge_done"},
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "summary": "获取 Aria2 状态",
                "description": "获取 Aria2 的任务与速度概览状态",
            },
            {
                "path": "/action",
                "endpoint": self.api_action,
                "methods": ["POST"],
                "summary": "执行 Aria2 控制动作",
                "description": "支持 pause_all / unpause_all / purge_done",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._monitor_interval > 0:
            return [
                {
                    "id": "Aria2Monitor",
                    "name": "Aria2 状态监控",
                    "trigger": "interval",
                    "func": self.monitor_service,
                    "kwargs": {"seconds": self._monitor_interval},
                }
            ]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "notify", "label": "状态异常通知"},
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "rpc_url",
                                            "label": "Aria2 RPC 地址",
                                            "placeholder": "http://127.0.0.1:6800/jsonrpc",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "timeout",
                                            "label": "HTTP超时（秒）",
                                            "placeholder": "8",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "rpc_secret",
                                            "label": "RPC Secret（可选）",
                                            "type": "password",
                                            "placeholder": "不填则不携带 token",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "downloader_type",
                                            "label": "自定义下载器类型标识",
                                            "placeholder": "aria2managerrehtt",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "monitor_interval",
                                            "label": "监控间隔（秒）",
                                            "placeholder": "60",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "命令：/aria2_status /aria2_pause_all /aria2_unpause_all /aria2_purge_done",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": False,
            "rpc_url": "http://127.0.0.1:6800/jsonrpc",
            "rpc_secret": "",
            "timeout": 8,
            "monitor_interval": 60,
            "downloader_type": "aria2managerrehtt",
        }

    def get_page(self) -> List[dict]:
        status = self._last_status or {}
        text = (
            f"连接状态：{status.get('connection', 'unknown')}\n"
            f"活跃：{status.get('active', 0)}\n"
            f"等待：{status.get('waiting', 0)}\n"
            f"停止：{status.get('stopped', 0)}\n"
            f"下载速度：{status.get('download_speed', 0)} B/s\n"
            f"上传速度：{status.get('upload_speed', 0)} B/s"
        )
        if self._last_error:
            text = f"{text}\n最近错误：{self._last_error}"
        return [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "title": "Aria2 监控状态",
                                    "text": text,
                                },
                            }
                        ],
                    }
                ],
            }
        ]

    def stop_service(self):
        pass

    def get_module(self) -> Dict[str, Any]:
        """
        声明下载器模块劫持方法。
        仅当目标下载器配置 type == downloader_type 时生效。
        """
        return {
            "download": self.download,
            "list_torrents": self.list_torrents,
            "transfer_completed": self.transfer_completed,
            "remove_torrents": self.remove_torrents,
            "set_torrents_tag": self.set_torrents_tag,
            "start_torrents": self.start_torrents,
            "stop_torrents": self.stop_torrents,
            "torrent_files": self.torrent_files,
            "downloader_info": self.downloader_info,
        }

    def api_status(self):
        return self.refresh_status()

    def api_action(self, action: Optional[str] = None):
        if action == "pause_all":
            return {"ok": self.pause_all()}
        if action == "unpause_all":
            return {"ok": self.unpause_all()}
        if action == "purge_done":
            return {"ok": self.purge_done()}
        return {"ok": False, "message": "unsupported action"}

    @eventmanager.register(EventType.PluginAction)
    def handle_plugin_action(self, event: Event):
        if not self._enabled or not event:
            return
        data = event.event_data or {}
        action = data.get("action")
        if action == "aria2_status":
            status = self.refresh_status()
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【Aria2 下载管理】",
                text=(
                    f"连接：{status.get('connection', 'unknown')}\n"
                    f"活跃：{status.get('active', 0)}\n"
                    f"等待：{status.get('waiting', 0)}\n"
                    f"停止：{status.get('stopped', 0)}\n"
                    f"下载：{status.get('download_speed', 0)} B/s\n"
                    f"上传：{status.get('upload_speed', 0)} B/s"
                ),
            )
        elif action == "aria2_pause_all":
            ok = self.pause_all()
            self._notify_action("暂停全部任务", ok)
        elif action == "aria2_unpause_all":
            ok = self.unpause_all()
            self._notify_action("恢复全部任务", ok)
        elif action == "aria2_purge_done":
            ok = self.purge_done()
            self._notify_action("清理完成/错误任务", ok)

    def monitor_service(self, event: Event = None):
        if not self._enabled:
            return
        status = self.refresh_status()
        if self._notify and status.get("connection") != "ok":
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【Aria2 下载管理】",
                text=f"监控异常：{self._last_error or 'Aria2 不可达'}",
            )

    def _rpc_call(self, method: str, params: Optional[List[Any]] = None) -> Any:
        payload: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": "moviepilot-aria2",
            "method": method,
            "params": [],
        }
        call_params: List[Any] = []
        if self._rpc_secret:
            call_params.append(f"token:{self._rpc_secret}")
        if params:
            call_params.extend(params)
        payload["params"] = call_params
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._rpc_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            data = resp.read().decode("utf-8")
            parsed = json.loads(data)
            if parsed.get("error"):
                raise RuntimeError(str(parsed.get("error")))
            return parsed.get("result", {})

    def _rpc_ignore_error(self, method: str, params: Optional[List[Any]] = None) -> bool:
        try:
            self._rpc_call(method, params)
            return True
        except Exception as err:
            logger.debug(f"Aria2 RPC 忽略错误 {method}: {err}")
            return False

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).lower() in ("true", "1", "yes", "y")

    @staticmethod
    def _as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, tuple) or isinstance(value, set):
            return list(value)
        return [value]

    @staticmethod
    def _unique(values: List[Any]) -> List[Any]:
        ret = []
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if text and text not in ret:
                ret.append(text)
        return ret

    @staticmethod
    def _str_filesize(value: Any) -> str:
        try:
            return StringUtils.str_filesize(int(value or 0))
        except Exception:
            return str(value or 0)

    def _load_task_meta(self) -> Dict[str, Dict[str, Any]]:
        try:
            data = self.get_data(self._task_data_key) or {}
            return data if isinstance(data, dict) else {}
        except Exception as err:
            logger.debug(f"读取 Aria2 任务元数据失败：{err}")
            return {}

    def _save_task_meta(self, data: Dict[str, Dict[str, Any]]) -> None:
        try:
            self.save_data(self._task_data_key, data or {})
        except Exception as err:
            logger.debug(f"保存 Aria2 任务元数据失败：{err}")

    def _get_task_meta(self, gid: Optional[str]) -> Dict[str, Any]:
        if not gid:
            return {}
        return self._load_task_meta().get(str(gid), {}) or {}

    def _update_task_meta(self, gid: Optional[str], **kwargs) -> None:
        if not gid:
            return
        data = self._load_task_meta()
        gid = str(gid)
        current = data.get(gid, {}) or {}
        tags = self._unique(self._as_list(current.get("tags")) + self._as_list(kwargs.pop("tags", [])))
        current.update(kwargs)
        if tags:
            current["tags"] = tags
        data[gid] = current
        self._save_task_meta(data)

    def _remove_task_meta(self, gid: Optional[str]) -> None:
        if not gid:
            return
        data = self._load_task_meta()
        if data.pop(str(gid), None) is not None:
            self._save_task_meta(data)

    def _resolve_downloader_name(self, downloader: Optional[str] = None) -> Optional[str]:
        if downloader:
            return downloader
        helper = DownloaderHelper()
        configs = helper.get_configs()
        target_type = self._target_type()
        for name, conf in configs.items():
            if conf.default and str(conf.type).lower() == target_type:
                return name
        for name, conf in configs.items():
            if str(conf.type).lower() == target_type:
                return name
        return downloader or target_type

    def _tell_status(self, gid: str, fields: Optional[List[str]] = None) -> Dict[str, Any]:
        if fields:
            return self._rpc_call("aria2.tellStatus", [gid, fields]) or {}
        return self._rpc_call("aria2.tellStatus", [gid, self._status_fields]) or {}

    def _target_type(self) -> str:
        return (self._downloader_type or "aria2managerrehtt").lower()

    def _is_target_downloader(self, downloader: Optional[str], default_only_when_empty: bool = True) -> bool:
        helper = DownloaderHelper()
        # MoviePilot 在不同场景下，可能会传入“下载器服务名”（name）或直接传入“下载器类型”（type）。
        # 这里同时兼容两种输入，避免因为识别失败导致 download 返回 None，从而影响通用下载管理页的入口显示/功能可用性。
        target_type = self._target_type()
        if downloader and str(downloader).strip().lower() == target_type:
            return True

        service = helper.get_service(name=downloader) if downloader else None
        if service and service.type:
            return str(service.type).lower() == self._target_type()
        if not downloader:
            configs = helper.get_configs()
            for conf in configs.values():
                if default_only_when_empty and conf.default:
                    return str(conf.type).lower() == self._target_type()
                if not default_only_when_empty and str(conf.type).lower() == self._target_type():
                    return True
        return False

    def _task_path(self, task: Dict[str, Any]) -> Path:
        base_dir = task.get("dir") or ""
        files = task.get("files") or []
        bt = task.get("bittorrent", {}) or {}
        info = bt.get("info", {}) or {}
        info_name = info.get("name")

        if base_dir and info_name:
            return Path(base_dir) / info_name

        if files and isinstance(files, list):
            paths = [Path(file.get("path")) for file in files if file and file.get("path")]
            if len(paths) == 1:
                return paths[0]
            if len(paths) > 1:
                try:
                    common = Path(os.path.commonpath([path.as_posix() for path in paths]))
                    return common
                except Exception:
                    return paths[0].parent
        if base_dir:
            return Path(base_dir)
        return Path("/")

    @staticmethod
    def _task_title(task: Dict[str, Any]) -> str:
        gid = task.get("gid") or ""
        bt = task.get("bittorrent", {}) or {}
        info = bt.get("info", {}) or {}
        if info.get("name"):
            return info.get("name")
        files = task.get("files") or []
        if files and isinstance(files, list):
            first = files[0] or {}
            path = first.get("path")
            if path:
                return Path(path).name
            uris = first.get("uris") or []
            if uris and isinstance(uris, list):
                uri = (uris[0] or {}).get("uri")
                if uri:
                    parsed = urllib.parse.urlparse(uri)
                    return urllib.parse.unquote(Path(parsed.path or uri).name)
        return gid

    def _task_tags(self, task: Dict[str, Any]) -> str:
        meta = self._get_task_meta(task.get("gid"))
        return ",".join(self._unique(self._as_list(meta.get("tags"))))

    def _task_done(self, task: Dict[str, Any]) -> bool:
        meta = self._get_task_meta(task.get("gid"))
        tags = self._as_list(meta.get("tags"))
        return bool(meta.get("done")) or self._done_tag in tags

    def _task_files_root(self, task: Dict[str, Any]) -> Path:
        task_path = self._task_path(task)
        files = task.get("files") or []
        if len(files) <= 1:
            return task_path.parent if task_path.suffix else task_path.parent
        return task_path.parent

    def _to_file_entries(self, task: Dict[str, Any]) -> List[Aria2File]:
        files = task.get("files") or []
        if not isinstance(files, list):
            return []
        base = self._task_files_root(task)
        ret: List[Aria2File] = []
        for idx, file in enumerate(files, start=1):
            if not file:
                continue
            absolute_path = Path(file.get("path") or "")
            index = self._to_int(file.get("index"), idx)
            size = self._to_int(file.get("length"), 0)
            completed = self._to_int(file.get("completedLength"), 0)
            selected = self._to_bool(file.get("selected", True))
            try:
                name = absolute_path.relative_to(base).as_posix()
            except Exception:
                name = absolute_path.name
            progress = round(completed * 100 / size, 2) if size > 0 else 0
            ret.append(
                Aria2File(
                    {
                        "id": index,
                        "index": index,
                        "name": name,
                        "path": absolute_path.as_posix(),
                        "size": size,
                        "completed": completed,
                        "priority": 1 if selected else 0,
                        "progress": progress,
                        "selected": selected,
                    }
                )
            )
        return ret

    def _task_progress(self, task: Dict[str, Any]) -> float:
        total = self._to_int(task.get("totalLength"), 0)
        completed = self._to_int(task.get("completedLength"), 0)
        if total <= 0:
            return 0
        return round(completed * 100 / total, 2)

    def _task_left_time(self, task: Dict[str, Any]) -> str:
        total = self._to_int(task.get("totalLength"), 0)
        completed = self._to_int(task.get("completedLength"), 0)
        speed = self._to_int(task.get("downloadSpeed"), 0)
        if speed <= 0 or completed >= total:
            return ""
        left_seconds = int((total - completed) / speed)
        if left_seconds <= 0:
            return ""
        hours = left_seconds // 3600
        minutes = (left_seconds % 3600) // 60
        seconds = left_seconds % 60
        if hours > 0:
            return f"{hours}h{minutes}m{seconds}s"
        if minutes > 0:
            return f"{minutes}m{seconds}s"
        return f"{seconds}s"

    @staticmethod
    def _task_state(status: str) -> str:
        if status in ("paused",):
            return "paused"
        if status in ("active", "waiting"):
            return "downloading"
        if status in ("error", "removed"):
            return "error"
        if status == "complete":
            return "completed"
        return "downloading"

    def _to_transfer_torrent(self, task: Dict[str, Any], downloader: Optional[str]) -> TransferTorrent:
        downloader_name = self._resolve_downloader_name(downloader)
        return TransferTorrent(
            downloader=downloader_name,
            title=self._task_title(task),
            path=self._task_path(task),
            hash=task.get("gid"),
            size=self._to_int(task.get("totalLength")),
            tags=self._task_tags(task),
            progress=self._task_progress(task),
            state=self._task_state(task.get("status") or ""),
        )

    def _to_downloading_torrent(self, task: Dict[str, Any], downloader: Optional[str]) -> DownloadingTorrent:
        downloader_name = self._resolve_downloader_name(downloader)
        title = self._task_title(task)
        meta = MetaInfo(title)
        return DownloadingTorrent(
            downloader=downloader_name,
            hash=task.get("gid"),
            title=title,
            name=meta.name or title,
            year=meta.year,
            season_episode=meta.season_episode,
            progress=self._task_progress(task),
            size=self._to_int(task.get("totalLength")),
            state=self._task_state(task.get("status") or ""),
            dlspeed=self._str_filesize(task.get("downloadSpeed")),
            upspeed=self._str_filesize(task.get("uploadSpeed")),
            tags=self._task_tags(task),
            left_time=self._task_left_time(task),
        )

    @staticmethod
    def _is_magnet(content: Any) -> bool:
        if isinstance(content, bytes):
            return content.startswith(b"magnet:")
        if isinstance(content, str):
            return content.startswith("magnet:")
        return False

    @staticmethod
    def _is_http_url(content: Any) -> bool:
        return isinstance(content, str) and (content.startswith("http://") or content.startswith("https://"))

    def _build_add_options(self, download_dir: Path, cookie: str = "", pause: bool = False) -> Dict[str, Any]:
        options: Dict[str, Any] = {"dir": str(download_dir)}
        if pause:
            options["pause"] = "true"
        if cookie:
            options["header"] = [f"Cookie: {cookie}"]
        return options

    def _record_added_task(
        self,
        gid: str,
        downloader: Optional[str],
        category: Optional[str] = None,
        label: Optional[str] = None,
        episodes: Optional[set] = None,
        selected_episodes: Optional[List[int]] = None,
    ) -> None:
        tags = []
        if label:
            tags.extend([tag.strip() for tag in str(label).split(",")])
        self._update_task_meta(
            gid,
            downloader=self._resolve_downloader_name(downloader),
            category=category,
            tags=tags,
            episodes=sorted(list(episodes or [])),
            selected_episodes=selected_episodes or [],
            done=False,
        )

    def _select_episode_files(self, gid: str, episodes: set) -> Tuple[bool, str, List[int]]:
        task = self._tell_status(gid, ["gid", "dir", "files", "bittorrent"])
        files = self._to_file_entries(task)
        if not files:
            return False, "获取种子文件失败，无法选择集数", []

        selected_indices = []
        selected_episodes = []
        for file in files:
            meta = MetaInfo(Path(file.name).stem)
            episode_list = set(meta.episode_list or [])
            if episode_list and episode_list.issubset(episodes):
                selected_indices.append(str(file.index))
                selected_episodes.extend(list(episode_list))

        if not selected_indices:
            return False, "未匹配到需要下载的集数文件", []

        self._rpc_call("aria2.changeOption", [gid, {"select-file": ",".join(selected_indices)}])
        return True, f"已选择集数：{sorted(set(selected_episodes))}", sorted(set(selected_episodes))

    def download(
        self,
        content: Any,
        download_dir: Path,
        cookie: str,
        episodes: set = None,
        category: Optional[str] = None,
        label: Optional[str] = None,
        downloader: Optional[str] = None,
    ) -> Optional[Tuple[Optional[str], Optional[str], Optional[str], str]]:
        if not self._enabled or not self._is_target_downloader(downloader):
            return None
        downloader_name = self._resolve_downloader_name(downloader)
        try:
            select_episodes = bool(episodes) and not self._is_magnet(content) and not self._is_http_url(content)
            options = self._build_add_options(download_dir=download_dir, pause=select_episodes)
            gid: Optional[str] = None
            magnet_with_episodes = bool(episodes) and self._is_magnet(content)

            if isinstance(content, Path):
                if not content.exists():
                    return downloader_name, None, "Original", "种子文件不存在"
                b64 = base64.b64encode(content.read_bytes()).decode("utf-8")
                gid = self._rpc_call("aria2.addTorrent", [b64, [], options])
            elif isinstance(content, bytes):
                # 兼容磁力链 bytes
                if content.startswith(b"magnet:"):
                    gid = self._rpc_call("aria2.addUri", [[content.decode("utf-8")], options])
                else:
                    b64 = base64.b64encode(content).decode("utf-8")
                    gid = self._rpc_call("aria2.addTorrent", [b64, [], options])
            elif isinstance(content, str):
                if content.startswith("magnet:"):
                    gid = self._rpc_call("aria2.addUri", [[content], options])
                elif content.startswith("http://") or content.startswith("https://"):
                    http_options = self._build_add_options(download_dir=download_dir, cookie=cookie)
                    gid = self._rpc_call("aria2.addUri", [[content], http_options])
                else:
                    return downloader_name, None, "Original", "不支持的下载内容格式"
            else:
                return downloader_name, None, "Original", "不支持的下载内容类型"

            if not gid:
                return downloader_name, None, "Original", "添加下载失败"

            selected_episodes: List[int] = []
            message = "添加下载任务成功"
            if select_episodes:
                selected, select_message, selected_episodes = self._select_episode_files(gid, episodes)
                if not selected:
                    self._rpc_ignore_error("aria2.remove", [gid])
                    self._rpc_ignore_error("aria2.removeDownloadResult", [gid])
                    return downloader_name, None, "Original", select_message
                self._rpc_call("aria2.unpause", [gid])
                message = f"添加下载任务成功，{select_message}"
            elif magnet_with_episodes:
                message = "添加下载任务成功；磁力链无法预读取文件列表，已按全集下载"

            self._record_added_task(
                gid=gid,
                downloader=downloader_name,
                category=category,
                label=label,
                episodes=episodes,
                selected_episodes=selected_episodes,
            )
            return downloader_name, gid, "Original", message
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 添加下载失败：{self._last_error}")
            return downloader_name, None, "Original", f"添加下载失败：{self._last_error}"

    def list_torrents(
        self,
        status: TorrentStatus = None,
        hashs: Any = None,
        downloader: Optional[str] = None,
    ) -> Optional[List[Any]]:
        if not self._enabled or not self._is_target_downloader(downloader, default_only_when_empty=False):
            return None
        try:
            if hashs:
                gids = hashs if isinstance(hashs, list) else [hashs]
                results = []
                for gid in gids:
                    try:
                        task = self._tell_status(str(gid))
                    except Exception as err:
                        logger.debug(f"Aria2 查询任务 {gid} 失败：{err}")
                        continue
                    if task:
                        results.append(self._to_transfer_torrent(task, downloader))
                return results

            if status == TorrentStatus.DOWNLOADING:
                active = self._rpc_call("aria2.tellActive", [])
                waiting = self._rpc_call("aria2.tellWaiting", [0, 200])
                tasks = (active or []) + (waiting or [])
                ret = []
                for t in tasks:
                    ret.append(self._to_downloading_torrent(t, downloader))
                return ret

            if status == TorrentStatus.TRANSFER:
                stopped = self._rpc_call("aria2.tellStopped", [0, 200]) or []
                ret = []
                for t in stopped:
                    # 仅返回“可转移”的完成任务
                    if (t.get("status") or "") != "complete":
                        continue
                    if self._task_done(t):
                        continue
                    ret.append(self._to_transfer_torrent(t, downloader))
                return ret
            return None
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 查询任务失败：{self._last_error}")
            return None

    def transfer_completed(self, hashs: Any, downloader: Optional[str] = None) -> None:
        if not self._enabled or not self._is_target_downloader(downloader):
            return None
        for gid in self._as_list(hashs):
            meta = self._get_task_meta(str(gid))
            tags = self._unique(self._as_list(meta.get("tags")) + [self._done_tag])
            self._update_task_meta(str(gid), tags=tags, done=True)
        return None

    @staticmethod
    def _unlink_file(path: Path) -> bool:
        try:
            if path.exists() and (path.is_file() or path.is_symlink()):
                path.unlink()
                return True
        except Exception as err:
            logger.warn(f"删除文件失败 {path}: {err}")
        return False

    @staticmethod
    def _remove_empty_dirs(start: Path, stop: Path) -> None:
        current = start
        while current and current != stop and current.exists():
            try:
                current.rmdir()
            except Exception:
                break
            current = current.parent

    def _delete_task_files(self, task: Dict[str, Any]) -> None:
        entries = self._to_file_entries(task)
        task_path = self._task_path(task)
        cleanup_stop = task_path.parent
        parent_dirs = []

        for entry in entries:
            path = Path(entry.path)
            self._unlink_file(path)
            self._unlink_file(Path(f"{path}.aria2"))
            parent_dirs.append(path.parent)

        self._unlink_file(Path(f"{task_path}.aria2"))
        for parent in sorted(set(parent_dirs), key=lambda item: len(item.parts), reverse=True):
            self._remove_empty_dirs(parent, cleanup_stop)

    def _remove_task_from_aria2(self, gid: str, task: Dict[str, Any]) -> None:
        status = task.get("status") if task else ""
        if status in ("active", "waiting", "paused"):
            self._rpc_ignore_error("aria2.remove", [gid])
            self._rpc_ignore_error("aria2.removeDownloadResult", [gid])
            return
        if not self._rpc_ignore_error("aria2.removeDownloadResult", [gid]):
            self._rpc_ignore_error("aria2.remove", [gid])

    def remove_torrents(
        self,
        hashs: Any,
        delete_file: Optional[bool] = True,
        downloader: Optional[str] = None,
    ) -> Optional[bool]:
        if not self._enabled or not self._is_target_downloader(downloader):
            return None
        try:
            gids = hashs if isinstance(hashs, list) else [hashs]
            for gid in gids:
                gid = str(gid)
                try:
                    task = self._tell_status(gid)
                except Exception as err:
                    logger.debug(f"Aria2 删除前查询任务 {gid} 失败：{err}")
                    task = {}
                self._remove_task_from_aria2(gid, task)
                if delete_file and task:
                    self._delete_task_files(task)
                self._remove_task_meta(gid)
            return True
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 删除任务失败：{self._last_error}")
            return False

    def set_torrents_tag(self, hashs: Any, tags: list, downloader: Optional[str] = None) -> Optional[bool]:
        if not self._enabled or not self._is_target_downloader(downloader):
            return None
        for gid in self._as_list(hashs):
            current = self._get_task_meta(str(gid))
            merged = self._unique(self._as_list(current.get("tags")) + self._as_list(tags))
            self._update_task_meta(str(gid), tags=merged)
        return True

    def start_torrents(self, hashs: Any, downloader: Optional[str] = None) -> Optional[bool]:
        if not self._enabled or not self._is_target_downloader(downloader):
            return None
        try:
            gids = hashs if isinstance(hashs, list) else [hashs]
            for gid in gids:
                self._rpc_call("aria2.unpause", [gid])
            return True
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 恢复任务失败：{self._last_error}")
            return False

    def stop_torrents(self, hashs: Any, downloader: Optional[str] = None) -> Optional[bool]:
        if not self._enabled or not self._is_target_downloader(downloader):
            return None
        try:
            gids = hashs if isinstance(hashs, list) else [hashs]
            for gid in gids:
                self._rpc_call("aria2.pause", [gid])
            return True
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 暂停任务失败：{self._last_error}")
            return False

    def torrent_files(self, tid: str, downloader: Optional[str] = None) -> Optional[List[Aria2File]]:
        if not self._enabled or not self._is_target_downloader(downloader):
            return None
        try:
            status = self._tell_status(tid, ["gid", "dir", "files", "bittorrent"])
            return self._to_file_entries(status)
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 获取任务文件失败：{self._last_error}")
            return None

    def downloader_info(self, downloader: Optional[str] = None) -> Optional[List[DownloaderInfo]]:
        if not self._enabled or not self._is_target_downloader(downloader, default_only_when_empty=False):
            return None
        try:
            stat = self._rpc_call("aria2.getGlobalStat") or {}
            return [
                DownloaderInfo(
                    download_speed=self._to_int(stat.get("downloadSpeed")),
                    upload_speed=self._to_int(stat.get("uploadSpeed")),
                    download_size=0,
                    upload_size=0,
                )
            ]
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 获取下载器信息失败：{self._last_error}")
            return None

    def refresh_status(self) -> Dict[str, Any]:
        try:
            stat = self._rpc_call("aria2.getGlobalStat")
            waiting = self._rpc_call("aria2.tellWaiting", [0, 1000])
            stopped = self._rpc_call("aria2.tellStopped", [0, 1000])
            active_tasks = self._rpc_call("aria2.tellActive")
            stopped_error = 0
            stopped_complete = 0
            for t in stopped or []:
                if t.get("status") == "error":
                    stopped_error += 1
                elif t.get("status") == "complete":
                    stopped_complete += 1
            status = {
                "connection": "ok",
                "active": self._to_int(stat.get("numActive"), len(active_tasks) if isinstance(active_tasks, list) else 0),
                "waiting": self._to_int(stat.get("numWaiting"), len(waiting) if isinstance(waiting, list) else 0),
                "stopped": self._to_int(stat.get("numStopped"), len(stopped) if isinstance(stopped, list) else 0),
                "stopped_complete": stopped_complete,
                "stopped_error": stopped_error,
                "download_speed": self._to_int(stat.get("downloadSpeed", 0)),
                "upload_speed": self._to_int(stat.get("uploadSpeed", 0)),
            }
            self._last_status = status
            self._last_error = ""
            return status
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 状态刷新失败：{self._last_error}")
            status = {
                "connection": "error",
                "active": 0,
                "waiting": 0,
                "stopped": 0,
                "download_speed": 0,
                "upload_speed": 0,
            }
            self._last_status = status
            return status

    def pause_all(self) -> bool:
        try:
            self._rpc_call("aria2.pauseAll")
            return True
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 pauseAll 失败：{self._last_error}")
            return False

    def unpause_all(self) -> bool:
        try:
            self._rpc_call("aria2.unpauseAll")
            return True
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 unpauseAll 失败：{self._last_error}")
            return False

    def purge_done(self) -> bool:
        try:
            self._rpc_call("aria2.purgeDownloadResult")
            return True
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 purgeDownloadResult 失败：{self._last_error}")
            return False

    def _notify_action(self, action_name: str, ok: bool):
        if not self._notify:
            return
        self.post_message(
            mtype=NotificationType.Plugin,
            title="【Aria2 下载管理】",
            text=f"{action_name}{'成功' if ok else '失败'}",
        )

import json
import urllib.request
import base64
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

from app.core.event import eventmanager, Event
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType, TransferTorrent, DownloadingTorrent, DownloaderInfo
from app.schemas.types import EventType, TorrentStatus


class Aria2ManagerRehtt(_PluginBase):
    # 插件名称
    plugin_name = "Aria2 下载管理"
    # 插件描述
    plugin_desc = "在下载管理中控制和监控 Aria2 任务状态。"
    # 插件图标（使用在线图标，避免仓库内额外资源依赖）
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/download.png"
    # 插件版本
    plugin_version = "1.0"
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

    def _rpc_call(self, method: str, params: Optional[List[Any]] = None) -> Dict[str, Any]:
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

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    def _target_type(self) -> str:
        return (self._downloader_type or "aria2managerrehtt").lower()

    def _is_target_downloader(self, downloader: Optional[str]) -> bool:
        helper = DownloaderHelper()
        service = helper.get_service(name=downloader) if downloader else None
        if service and service.type:
            return str(service.type).lower() == self._target_type()
        if not downloader:
            configs = helper.get_configs()
            for conf in configs.values():
                if conf.default:
                    return str(conf.type).lower() == self._target_type()
        return False

    @staticmethod
    def _task_path(task: Dict[str, Any]) -> Path:
        base_dir = task.get("dir") or ""
        files = task.get("files") or []
        if files and isinstance(files, list):
            first = files[0] or {}
            fpath = first.get("path")
            if fpath:
                return Path(fpath)
        if base_dir:
            return Path(base_dir)
        return Path("/")

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
        try:
            options: Dict[str, Any] = {"dir": str(download_dir)}
            if category:
                options["dir"] = str(download_dir / category)
            if label:
                options["gid"] = None

            gid: Optional[str] = None
            if isinstance(content, Path):
                if not content.exists():
                    return downloader, None, "Original", "种子文件不存在"
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
                if content.startswith("magnet:") or content.startswith("http://") or content.startswith("https://"):
                    gid = self._rpc_call("aria2.addUri", [[content], options])
                else:
                    return downloader, None, "Original", "不支持的下载内容格式"
            else:
                return downloader, None, "Original", "不支持的下载内容类型"

            if not gid:
                return downloader, None, "Original", "添加下载失败"
            return downloader, gid, "Original", "添加下载任务成功"
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 添加下载失败：{self._last_error}")
            return downloader, None, "Original", f"添加下载失败：{self._last_error}"

    def list_torrents(
        self,
        status: TorrentStatus = None,
        hashs: Any = None,
        downloader: Optional[str] = None,
    ) -> Optional[List[Any]]:
        if not self._enabled or not self._is_target_downloader(downloader):
            return None
        try:
            if hashs:
                gids = hashs if isinstance(hashs, list) else [hashs]
                results = []
                for gid in gids:
                    t = self._rpc_call("aria2.tellStatus", [gid])
                    if t:
                        results.append(
                            TransferTorrent(
                                downloader=downloader,
                                title=t.get("bittorrent", {}).get("info", {}).get("name") or gid,
                                path=self._task_path(t),
                                hash=gid,
                                size=self._to_int(t.get("totalLength")),
                                progress=(self._to_int(t.get("completedLength")) / max(self._to_int(t.get("totalLength"), 1), 1)) * 100,
                                state="paused" if t.get("status") == "paused" else "downloading",
                            )
                        )
                return results

            if status == TorrentStatus.DOWNLOADING:
                active = self._rpc_call("aria2.tellActive", [])
                waiting = self._rpc_call("aria2.tellWaiting", [0, 200])
                tasks = (active or []) + (waiting or [])
                ret = []
                for t in tasks:
                    total = self._to_int(t.get("totalLength"), 1)
                    completed = self._to_int(t.get("completedLength"), 0)
                    ret.append(
                        DownloadingTorrent(
                            downloader=downloader,
                            hash=t.get("gid"),
                            title=t.get("bittorrent", {}).get("info", {}).get("name") or t.get("gid"),
                            name=t.get("bittorrent", {}).get("info", {}).get("name") or t.get("gid"),
                            year=None,
                            season_episode=None,
                            progress=(completed / max(total, 1)) * 100,
                            size=total,
                            state="paused" if t.get("status") == "paused" else "downloading",
                            dlspeed=str(self._to_int(t.get("downloadSpeed"))),
                            upspeed=str(self._to_int(t.get("uploadSpeed"))),
                            tags=None,
                            left_time="",
                        )
                    )
                return ret

            if status == TorrentStatus.TRANSFER:
                stopped = self._rpc_call("aria2.tellStopped", [0, 200]) or []
                ret = []
                for t in stopped:
                    if t.get("status") != "complete":
                        continue
                    gid = t.get("gid")
                    ret.append(
                        TransferTorrent(
                            downloader=downloader,
                            title=t.get("bittorrent", {}).get("info", {}).get("name") or gid,
                            path=self._task_path(t),
                            hash=gid,
                            tags="",
                            progress=100,
                            state="downloading",
                        )
                    )
                return ret
            return None
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 查询任务失败：{self._last_error}")
            return None

    def transfer_completed(self, hashs: str, downloader: Optional[str] = None) -> None:
        # aria2 默认无标签体系，转移完成无需动作
        return None

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
                method = "aria2.removeDownloadResult" if delete_file else "aria2.remove"
                self._rpc_call(method, [gid])
            return True
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 删除任务失败：{self._last_error}")
            return False

    def set_torrents_tag(self, hashs: Any, tags: list, downloader: Optional[str] = None) -> Optional[bool]:
        if not self._enabled or not self._is_target_downloader(downloader):
            return None
        # aria2 无原生标签体系，返回 True 以兼容链路调用
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

    def torrent_files(self, tid: str, downloader: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        if not self._enabled or not self._is_target_downloader(downloader):
            return None
        try:
            status = self._rpc_call("aria2.tellStatus", [tid, ["files"]]) or {}
            return status.get("files") or []
        except Exception as err:
            self._last_error = str(err)
            logger.error(f"Aria2 获取任务文件失败：{self._last_error}")
            return None

    def downloader_info(self, downloader: Optional[str] = None) -> Optional[List[DownloaderInfo]]:
        if not self._enabled or not self._is_target_downloader(downloader):
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
            active = self._rpc_call("aria2.getGlobalStat")
            waiting = self._rpc_call("aria2.tellWaiting", [0, 1000])
            stopped = self._rpc_call("aria2.tellStopped", [0, 1000])
            active_tasks = self._rpc_call("aria2.tellActive")
            status = {
                "connection": "ok",
                "active": len(active_tasks) if isinstance(active_tasks, list) else 0,
                "waiting": len(waiting) if isinstance(waiting, list) else 0,
                "stopped": len(stopped) if isinstance(stopped, list) else 0,
                "download_speed": int(active.get("downloadSpeed", 0)),
                "upload_speed": int(active.get("uploadSpeed", 0)),
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

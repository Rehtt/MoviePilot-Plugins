from typing import Any, Dict, List, Optional, Tuple

from app.core.event import Event, eventmanager
from app.helper.directory import DirectoryHelper
from app.helper.downloader import DownloaderHelper
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType

from .aria2_client import Aria2Client
from .config import (
    DownloaderBinding,
    PluginConfig,
    matching_downloader_names,
    resolve_downloader_binding,
)
from .downloader import Aria2Downloader
from .pathing import PathMapper
from .task_store import TaskStore, moviepilot_atomic_migration
from .ui import build_form, build_page


class Aria2ManagerRehtt(_PluginBase):
    plugin_name = "Aria2 下载管理"
    plugin_desc = "将单个 Aria2 RPC 实例接入 MoviePilot 下载、整理与任务管理。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/download.png"
    plugin_version = "1.3.0"
    plugin_author = "Rehtt"
    author_url = "https://github.com/Rehtt"
    plugin_config_prefix = "aria2managerrehtt_"
    plugin_order = 20
    auth_level = 1

    _task_data_key = "tasks"

    def __init__(self):
        super().__init__()
        self._config = PluginConfig()
        self._binding: Optional[DownloaderBinding] = None
        self._binding_error = ""
        self._adapter: Optional[Aria2Downloader] = None
        self._last_status: Dict[str, Any] = {"connection": "disabled"}
        self._last_error = ""
        self._connection_ok: Optional[bool] = None

    def init_plugin(self, config: dict = None):
        self._config = PluginConfig.parse(config)
        self._binding = None
        self._adapter = None
        self._binding_error = ""
        self._last_error = "；".join(self._config.errors)
        self._connection_ok = None

        downloader_configs = DownloaderHelper().get_configs()
        self._binding, self._binding_error = resolve_downloader_binding(
            self._config, downloader_configs
        )
        if self._binding_error:
            self._last_error = "；".join(
                value
                for value in [self._last_error, self._binding_error]
                if value
            )

        if self._config.valid and self._binding:
            client = Aria2Client(
                url=self._config.rpc_url,
                secret=self._config.rpc_secret,
                timeout=self._config.timeout,
            )
            plugin_id = self.__class__.__name__
            store = TaskStore(
                load_callback=lambda: self.get_data(self._task_data_key),
                save_callback=lambda payload: self.save_data(
                    self._task_data_key, payload
                ),
                default_downloader=self._binding.name,
                migration_callback=lambda payload, old_hash, new_hash, downloader: (
                    moviepilot_atomic_migration(
                        plugin_id=plugin_id,
                        data_key=self._task_data_key,
                        payload=payload,
                        old_hash=old_hash,
                        new_hash=new_hash,
                        downloader=downloader,
                    )
                ),
            )
            self._adapter = Aria2Downloader(
                plugin_id=plugin_id,
                config=self._config,
                binding=self._binding,
                client=client,
                store=store,
                path_mapper=PathMapper(self._binding.path_mapping),
                allowed_roots_provider=self._local_download_roots,
            )

        if not self._config.enabled:
            self._last_status = {"connection": "disabled"}
        elif self._adapter:
            self._last_status = self.refresh_status()
        else:
            self._last_status = {"connection": "error"}

    @staticmethod
    def _local_download_roots() -> List[str]:
        return [
            str(directory.download_path)
            for directory in DirectoryHelper().get_local_download_dirs()
            if directory.download_path
        ]

    @property
    def _ready(self) -> bool:
        return bool(self._config.enabled and self._adapter and self._adapter.ready)

    def get_state(self) -> bool:
        return self._config.enabled

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
                "desc": "清理已整理/错误的 Aria2 结果",
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
                "auth": "apikey",
                "summary": "获取 Aria2 状态",
                "description": "获取绑定、就绪状态与 Aria2 任务速度概览",
            },
            {
                "path": "/action",
                "endpoint": self.api_action,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "执行 Aria2 控制动作",
                "description": "支持 pause_all / unpause_all / purge_done",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._ready and self._config.monitor_interval > 0:
            return [
                {
                    "id": f"{self.__class__.__name__}.monitor",
                    "name": "Aria2 状态监控与任务关系同步",
                    "trigger": "interval",
                    "func": self.monitor_service,
                    "kwargs": {"seconds": self._config.monitor_interval},
                }
            ]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        downloader_configs = DownloaderHelper().get_configs()
        downloader_names = matching_downloader_names(
            self._config, downloader_configs
        )
        return build_form(
            config=self._config,
            downloader_names=downloader_names,
            selected_downloader=self._binding.name if self._binding else "",
            error=self._last_error,
        )

    def get_page(self) -> List[dict]:
        return build_page(
            status=self._last_status,
            bound_downloader=self._binding.name if self._binding else None,
            error=self._last_error,
        )

    def stop_service(self):
        self._connection_ok = None

    def get_module(self) -> Dict[str, Any]:
        if not self._ready:
            return {}
        return {
            "download": self._adapter.download,
            "list_torrents": self._adapter.list_torrents,
            "transfer_completed": self._adapter.transfer_completed,
            "remove_torrents": self._adapter.remove_torrents,
            "set_torrents_tag": self._adapter.set_torrents_tag,
            "update_torrent": self._adapter.update_torrent,
            "get_torrent_trackers": self._adapter.get_torrent_trackers,
            "start_torrents": self._adapter.start_torrents,
            "stop_torrents": self._adapter.stop_torrents,
            "torrent_files": self._adapter.torrent_files,
            "downloader_info": self._adapter.downloader_info,
        }

    def _not_ready_message(self) -> str:
        if not self._config.enabled:
            return "插件未启用"
        return self._last_error or "插件配置未就绪"

    def api_status(self):
        if not self._ready:
            return {
                "ok": False,
                "enabled": self._config.enabled,
                "ready": False,
                "bound_downloader": self._binding.name if self._binding else None,
                "message": self._not_ready_message(),
                **self._last_status,
            }
        status = self.refresh_status()
        return {
            "ok": status.get("connection") == "ok",
            "enabled": True,
            "ready": True,
            "bound_downloader": self._binding.name,
            **status,
        }

    def api_action(self, action: Optional[str] = None):
        if not self._ready:
            return {"ok": False, "message": self._not_ready_message()}
        actions = {
            "pause_all": self.pause_all,
            "unpause_all": self.unpause_all,
            "purge_done": self.purge_done,
        }
        handler = actions.get(action)
        if not handler:
            return {"ok": False, "message": "unsupported action"}
        ok = handler()
        return {
            "ok": ok,
            "message": f"{action}{'成功' if ok else '失败'}",
        }

    @eventmanager.register(EventType.PluginAction)
    def handle_plugin_action(self, event: Event):
        if not self._config.enabled or not event:
            return
        action = (event.event_data or {}).get("action")
        if action == "aria2_status":
            if not self._ready:
                self._post_action_result("查看状态", False, self._not_ready_message())
                return
            status = self.refresh_status()
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【Aria2 下载管理】",
                text=(
                    f"绑定：{self._binding.name}\n"
                    f"连接：{status.get('connection', 'unknown')}\n"
                    f"活跃：{status.get('active', 0)}\n"
                    f"等待：{status.get('waiting', 0)}\n"
                    f"停止：{status.get('stopped', 0)}\n"
                    f"下载：{status.get('download_speed', 0)} B/s\n"
                    f"上传：{status.get('upload_speed', 0)} B/s"
                ),
            )
            return
        handlers = {
            "aria2_pause_all": ("暂停全部任务", self.pause_all),
            "aria2_unpause_all": ("恢复全部任务", self.unpause_all),
            "aria2_purge_done": ("清理已整理/错误任务", self.purge_done),
        }
        item = handlers.get(action)
        if not item:
            return
        name, handler = item
        if not self._ready:
            self._post_action_result(name, False, self._not_ready_message())
            return
        self._post_action_result(name, handler())

    def _post_action_result(
        self, action_name: str, ok: bool, detail: str = ""
    ) -> None:
        text = f"{action_name}{'成功' if ok else '失败'}"
        if detail:
            text = f"{text}：{detail}"
        self.post_message(
            mtype=NotificationType.Plugin,
            title="【Aria2 下载管理】",
            text=text,
        )

    def monitor_service(self, event: Event = None):
        if not self._ready:
            return
        previous = self._connection_ok
        status = self.refresh_status()
        current = status.get("connection") == "ok"
        if self._config.notify:
            if not current and previous is not False:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【Aria2 下载管理】",
                    text=f"连接异常：{self._last_error or 'Aria2不可达'}",
                )
            elif current and previous is False:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【Aria2 下载管理】",
                    text="Aria2连接已恢复",
                )
        self._connection_ok = current

    def refresh_status(self) -> Dict[str, Any]:
        if not self._config.enabled:
            self._last_status = {"connection": "disabled"}
            return self._last_status
        if not self._adapter:
            self._last_status = {"connection": "error"}
            return self._last_status
        self._last_status = self._adapter.refresh_status()
        self._last_error = self._adapter.last_error or self._binding_error
        return self._last_status

    def pause_all(self) -> bool:
        return bool(self._ready and self._adapter.pause_all())

    def unpause_all(self) -> bool:
        return bool(self._ready and self._adapter.unpause_all())

    def purge_done(self) -> bool:
        return bool(self._ready and self._adapter.purge_done())

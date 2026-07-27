import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse


DEFAULT_RPC_URL = "http://127.0.0.1:6800/jsonrpc"
DEFAULT_DOWNLOADER_TYPE = "aria2managerrehtt"


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "1", "yes", "on"}


def _bounded_int(
    value: Any,
    default: int,
    minimum: int,
    maximum: int,
    label: str,
    errors: List[str],
    allow_zero: bool = False,
) -> int:
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        errors.append(f"{label}必须是整数")
        return default
    if allow_zero and parsed == 0:
        return 0
    if parsed < minimum or parsed > maximum:
        errors.append(f"{label}必须在 {minimum} 到 {maximum} 之间")
        return default
    return parsed


@dataclass(frozen=True)
class PluginConfig:
    enabled: bool = False
    notify: bool = False
    rpc_url: str = DEFAULT_RPC_URL
    rpc_secret: str = ""
    timeout: int = 8
    monitor_interval: int = 60
    downloader_type: str = DEFAULT_DOWNLOADER_TYPE
    downloader_name: str = ""
    errors: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def valid(self) -> bool:
        return not self.errors

    @classmethod
    def parse(cls, raw: Optional[Dict[str, Any]]) -> "PluginConfig":
        raw = raw or {}
        errors: List[str] = []
        rpc_url = str(raw.get("rpc_url") or DEFAULT_RPC_URL).strip()
        parsed_url = urlparse(rpc_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            errors.append("Aria2 RPC地址必须是有效的HTTP或HTTPS URL")

        downloader_type = str(
            raw.get("downloader_type") or DEFAULT_DOWNLOADER_TYPE
        ).strip().lower()
        if not re.fullmatch(r"[a-z0-9_-]+", downloader_type):
            errors.append("自定义下载器类型只能包含小写字母、数字、下划线和连字符")

        return cls(
            enabled=_as_bool(raw.get("enabled")),
            notify=_as_bool(raw.get("notify")),
            rpc_url=rpc_url,
            rpc_secret=str(raw.get("rpc_secret") or "").strip(),
            timeout=_bounded_int(
                raw.get("timeout"), 8, 1, 120, "HTTP超时", errors
            ),
            monitor_interval=_bounded_int(
                raw.get("monitor_interval"),
                60,
                10,
                86400,
                "监控间隔",
                errors,
                allow_zero=True,
            ),
            downloader_type=downloader_type,
            downloader_name=str(raw.get("downloader_name") or "").strip(),
            errors=tuple(errors),
        )

    def defaults(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "notify": self.notify,
            "rpc_url": self.rpc_url,
            "rpc_secret": self.rpc_secret,
            "timeout": self.timeout,
            "monitor_interval": self.monitor_interval,
            "downloader_type": self.downloader_type,
            "downloader_name": self.downloader_name,
        }


@dataclass(frozen=True)
class DownloaderBinding:
    name: str
    type: str
    default: bool
    path_mapping: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)


def matching_downloader_names(
    config: PluginConfig, downloader_configs: Dict[str, Any]
) -> List[str]:
    return sorted(
        name
        for name, downloader_config in (downloader_configs or {}).items()
        if str(getattr(downloader_config, "type", "") or "").strip().lower()
        == config.downloader_type
    )


def resolve_downloader_binding(
    config: PluginConfig, downloader_configs: Dict[str, Any]
) -> Tuple[Optional[DownloaderBinding], Optional[str]]:
    configs = downloader_configs or {}
    matches = matching_downloader_names(config, configs)
    selected_name = config.downloader_name
    if selected_name:
        selected = configs.get(selected_name)
        if not selected:
            return None, f"未找到已启用的下载器配置：{selected_name}"
        selected_type = str(getattr(selected, "type", "") or "").strip().lower()
        if selected_type != config.downloader_type:
            return None, (
                f"下载器 {selected_name} 的类型是 {selected_type or '空'}，"
                f"与 {config.downloader_type} 不一致"
            )
    elif len(matches) == 1:
        selected_name = matches[0]
        selected = configs[selected_name]
    elif not matches:
        return None, f"没有已启用的 {config.downloader_type} 自定义下载器配置"
    else:
        return None, "检测到多个同类型下载器，请明确选择要绑定的下载器名称"

    mappings: List[Tuple[str, str]] = []
    for mapping in getattr(selected, "path_mapping", None) or []:
        if isinstance(mapping, (list, tuple)) and len(mapping) == 2:
            source, target = str(mapping[0]).strip(), str(mapping[1]).strip()
            if source and target:
                mappings.append((source, target))
    return (
        DownloaderBinding(
            name=selected_name,
            type=config.downloader_type,
            default=bool(getattr(selected, "default", False)),
            path_mapping=tuple(mappings),
        ),
        None,
    )


from typing import Any, Dict, List, Optional, Tuple

from .config import PluginConfig


def build_form(
    config: PluginConfig,
    downloader_names: List[str],
    selected_downloader: str = "",
    error: str = "",
) -> Tuple[List[dict], Dict[str, Any]]:
    downloader_items = [
        {"title": name, "value": name} for name in downloader_names
    ]
    alerts: List[dict] = []
    if error:
        alerts.append(
            {
                "component": "VAlert",
                "props": {
                    "type": "error",
                    "variant": "tonal",
                    "text": error,
                },
            }
        )
    alerts.append(
        {
            "component": "VAlert",
            "props": {
                "type": "info",
                "variant": "tonal",
                "text": (
                    "请先在系统下载器设置中新增并启用自定义下载器，类型需与下方标识一致。"
                    "Docker部署请同时配置下载器路径映射；删除文件只允许发生在MoviePilot本地下载根目录内。"
                ),
            },
        }
    )
    form = [
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
                                    "props": {
                                        "model": "enabled",
                                        "label": "启用插件",
                                    },
                                }
                            ],
                        },
                        {
                            "component": "VCol",
                            "props": {"cols": 12, "md": 6},
                            "content": [
                                {
                                    "component": "VSwitch",
                                    "props": {
                                        "model": "notify",
                                        "label": "连接异常通知",
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
                                        "model": "rpc_url",
                                        "label": "Aria2 RPC地址",
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
                                        "type": "number",
                                        "min": 1,
                                        "max": 120,
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
                            "props": {"cols": 12, "md": 6},
                            "content": [
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "rpc_secret",
                                        "label": "RPC Secret（可选）",
                                        "type": "password",
                                    },
                                }
                            ],
                        },
                        {
                            "component": "VCol",
                            "props": {"cols": 12, "md": 3},
                            "content": [
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "downloader_type",
                                        "label": "自定义下载器类型",
                                    },
                                }
                            ],
                        },
                        {
                            "component": "VCol",
                            "props": {"cols": 12, "md": 3},
                            "content": [
                                {
                                    "component": "VTextField",
                                    "props": {
                                        "model": "monitor_interval",
                                        "label": "监控间隔（秒，0关闭）",
                                        "type": "number",
                                        "min": 0,
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
                                    "component": "VSelect",
                                    "props": {
                                        "model": "downloader_name",
                                        "label": "绑定的自定义下载器",
                                        "items": downloader_items,
                                        "clearable": True,
                                        "hint": "只有一个同类型配置时可留空自动绑定",
                                        "persistent-hint": True,
                                    },
                                }
                            ],
                        }
                    ],
                },
                {
                    "component": "VRow",
                    "content": [
                        {
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": alerts,
                        }
                    ],
                },
            ],
        }
    ]
    defaults = config.defaults()
    defaults["downloader_name"] = selected_downloader or config.downloader_name
    return form, defaults


def build_page(
    status: Dict[str, Any],
    bound_downloader: Optional[str],
    error: str = "",
) -> List[dict]:
    connection = status.get("connection", "unknown")
    alert_type = {
        "ok": "success",
        "disabled": "info",
        "error": "error",
    }.get(connection, "warning")
    lines = [
        f"绑定下载器：{bound_downloader or '未绑定'}",
        f"连接状态：{connection}",
        f"活跃：{status.get('active', 0)}",
        f"等待：{status.get('waiting', 0)}",
        f"停止：{status.get('stopped', 0)}",
        f"下载速度：{status.get('download_speed', 0)} B/s",
        f"上传速度：{status.get('upload_speed', 0)} B/s",
    ]
    if error:
        lines.append(f"最近错误：{error}")
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
                                "type": alert_type,
                                "variant": "tonal",
                                "title": "Aria2下载管理状态",
                                "text": "\n".join(lines),
                            },
                        }
                    ],
                }
            ],
        }
    ]

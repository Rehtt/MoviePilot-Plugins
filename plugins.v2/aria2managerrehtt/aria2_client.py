import itertools
import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Tuple


class Aria2RpcError(RuntimeError):
    def __init__(
        self,
        method: str,
        message: str,
        code: Optional[int] = None,
        data: Any = None,
    ):
        self.method = method
        self.code = code
        self.data = data
        super().__init__(f"{method}: {message}")

    @property
    def not_found(self) -> bool:
        text = str(self).lower()
        return (
            "not found" in text
            or "cannot find" in text
            or "is not found" in text
            or (self.code == 1 and "gid" in text)
        )


class Aria2Client:
    DEFAULT_STATUS_FIELDS = [
        "gid",
        "status",
        "totalLength",
        "completedLength",
        "downloadSpeed",
        "uploadSpeed",
        "dir",
        "files",
        "bittorrent",
        "infoHash",
        "seeder",
        "followedBy",
        "following",
        "errorCode",
        "errorMessage",
    ]

    def __init__(self, url: str, secret: str = "", timeout: int = 8):
        self.url = url
        self.secret = secret or ""
        self.timeout = timeout
        self._request_ids = itertools.count(1)

    def call(self, method: str, params: Optional[List[Any]] = None) -> Any:
        call_params: List[Any] = []
        if self.secret:
            call_params.append(f"token:{self.secret}")
        call_params.extend(params or [])
        payload = {
            "jsonrpc": "2.0",
            "id": f"moviepilot-aria2-{next(self._request_ids)}",
            "method": method,
            "params": call_params,
        }
        request = urllib.request.Request(
            self.url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                parsed = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as err:
            raise Aria2RpcError(method, str(err)) from err
        if not isinstance(parsed, dict):
            raise Aria2RpcError(method, "RPC响应不是JSON对象")
        error = parsed.get("error")
        if error:
            raise Aria2RpcError(
                method=method,
                message=str(error.get("message") or error),
                code=error.get("code"),
                data=error.get("data"),
            )
        return parsed.get("result")

    def try_call(self, method: str, params: Optional[List[Any]] = None) -> bool:
        try:
            self.call(method, params)
            return True
        except Aria2RpcError:
            return False

    def tell_status(
        self, gid: str, fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        return self.call(
            "aria2.tellStatus", [str(gid), fields or self.DEFAULT_STATUS_FIELDS]
        ) or {}

    def tell_status_optional(
        self, gid: str, fields: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        try:
            return self.tell_status(gid, fields)
        except Aria2RpcError as err:
            if err.not_found:
                return None
            raise

    def tell_active(self) -> List[Dict[str, Any]]:
        return self.call("aria2.tellActive", [self.DEFAULT_STATUS_FIELDS]) or []

    def _paged(
        self,
        method: str,
        page_size: int = 200,
        max_tasks: int = 10000,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        results: List[Dict[str, Any]] = []
        offset = 0
        truncated = False
        while len(results) < max_tasks:
            remaining = max_tasks - len(results)
            size = min(page_size, remaining)
            page = self.call(method, [offset, size, self.DEFAULT_STATUS_FIELDS]) or []
            if not isinstance(page, list):
                break
            results.extend(page)
            if len(page) < size:
                break
            offset += len(page)
        else:
            truncated = True
        return results, truncated

    def tell_waiting(
        self, page_size: int = 200, max_tasks: int = 10000
    ) -> Tuple[List[Dict[str, Any]], bool]:
        return self._paged("aria2.tellWaiting", page_size, max_tasks)

    def tell_stopped(
        self, page_size: int = 200, max_tasks: int = 10000
    ) -> Tuple[List[Dict[str, Any]], bool]:
        return self._paged("aria2.tellStopped", page_size, max_tasks)

    def all_tasks(self) -> Tuple[List[Dict[str, Any]], bool]:
        active = self.tell_active()
        waiting, waiting_truncated = self.tell_waiting()
        stopped, stopped_truncated = self.tell_stopped()
        seen = set()
        results: List[Dict[str, Any]] = []
        for task in [*active, *waiting, *stopped]:
            gid = str((task or {}).get("gid") or "")
            if not gid or gid in seen:
                continue
            seen.add(gid)
            results.append(task)
        return results, waiting_truncated or stopped_truncated

    def add_torrent(
        self, torrent_base64: str, options: Dict[str, Any]
    ) -> str:
        return str(self.call("aria2.addTorrent", [torrent_base64, [], options]) or "")

    def add_uri(self, uri: str, options: Dict[str, Any]) -> str:
        return str(self.call("aria2.addUri", [[uri], options]) or "")

    def change_option(self, gid: str, options: Dict[str, Any]) -> bool:
        return self.call("aria2.changeOption", [gid, options]) == "OK"

    def get_global_stat(self) -> Dict[str, Any]:
        return self.call("aria2.getGlobalStat") or {}

    def pause(self, gid: str) -> bool:
        return bool(self.call("aria2.pause", [gid]))

    def unpause(self, gid: str) -> bool:
        return bool(self.call("aria2.unpause", [gid]))

    def pause_all(self) -> bool:
        return self.call("aria2.pauseAll") == "OK"

    def unpause_all(self) -> bool:
        return self.call("aria2.unpauseAll") == "OK"

    def remove_and_purge(self, gid: str, status: Optional[str] = None) -> bool:
        current = self.tell_status_optional(gid)
        if current is None:
            return True
        current_status = status or str(current.get("status") or "")
        if current_status in {"active", "waiting", "paused"}:
            self.call("aria2.remove", [gid])
            for _ in range(20):
                time.sleep(0.1)
                current = self.tell_status_optional(gid)
                if current is None or current.get("status") in {
                    "removed",
                    "complete",
                    "error",
                }:
                    break
        current = self.tell_status_optional(gid)
        if current is not None:
            self.call("aria2.removeDownloadResult", [gid])
        return self.tell_status_optional(gid) is None

    def remove_many(self, gids: Iterable[str]) -> bool:
        ok = True
        for gid in gids:
            try:
                ok = self.remove_and_purge(str(gid)) and ok
            except Aria2RpcError:
                ok = False
        return ok

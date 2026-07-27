import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from _aria2manager_loader import load_module


client_module = load_module("aria2_client")
pathing = load_module("pathing")


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class Aria2ClientTests(unittest.TestCase):
    def test_rpc_secret_is_first_parameter(self):
        client = client_module.Aria2Client(
            "http://127.0.0.1:6800/jsonrpc",
            secret="secret",
            timeout=4,
        )
        with patch.object(
            client_module.urllib.request,
            "urlopen",
            return_value=_Response({"jsonrpc": "2.0", "result": "OK"}),
        ) as urlopen:
            self.assertEqual(client.call("aria2.pause", ["gid"]), "OK")

        request = urlopen.call_args.args[0]
        payload = json.loads(request.data.decode("utf-8"))
        self.assertEqual(payload["params"], ["token:secret", "gid"])
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 4)

    def test_waiting_tasks_are_fully_paginated(self):
        client = client_module.Aria2Client("http://localhost/jsonrpc")
        tasks = [{"gid": f"{index:016x}"} for index in range(450)]

        def fake_call(method, params):
            self.assertEqual(method, "aria2.tellWaiting")
            offset, size, _ = params
            return tasks[offset:offset + size]

        client.call = fake_call
        result, truncated = client.tell_waiting(page_size=200, max_tasks=1000)
        self.assertEqual(len(result), 450)
        self.assertFalse(truncated)

    def test_optional_status_only_swallows_not_found(self):
        client = client_module.Aria2Client("http://localhost/jsonrpc")
        client.tell_status = lambda gid, fields=None: (_ for _ in ()).throw(
            client_module.Aria2RpcError(
                "aria2.tellStatus", "GID was not found", code=1
            )
        )
        self.assertIsNone(client.tell_status_optional("missing"))


class PathingTests(unittest.TestCase):
    def test_mapping_matches_complete_path_segments(self):
        mapper = pathing.PathMapper((("/downloads", "/data"),))
        self.assertEqual(
            mapper.to_downloader("/downloads/show/a.mkv"),
            "/data/show/a.mkv",
        )
        self.assertEqual(
            mapper.to_moviepilot("/data/show/a.mkv"),
            "/downloads/show/a.mkv",
        )
        self.assertEqual(
            mapper.to_downloader("/downloads-old/a.mkv"),
            "/downloads-old/a.mkv",
        )
        self.assertEqual(mapper.to_moviepilot(""), "")

    def test_deletion_is_restricted_to_files_under_allowed_roots(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root = base / "downloads"
            nested = root / "show"
            nested.mkdir(parents=True)
            media = nested / "episode.mkv"
            media.write_text("video", encoding="utf-8")
            outside = base / "outside.mkv"
            outside.write_text("keep", encoding="utf-8")

            deleter = pathing.SafeFileDeleter([root])
            rejected = deleter.prepare([media, outside])
            self.assertFalse(rejected.valid)
            self.assertTrue(media.exists())

            prepared = deleter.prepare([media])
            result = deleter.execute(prepared)
            self.assertTrue(result.success)
            self.assertFalse(media.exists())
            self.assertFalse(nested.exists())
            self.assertTrue(root.exists())
            self.assertTrue(outside.exists())

    def test_symlink_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root = base / "downloads"
            root.mkdir()
            outside = base / "outside.mkv"
            outside.write_text("keep", encoding="utf-8")
            link = root / "episode.mkv"
            link.symlink_to(outside)

            prepared = pathing.SafeFileDeleter([root]).prepare([link])
            self.assertFalse(prepared.valid)
            self.assertTrue(link.exists())
            self.assertTrue(outside.exists())


if __name__ == "__main__":
    unittest.main()


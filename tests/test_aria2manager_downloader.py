import inspect
import re
import sys
import types
import unittest
from enum import Enum
from types import SimpleNamespace

from _aria2manager_loader import load_module


def _install_moviepilot_stubs():
    class Model:
        def __init__(self, **values):
            self.__dict__.update(values)

    class TorrentStatus(Enum):
        TRANSFER = "可转移"
        DOWNLOADING = "下载中"

    class TorrentQueryStatus(Enum):
        ALL = "all"
        TRANSFER = "transfer"
        DOWNLOADING = "downloading"
        COMPLETED = "completed"
        PAUSED = "paused"

    class DownloadTaskState(Enum):
        DOWNLOADING = "downloading"
        PAUSED = "paused"
        COMPLETED = "completed"

    class MetaInfo:
        def __init__(self, title):
            self.name = title
            self.year = None
            self.season_episode = None
            match = re.search(r"[Ee](\d+)", title or "")
            self.episode_list = [int(match.group(1))] if match else []

    class Logger:
        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    modules = {
        "app": types.ModuleType("app"),
        "app.core": types.ModuleType("app.core"),
        "app.core.cache": types.ModuleType("app.core.cache"),
        "app.core.config": types.ModuleType("app.core.config"),
        "app.core.metainfo": types.ModuleType("app.core.metainfo"),
        "app.log": types.ModuleType("app.log"),
        "app.schemas": types.ModuleType("app.schemas"),
        "app.schemas.types": types.ModuleType("app.schemas.types"),
        "app.utils": types.ModuleType("app.utils"),
        "app.utils.string": types.ModuleType("app.utils.string"),
    }
    modules["app.core.cache"].FileCache = type(
        "FileCache", (), {"get": lambda self, *args, **kwargs: None}
    )
    modules["app.core.config"].settings = SimpleNamespace(
        TORRENT_TAG="MOVIEPILOT"
    )
    modules["app.core.metainfo"].MetaInfo = MetaInfo
    modules["app.log"].logger = Logger()
    modules["app.schemas"].DownloaderInfo = Model
    modules["app.schemas"].DownloaderTorrent = Model
    modules["app.schemas"].DownloadingTorrent = Model
    modules["app.schemas"].TransferTorrent = Model
    modules["app.schemas.types"].TorrentStatus = TorrentStatus
    modules["app.schemas.types"].TorrentQueryStatus = TorrentQueryStatus
    modules["app.schemas.types"].DownloadTaskState = DownloadTaskState
    modules["app.utils.string"].StringUtils = type(
        "StringUtils",
        (),
        {"str_filesize": staticmethod(lambda value: str(value or 0))},
    )
    sys.modules.update(modules)


_install_moviepilot_stubs()
config_module = load_module("config")
models = load_module("models")
pathing = load_module("pathing")
store_module = load_module("task_store")
downloader_module = load_module("downloader")


class FakeClient:
    def __init__(self, tasks):
        self.tasks = {task["gid"]: task for task in tasks}
        self.removed = []

    def all_tasks(self):
        return list(self.tasks.values()), False

    def tell_status_optional(self, gid):
        return self.tasks.get(gid)

    def remove_many(self, gids):
        self.removed.append(list(gids))
        return True


def make_adapter(tasks, raw_store=None, migration_callback=None):
    store = store_module.TaskStore(
        load_callback=lambda: raw_store or {},
        save_callback=lambda payload: None,
        default_downloader="aria-main",
        migration_callback=migration_callback,
    )
    config = config_module.PluginConfig.parse(
        {"enabled": True, "downloader_name": "aria-main"}
    )
    binding = config_module.DownloaderBinding(
        name="aria-main",
        type="aria2managerrehtt",
        default=True,
    )
    adapter = downloader_module.Aria2Downloader(
        plugin_id="Aria2ManagerRehtt",
        config=config,
        binding=binding,
        client=FakeClient(tasks),
        store=store,
        path_mapper=pathing.PathMapper(),
        allowed_roots_provider=lambda: ["/downloads"],
    )
    return adapter, store


class DownloaderContractTests(unittest.TestCase):
    def test_moviepilot_v2_list_signature_is_complete(self):
        parameters = list(
            inspect.signature(
                downloader_module.Aria2Downloader.list_torrents
            ).parameters
        )
        self.assertEqual(
            parameters,
            [
                "self",
                "status",
                "hashs",
                "downloader",
                "include_all_tags",
            ],
        )

    def test_default_list_only_returns_owned_tasks(self):
        owned_hash = "a" * 40
        external_hash = "b" * 40
        tasks = [
            {
                "gid": "owned-gid",
                "status": "complete",
                "infoHash": owned_hash,
                "totalLength": "10",
                "completedLength": "10",
                "dir": "/downloads",
                "files": [{"path": "/downloads/owned.mkv", "length": "10"}],
            },
            {
                "gid": "external-gid",
                "status": "active",
                "infoHash": external_hash,
                "totalLength": "10",
                "completedLength": "1",
                "dir": "/downloads",
                "files": [{"path": "/downloads/external.mkv", "length": "10"}],
            },
        ]
        raw_store = {
            "schema_version": 2,
            "records": {
                owned_hash: models.TaskRecord(
                    public_id=owned_hash,
                    root_gid="owned-gid",
                    effective_gid="owned-gid",
                    downloader="aria-main",
                ).to_dict()
            },
        }
        adapter, _ = make_adapter(tasks, raw_store)

        default_result = adapter.list_torrents()
        all_result = adapter.list_torrents(include_all_tags=True)
        transfer_result = adapter.list_torrents(status="transfer")

        self.assertEqual([item.hash for item in default_result], [owned_hash])
        self.assertEqual(
            {item.hash for item in all_result}, {owned_hash, external_hash}
        )
        self.assertEqual([item.hash for item in transfer_result], [owned_hash])
        self.assertIsNone(adapter.list_torrents(downloader="another"))

    def test_external_tag_record_is_not_duplicated(self):
        external_hash = "c" * 40
        tasks = [
            {
                "gid": "external-gid",
                "status": "active",
                "infoHash": external_hash,
                "totalLength": "10",
                "completedLength": "1",
                "dir": "/downloads",
                "files": [{"path": "/downloads/external.mkv", "length": "10"}],
            }
        ]
        adapter, _ = make_adapter(tasks)
        self.assertTrue(
            adapter.set_torrents_tag(
                external_hash, ["manual"], downloader="aria-main"
            )
        )
        result = adapter.list_torrents(include_all_tags=True)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].tags, "manual")

    def test_legacy_parent_child_is_migrated_to_real_info_hash(self):
        info_hash = "d" * 40
        tasks = [
            {
                "gid": "legacy-parent",
                "status": "complete",
                "followedBy": ["bt-child"],
                "files": [],
            },
            {
                "gid": "bt-child",
                "following": "legacy-parent",
                "status": "active",
                "infoHash": info_hash,
                "totalLength": "10",
                "completedLength": "2",
                "dir": "/downloads",
                "files": [{"path": "/downloads/show.mkv", "length": "10"}],
            },
        ]
        migrations = []
        adapter, store = make_adapter(
            tasks,
            raw_store={"legacy-parent": {"downloader": "aria-main"}},
            migration_callback=lambda *args: migrations.append(args),
        )
        result = adapter.list_torrents()

        self.assertEqual([item.hash for item in result], [info_hash])
        self.assertEqual(store.find("legacy-parent").public_id, info_hash)
        self.assertEqual(store.find(info_hash).effective_gid, "bt-child")
        self.assertEqual(len(migrations), 1)

    def test_unsafe_file_path_blocks_rpc_task_removal(self):
        info_hash = "e" * 40
        tasks = [
            {
                "gid": "owned-gid",
                "status": "complete",
                "infoHash": info_hash,
                "totalLength": "10",
                "completedLength": "10",
                "dir": "/outside",
                "files": [{"path": "/outside/video.mkv", "length": "10"}],
            }
        ]
        raw_store = {
            "schema_version": 2,
            "records": {
                info_hash: models.TaskRecord(
                    public_id=info_hash,
                    root_gid="owned-gid",
                    effective_gid="owned-gid",
                    downloader="aria-main",
                ).to_dict()
            },
        }
        adapter, store = make_adapter(tasks, raw_store)
        self.assertFalse(adapter.remove_torrents(info_hash, delete_file=True))
        self.assertEqual(adapter.client.removed, [])
        self.assertIsNotNone(store.find(info_hash))

    def test_bt_metadata_container_is_not_reported_as_transferable(self):
        info_hash = "1" * 40
        tasks = [
            {
                "gid": "metadata-gid",
                "status": "complete",
                "totalLength": "100",
                "completedLength": "100",
                "dir": "/downloads",
                "files": [
                    {
                        "path": "/downloads/metadata.torrent",
                        "length": "100",
                    }
                ],
            }
        ]
        raw_store = {
            "schema_version": 2,
            "records": {
                info_hash: models.TaskRecord(
                    public_id=info_hash,
                    root_gid="metadata-gid",
                    effective_gid="metadata-gid",
                    downloader="aria-main",
                    info_hash=info_hash,
                    bt_expected=True,
                ).to_dict()
            },
        }
        adapter, _ = make_adapter(tasks, raw_store)
        self.assertEqual(adapter.list_torrents(status="transfer"), [])


if __name__ == "__main__":
    unittest.main()

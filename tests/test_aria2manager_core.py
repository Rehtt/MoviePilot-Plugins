import base64
import unittest
from types import SimpleNamespace

from _aria2manager_loader import load_module


config_module = load_module("config")
models = load_module("models")
task_store_module = load_module("task_store")


class IdentityTests(unittest.TestCase):
    def test_magnet_hex_and_base32_hashes_are_normalized(self):
        expected = "0123456789abcdef0123456789abcdef01234567"
        encoded = base64.b32encode(bytes.fromhex(expected)).decode("ascii")

        self.assertEqual(
            models.magnet_info_hash(f"magnet:?xt=urn:btih:{expected.upper()}"),
            expected,
        )
        self.assertEqual(
            models.magnet_info_hash(f"magnet:?xt=urn:btih:{encoded}"),
            expected,
        )

    def test_proxy_public_id_is_stable_and_scoped(self):
        first = models.proxy_public_id("Plugin", "aria-main", "gid-1")
        self.assertRegex(first, r"^[0-9a-f]{40}$")
        self.assertEqual(
            first,
            models.proxy_public_id("Plugin", "aria-main", "gid-1"),
        )
        self.assertNotEqual(
            first,
            models.proxy_public_id("Plugin", "aria-backup", "gid-1"),
        )


class ConfigTests(unittest.TestCase):
    def test_invalid_values_are_reported_without_crashing(self):
        parsed = config_module.PluginConfig.parse(
            {
                "enabled": True,
                "rpc_url": "file:///tmp/aria.sock",
                "timeout": "bad",
                "monitor_interval": 3,
                "downloader_type": "Aria 2",
            }
        )
        self.assertFalse(parsed.valid)
        self.assertGreaterEqual(len(parsed.errors), 4)

    def test_binding_is_exact_and_preserves_path_mapping(self):
        config = config_module.PluginConfig.parse(
            {
                "downloader_type": "aria2managerrehtt",
                "downloader_name": "aria-main",
            }
        )
        downloader_configs = {
            "aria-main": SimpleNamespace(
                type="aria2managerrehtt",
                default=True,
                path_mapping=[("/downloads", "/data")],
            ),
            "aria-other": SimpleNamespace(
                type="aria2managerrehtt",
                default=False,
                path_mapping=[],
            ),
        }
        binding, error = config_module.resolve_downloader_binding(
            config, downloader_configs
        )
        self.assertIsNone(error)
        self.assertEqual(binding.name, "aria-main")
        self.assertTrue(binding.default)
        self.assertEqual(binding.path_mapping, (("/downloads", "/data"),))

    def test_multiple_bindings_require_an_explicit_name(self):
        config = config_module.PluginConfig.parse({})
        downloader_configs = {
            "one": SimpleNamespace(type="aria2managerrehtt"),
            "two": SimpleNamespace(type="aria2managerrehtt"),
        }
        binding, error = config_module.resolve_downloader_binding(
            config, downloader_configs
        )
        self.assertIsNone(binding)
        self.assertIn("多个", error)


class TaskStoreTests(unittest.TestCase):
    def test_legacy_data_is_loaded_lazily_then_migrated_atomically(self):
        saved = []
        migrations = []
        raw = {
            "legacy-gid": {
                "downloader": "aria-main",
                "tags": ["MOVIEPILOT"],
                "category": "tv",
            }
        }
        store = task_store_module.TaskStore(
            load_callback=lambda: raw,
            save_callback=saved.append,
            default_downloader="aria-main",
            migration_callback=lambda *args: migrations.append(args),
        )

        legacy = store.find("legacy-gid")
        self.assertTrue(legacy.migration_pending)
        self.assertEqual(saved, [])

        new_hash = "a" * 40
        migrated = store.migrate(
            "legacy-gid",
            new_hash,
            new_hash,
            ["legacy-gid", "child-gid"],
            "child-gid",
        )
        self.assertEqual(migrated.public_id, new_hash)
        self.assertFalse(migrated.migration_pending)
        self.assertIsNone(store.find("missing"))
        self.assertEqual(store.find("legacy-gid").public_id, new_hash)
        self.assertEqual(len(migrations), 1)
        payload, old_id, public_id, downloader = migrations[0]
        self.assertEqual((old_id, public_id, downloader), (
            "legacy-gid",
            new_hash,
            "aria-main",
        ))
        self.assertIn(new_hash, payload["records"])

    def test_failed_atomic_migration_does_not_change_memory(self):
        store = task_store_module.TaskStore(
            load_callback=lambda: {"legacy": {}},
            save_callback=lambda value: None,
            default_downloader="aria-main",
            migration_callback=lambda *args: (_ for _ in ()).throw(
                RuntimeError("rollback")
            ),
        )
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            store.migrate("legacy", "b" * 40, "b" * 40, ["legacy"], "legacy")
        self.assertEqual(store.find("legacy").public_id, "legacy")
        self.assertTrue(store.find("legacy").migration_pending)

    def test_unchanged_upsert_does_not_write_again(self):
        saved = []
        store = task_store_module.TaskStore(
            load_callback=lambda: {},
            save_callback=saved.append,
            default_downloader="aria-main",
        )
        record = models.TaskRecord(
            public_id="c" * 40,
            root_gid="gid",
            effective_gid="gid",
            downloader="aria-main",
        )
        stored = store.upsert(record)
        store.upsert(stored)
        self.assertEqual(len(saved), 1)

    def test_current_file_paths_replace_stale_paths(self):
        store = task_store_module.TaskStore(
            load_callback=lambda: {},
            save_callback=lambda payload: None,
            default_downloader="aria-main",
        )
        record = models.TaskRecord(
            public_id="f" * 40,
            root_gid="gid",
            effective_gid="gid",
            downloader="aria-main",
            file_paths=["/downloads/old/video.mkv"],
        )
        store.upsert(record)
        record.file_paths = ["/downloads/current/video.mkv"]
        updated = store.upsert(record)
        self.assertEqual(
            updated.file_paths, ["/downloads/current/video.mkv"]
        )


if __name__ == "__main__":
    unittest.main()

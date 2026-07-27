import importlib
import sys
import types
from pathlib import Path


PACKAGE_NAME = "_aria2managerrehtt_under_test"
PLUGIN_DIR = (
    Path(__file__).resolve().parents[1]
    / "plugins.v2"
    / "aria2managerrehtt"
)


def install_package() -> None:
    if PACKAGE_NAME in sys.modules:
        return
    package = types.ModuleType(PACKAGE_NAME)
    package.__path__ = [PLUGIN_DIR.as_posix()]
    package.__package__ = PACKAGE_NAME
    sys.modules[PACKAGE_NAME] = package


def load_module(name: str):
    install_package()
    return importlib.import_module(f"{PACKAGE_NAME}.{name}")


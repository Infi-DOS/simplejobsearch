from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import ModuleType

from .config import get_settings


def load_legacy(filename: str) -> ModuleType:
    path = get_settings().project_root / filename
    if not path.exists():
        raise ImportError(f"Compatibility implementation not found: {path}")
    name = f"_simplejobsearch_legacy_{Path(filename).stem}"
    cached = sys.modules.get(name)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load compatibility implementation: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def export_legacy(filename: str, namespace: dict) -> None:
    module = load_legacy(filename)
    namespace.update(
        {
            name: value
            for name, value in vars(module).items()
            if name not in {"__name__", "__file__", "__package__", "__loader__", "__spec__"}
        }
    )


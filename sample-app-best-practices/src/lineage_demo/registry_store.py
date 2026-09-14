"""Persistence adapters for the mock registry.

The in-cluster implementation deliberately uses a ConfigMap so the learning app
has no hidden database.  It is suitable for one replica and small demonstrations,
not a production registry.
"""

from __future__ import annotations

import json
import os
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

from lineage_demo.config import Settings

DATA_KEY = "assets.json"


class RegistryStore(ABC):
    @abstractmethod
    def read(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def write(self, state: dict[str, Any]) -> None:
        raise NotImplementedError


class FileRegistryStore(RegistryStore):
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    def read(self) -> dict[str, Any]:
        with self._lock:
            if not self.path.exists():
                return {"assets": {}, "idempotencyKeys": {}}
            return json.loads(self.path.read_text(encoding="utf-8"))

    def write(self, state: dict[str, Any]) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
            os.replace(temporary, self.path)


class ConfigMapRegistryStore(RegistryStore):
    def __init__(self, *, namespace: str, name: str) -> None:
        config.load_incluster_config()
        self.api = client.CoreV1Api()
        self.namespace = namespace
        self.name = name
        self._lock = threading.RLock()

    def read(self) -> dict[str, Any]:
        with self._lock:
            config_map = self.api.read_namespaced_config_map(self.name, self.namespace)
            payload = (config_map.data or {}).get(DATA_KEY, "")
            return json.loads(payload) if payload else {"assets": {}, "idempotencyKeys": {}}

    def write(self, state: dict[str, Any]) -> None:
        payload = json.dumps(state, separators=(",", ":"), sort_keys=True)
        with self._lock:
            for attempt in range(3):
                current = self.api.read_namespaced_config_map(self.name, self.namespace)
                current.data = dict(current.data or {})
                current.data[DATA_KEY] = payload
                try:
                    self.api.replace_namespaced_config_map(self.name, self.namespace, current)
                    return
                except ApiException as exc:
                    if exc.status != 409 or attempt == 2:
                        raise


def build_store(settings: Settings) -> RegistryStore:
    if settings.registry_store_backend == "file":
        return FileRegistryStore(settings.registry_store_path)
    if settings.registry_store_backend == "configmap":
        return ConfigMapRegistryStore(
            namespace=settings.project_namespace,
            name=settings.registry_configmap_name,
        )
    raise ValueError(f"Unsupported registry store backend: {settings.registry_store_backend}")

"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)

    project_namespace: str = "ol-best-practices"
    cluster_name: str = "demo-cluster"
    registry_url: str = "http://registry:8080"
    marquez_url: str = "http://marquez:80"
    s3_endpoint: str = "http://minio:9000"
    s3_region: str = "us-east-1"
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    registry_store_backend: str = "configmap"
    registry_store_path: str = "/tmp/registry-assets.json"
    registry_configmap_name: str = "registry-data"
    lineage_emit_attempts: int = 3
    lineage_emit_timeout_seconds: float = 5.0
    lineage_emit_backoff_seconds: float = 0.5

    @property
    def registry_namespace(self) -> str:
        return f"dataregistry://{self.cluster_name}/{self.project_namespace}"

    @property
    def kfp_namespace(self) -> str:
        return f"kfp://{self.cluster_name}/{self.project_namespace}"

    @property
    def ingest_namespace(self) -> str:
        return f"dch-mock://{self.cluster_name}/{self.project_namespace}"

    @property
    def spark_namespace(self) -> str:
        return f"spark://{self.cluster_name}/{self.project_namespace}"


@lru_cache
def get_settings() -> Settings:
    return Settings()

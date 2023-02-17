from functools import lru_cache

from kedro.framework.project import pipelines

from .config import PluginConfig
from .task_group import TaskGroupFactory

CONFIG_FILE_PATTERN = "airflow-k8s*"


class ContextHelper(object):
    def __init__(self, metadata, env, pipeline_name):
        self._metadata = metadata
        self._env = env
        self._session = None
        self._pipeline_name = pipeline_name

    @property
    def env(self):
        return self._env

    @property
    def project_name(self):
        return self._metadata.project_name

    @property
    def source_dir(self):
        return self._metadata.source_dir

    @property
    def context(self):
        return self.session.load_context()

    @property
    def pipeline(self):
        return pipelines.get(self._pipeline_name)

    @property
    def catalog(self):
        return self.context.catalog

    @property
    def pipeline_grouped(self):
        return TaskGroupFactory().create(self.pipeline, self.context.catalog)

    @property
    def pipeline_name(self):
        return self._pipeline_name

    @property
    def session(self):
        from kedro.framework.session import KedroSession

        if self._session is None:
            self._session = KedroSession.create(
                self._metadata.package_name, env=self._env
            )

        return self._session

    @property
    @lru_cache()
    def config(self) -> PluginConfig:
        raw = self.context.config_loader.get(CONFIG_FILE_PATTERN)
        return PluginConfig(raw)

    @property
    @lru_cache()
    def mlflow_config(self):
        return self.context.config_loader.get("mlflow*")

    @staticmethod
    def init(metadata, env, pipeline_name="__default__"):
        return ContextHelper(metadata, env, pipeline_name)

import os
import unittest
from contextlib import contextmanager

from kedro_airflow_k8s.default_config_loader import KedroAirflowK8sConfigLoader


@contextmanager
def environment(env):
    original_environ = os.environ.copy()
    os.environ.update(env)
    yield
    os.environ = original_environ


class TestRegisterTemplatedConfigLoaderHook(unittest.TestCase):
    @staticmethod
    def get_config():
        config_path = os.path.dirname(os.path.abspath(__file__))
        loader = KedroAirflowK8sConfigLoader(conf_source=config_path)
        return loader.get("test_config.yml")

    def test_loader_with_defaults(self):
        config = self.get_config()
        assert config["run_config"]["image"] == "gcr.io/project-image/dirty"
        assert config["run_config"]["experiment_name"] == "[Test] local"
        assert config["run_config"]["run_name"] == "dirty"

    def test_loader_with_env(self):
        with environment(
            {
                "KEDRO_CONFIG_COMMIT_ID": "123abc",
                "KEDRO_CONFIG_BRANCH_NAME": "feature-1",
                "KEDRO_CONFIG_XYZ123": "123abc",
            }
        ):
            config = self.get_config()

        assert config["run_config"]["image"] == "gcr.io/project-image/123abc"
        assert config["run_config"]["experiment_name"] == "[Test] feature-1"
        assert config["run_config"]["run_name"] == "123abc"

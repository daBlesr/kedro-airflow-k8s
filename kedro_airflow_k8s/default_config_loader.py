import os
from typing import Dict

from kedro.config import TemplatedConfigLoader

VAR_PREFIX = "KEDRO_CONFIG_"

# defaults provided so default variables ${commit_id|dirty} work for some entries
ENV_DEFAULTS = {"commit_id": None, "branch_name": None}


def read_env_variables() -> Dict:
    config = ENV_DEFAULTS.copy()
    overrides = dict(
        [
            (k.replace(VAR_PREFIX, "").lower(), v)
            for k, v in os.environ.copy().items()
            if k.startswith(VAR_PREFIX)
        ]
    )
    config.update(**overrides)
    return config


class KedroAirflowK8sConfigLoader(TemplatedConfigLoader):
    """
    In order to compile airflow-k8s.yml, some parameters need to be passed on runtime.
    This class converts environment variables prefixed with KEDRO_CONFIG_X into usable
    variable named `x`, and can be used as `${x}` accordingly in the Data Catalog.

    To use this functionality, specify in settings.py
    `CONFIG_LOADER_CLASS = KedroAirflowK8sConfigLoader`
    """

    def __init__(self, conf_source: str, *args, **kwargs):
        super().__init__(conf_source, *args, **kwargs, globals_dict=read_env_variables())


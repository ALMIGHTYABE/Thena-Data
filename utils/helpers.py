import yaml
from typing import Any, Dict

def read_params(config_path: str) -> Dict[str, Any]:
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config
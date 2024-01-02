import pathlib
from os import path
from unittest.mock import Mock

from elena.adapters.config.local_config_manager import LocalConfigManager
from elena.shared.dynamic_loading import get_instance


def test_s3_storage_manager():
    test_home_dir = path.join(pathlib.Path(__file__).parent.parent.parent, "test_home")
    config_manager = LocalConfigManager()
    config_manager.init(url=test_home_dir)
    config = config_manager.get_config()

    logger = Mock()

    sut = get_instance(config["StorageManager"]["class"])
    sut.init(config, logger)

    assert (
        logger.mock_calls[0]._get_call_arguments()[0][0]
        == "Starting S3StorageManager ..."
    )

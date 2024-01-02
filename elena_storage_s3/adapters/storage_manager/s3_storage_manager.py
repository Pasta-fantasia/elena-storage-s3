from typing import Any

from elena.adapters.storage_manager.file_storage_manager import FileStorageManager
from elena.domain.ports.logger import Logger


class S3StorageManager(FileStorageManager):


    def init(self, config: dict, logger: Logger):
        self._logger = logger
        self._logger.info("Starting S3StorageManager ...")

    def _get_filepath(self, data_id: str, class_name: str) -> str:
        ...

    def _load_file(self, filepath: str) -> str:
        ...

    def _save_file(self, filepath: str, json_data: str):
        ...

    def _delete_file(self, filepath: str):
        ...
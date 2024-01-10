from typing import Any, Dict, Optional

import boto3
from elena.adapters.storage_manager.file_storage_manager import FileStorageManager
from elena.domain.ports.logger import Logger
import boto3
from elena.domain.ports.storage_manager import StorageError


class S3StorageManager(FileStorageManager):
    _config: Dict
    _s3: Optional[Any] = None
    _bucket_name: Optional[str] = None

    def init(self, config: dict, logger: Logger):
        self._logger = logger
        self._config = config
        session = boto3.Session(
            aws_access_key_id=self._config["StorageManager"]["access_key_id"],
            aws_secret_access_key=self._config["StorageManager"]["secret_access_key"],
            region_name=self._config["StorageManager"]["region_name"],
        )
        self._s3 = session.resource('s3')
        self._bucket_name = self._config["StorageManager"]["bucket_name"]

        for bucket in self._s3.buckets.all():
            if bucket.name == self._bucket_name:
                self._logger.info("Started S3 storage manager, working with bucket %s", self._bucket_name)
                return
        raise StorageError(f'Bucket {self._bucket_name} not found')

    def _get_filepath(self, data_id: str, class_name: str) -> str:
        return f"{class_name}/{data_id}.json"

    def _load_file(self, filepath: str) -> str:
        obj = self._s3.Object(self._bucket_name, filepath)
        file_content = obj.get()['Body'].read().decode('UTF-8')
        return file_content

    def _save_file(self, filepath: str, json_data: str):
        obj = self._s3.Object(self._bucket_name, filepath)
        obj.put(Body=json_data.encode('UTF-8'))

    def _delete_file(self, filepath: str):
        obj = self._s3.Object(self._bucket_name, filepath)
        obj.delete()
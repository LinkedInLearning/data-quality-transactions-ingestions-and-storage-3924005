from minio import Minio


class MinioConfig:
    """Configuration and connection management for MinIO."""

    def __init__(self):
        """Initialize database configuration with connection parameters."""

        self.minio_config = {
            'endpoint': 'minio:9000',
            'access_key': 'minioadmin',
            'secret_key': 'minioadmin',
            'secure': False
        }

    def connect_minio(self):
        """Create and return a MinIO client.

        Returns:
            Minio: MinIO client object
        """
        client = Minio(
            endpoint=self.minio_config['endpoint'],
            access_key=self.minio_config['access_key'],
            secret_key=self.minio_config['secret_key'],
            secure=self.minio_config['secure']
        )
        return client

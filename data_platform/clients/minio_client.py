from data_platform.config.minio_config import MinioConfig


class MinioClient:
    def __init__(self):
        self.config = MinioConfig()

    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        """Upload a file to MinIO, creating bucket if needed."""
        client = self.config.connect_minio()

        # Check if bucket exists, create if not
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        client.fput_object(bucket_name, object_name, file_path)

    def list_objects(self, bucket_name: str):
        """List objects in a bucket."""
        client = self.config.connect_minio()
        objects = client.list_objects(bucket_name, recursive=True)
        return [obj.object_name for obj in objects]

    def create_bucket(self, bucket_name: str):
        """Create a new bucket."""
        client = self.config.connect_minio()
        client.make_bucket(bucket_name)

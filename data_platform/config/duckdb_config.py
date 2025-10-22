class DuckdbConfig:
    def __init__(self):
        endpoint = 'minio:9000'
        access_key = 'minioadmin'
        secret_key = 'minioadmin'
        secure = False

        self.connection_string = f"""
        SET s3_endpoint='{endpoint}';
        SET s3_access_key_id='{access_key}';
        SET s3_secret_access_key='{secret_key}';
        SET s3_region='us-east-1';
        SET s3_url_style='path';
        SET s3_use_ssl={str(secure).lower()};
        """

import logging
from typing import Optional
import boto3
from botocore.exceptions import ClientError
from app.core.config import settings

logger = logging.getLogger(__name__)


class StorageService:
    def __init__(self):
        self._s3_client: Optional[boto3.client] = None
        logger.info("StorageService initialized (lazy client)")

    @property
    def s3_client(self):
        if self._s3_client is None:
            try:
                if settings.AWS_ACCESS_KEY_ID and settings.AWS_SECRET_ACCESS_KEY:
                    self._s3_client = boto3.client(
                        's3',
                        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                        region_name=settings.AWS_REGION,
                    )
                    logger.info("S3 client initialized successfully")
                else:
                    logger.warning("AWS credentials not configured")
            except Exception as e:
                logger.error(f"Failed to initialize S3 client: {e}")
        return self._s3_client

    async def upload_file(self, file_bytes: bytes, key: str) -> str:
        """Upload file to S3 and return URL"""
        try:
            if not self.s3_client:
                raise ValueError("S3 client not initialized")
            
            self.s3_client.put_object(
                Bucket=settings.AWS_S3_BUCKET,
                Key=key,
                Body=file_bytes,
            )
            
            url = f"https://{settings.AWS_S3_BUCKET}.s3.{settings.AWS_REGION}.amazonaws.com/{key}"
            return url
        except ClientError as e:
            logger.error(f"S3 upload failed: {e}")
            raise

    async def delete_file(self, key: str) -> bool:
        """Delete file from S3"""
        try:
            if not self.s3_client:
                return False
            
            self.s3_client.delete_object(
                Bucket=settings.AWS_S3_BUCKET,
                Key=key,
            )
            return True
        except ClientError as e:
            logger.error(f"S3 delete failed: {e}")
            return False


storage_service = StorageService()
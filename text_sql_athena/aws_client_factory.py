import boto3
from botocore.config import Config
from .custom_logger import logger


# Provide boto3 client for various AWS Services
class AwsClientFactory:
    def __init__(self, region_name="us-east-1", max_attempts=10):
        self.retry_config = Config(
            region_name = region_name,
            retries = {
                'max_attempts': max_attempts,
                'mode': 'standard'
            }               
        )
        self.session = boto3.session.Session()
        self._bedrock_client = None
        self._bedrock_runtime_client = None
        self._athena_client = None
        self._s3_client = None
        self._glue_client = None


    def createBedrockClient(self):
        if self._bedrock_client is None:
            self._bedrock_client = self.session.client('bedrock',config=self.retry_config)
            logger.debug(f'bedrock client created for profile')
        return self._bedrock_client
    
    def createBedrockRuntimeClient(self):
        if self._bedrock_runtime_client is None:
            self._bedrock_runtime_client = self.session.client('bedrock-runtime',config=self.retry_config)
            logger.debug(f'bedrock runtime client created ')
        return self._bedrock_runtime_client
    
    def createAthenaClient(self):
        if self._athena_client is None:
            self._athena_client = self.session.client('athena',config=self.retry_config)
            logger.debug(f'athena client created ')
        return self._athena_client
    
    def createS3Client(self):
        if self._s3_client is None:
            self._s3_client = self.session.client('s3',config=self.retry_config)
            logger.debug(f's3 client created !!')
        return self._s3_client

    def createGlueClient(self):
        if self._glue_client is None:
            self._glue_client = self.session.client('glue',config=self.retry_config)
            logger.debug(f'Glue client created !!')
        return self._glue_client    

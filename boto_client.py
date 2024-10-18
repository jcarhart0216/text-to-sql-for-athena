
import boto3                                    # Imports the AWS SDK for Python, which allows interaction with AWS services
from botocore.config import Config              # Imports the Config class for setting configurations like retries and timeouts
import logging                                  # Enables logging of messages for debugging and tracking program execution

# Sets up a logger to record debug-level messages and outputs them to the console
logger = logging.getLogger(__name__)            # Creates a logger with the name of the current module
logger.setLevel(logging.DEBUG)                  # Sets the logger to capture all levels of messages at and above DEBUG
logger.addHandler(logging.StreamHandler())      # Adds a handler to output log messages to the console


# Defines a retry configuration for AWS clients to handle transient errors and throttling
retry_config = Config(
        region_name = 'us-east-1',
        retries = {
            'max_attempts': 10,
            'mode': 'standard'
        }
)


# Define a class to encapsulate methods for creating AWS service clients
class Clientmodules():
    def __init__(self):
        pass

    # Creates a client for AWS Bedrock service
    def createBedrockClient():
        session = boto3.session.Session()                                   # Starts a new session with AWS
        bedrock_client = session.client('bedrock', config=retry_config)     # Creates a Bedrock client with retry config
        logger.info(f'bedrock client created for profile')                  # Logs the creation of the Bedrock client
        return bedrock_client                                               # Returns the Bedrock client

    # Creates a client for AWS Bedrock Runtime service
    def createBedrockRuntimeClient():
        session = boto3.session.Session()
        bedrock_runtime_client = session.client('bedrock-runtime', config=retry_config)
        logger.info(f'bedrock runtime client created ')
        return bedrock_runtime_client

    # Creates a client for AWS Athena service
    def createAthenaClient():
        session = boto3.session.Session()
        athena_client = session.client('athena', config=retry_config)
        logger.info(f'athena client created ')
        return athena_client

    def createS3Client():
        session = boto3.session.Session()
        s3_client = session.client('s3', config=retry_config)
        logger.info(f's3 client created !!')
        return s3_client
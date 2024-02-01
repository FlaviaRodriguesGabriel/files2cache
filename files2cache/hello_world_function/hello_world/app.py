from schema.aws.events.scheduledjson import Marshaller
from schema.aws.events.scheduledjson import AWSEvent
from schema.aws.events.scheduledjson import ScheduledEvent
import boto3
import json
import logging
import os
from rediscluster import RedisCluster

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Sample Lambda function reacting to EventBridge events

    Parameters
    ----------
    event: dict, required
        Event Bridge Events Format

        Event doc: https://docs.aws.amazon.com/eventbridge/latest/userguide/event-types.html

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
        The same input event file
    """

    #Deserialize event into strongly typed object
    awsEvent:AWSEvent = Marshaller.unmarshall(event, AWSEvent)
    detail:ScheduledEvent = awsEvent.detail

    #Execute business logic
    logger.info("Full event: %s", event)

    if "time" in event:
        process_event(event)

    #Return event for further processing
    return Marshaller.marshall(awsEvent)


def process_event(event):
    s3_client, s3_bucket = connect_s3()
    process_files(s3_client, s3_bucket)


def load_environment_variables(file_path=".env"):
    """
    Load environment variables from a file.

    Parameters:
    - file_path (str): Path to the environment file (default is ".env").
    """
    load_environment_variables()

def connect_s3():
    """
    Connect to an S3 bucket and return the S3 client and the bucket object.

    Returns:
    - s3_client (boto3.client): S3 client.
    - s3_bucket (boto3.resource): S3 bucket resource.
    """
    # Load environment variables from the .env file
    load_environment_variables()

    # Retrieve AWS credentials and S3 bucket information from environment variables
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    # Ensure all required variables are present
    if not all([access_key, secret_key, region, bucket_name]):
        raise ValueError("Missing one or more required environment variables.")

    # Create an S3 client
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)

    # Create an S3 bucket resource
    s3_resource = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)

    # Reference to the S3 bucket
    s3_bucket = s3_resource.Bucket(bucket_name)

    return s3_client, s3_bucket


def process_files(s3_client, s3_bucket):
    """
    Process files in S3 folders 'qa', 'cw3', and 'eq3' if they exist.

    Parameters:
    - s3_client (boto3.client): S3 client.
    - s3_bucket (boto3.resource.Bucket): S3 bucket resource.
    """
    folders_to_check = ['qa', 'cw3', 'eq3']

    # Check if all folders exist
    if all(s3_bucket.objects.filter(Prefix=folder).limit(1) for folder in folders_to_check):

        # Invoke processing methods for each folder
        logger.info("All folders exist. Initiating processing.")
        keys_qa = process_qa(s3_client, s3_bucket)
        # keys_cw3 = process_cw3(s3_client, s3_bucket)
        # keys_eq3 = process_eq3(s3_client, s3_bucket)
        logger.info("Folders processing completed.")

        # Loads do Cache - Redis
        load_qa(keys_qa)
        # load_cw3(keys_cw3)
        # load_eq3(keys_eq3)
        logger.info("All keys created in Redis.")


    else:
        raise ValueError("One or more folders do not exist. Aborting processing.")

def process_qa(s3_client, s3_bucket):
    """
    Process files in the 'qa' folder.

    Parameters:
    - s3_client (boto3.client): S3 client.
    - s3_bucket (boto3.resource.Bucket): S3 bucket resource.
    """
    logger.debug("Starting process QA files.")

    # Environment property containing a list of JSON file names
    json_files_env = os.getenv("QA_JSON_FILES")
    
    # Check if the 'QA_JSON_FILES' environment variable is defined
    if not json_files_env:
        raise ValueError("Environment variable 'QA_JSON_FILES' is not defined.")

    # Split the environment property into a list of JSON file names
    json_files_list = json_files_env.split(',')

    # Check if there are any files in the 'qa' folder
    qa_folder_prefix = 'qa/'
    objects_in_qa = list(s3_bucket.objects.filter(Prefix=qa_folder_prefix))

    if not objects_in_qa:
        raise FileNotFoundError(f"No files found in the '{qa_folder_prefix}' folder.")

    # Iterate through each JSON file and create variables
    for json_file in json_files_list:
        # Construct the S3 key for the JSON file in the 'qa' folder
        s3_key = f'{qa_folder_prefix}{json_file}'

        # Read the content of the JSON file from S3
        try:
            response = s3_client.get_object(Bucket=s3_bucket.name, Key=s3_key)
            json_content = json.loads(response['Body'].read().decode('utf-8'))

            # Create a variable with the same name as the JSON file (without extension)
            qa_variable_name = os.path.splitext(json_file)[0]

            # Get codigo_valor_dominio from valores_dominio list
            locals()[qa_variable_name] = [item['codigo_valor_dominio'] for item in json_content.get('valores_dominio', [])]

            logger.info(f"Variable '{qa_variable_name}' created with content from '{s3_key}'.")    
        except Exception as e:
            logger.error(f"Error processing '{s3_key}': {str(e)}")
    
    logger.debug("Finished to process QA files.")
    return [qa_variable_name]
    

# def process_cw3(s3_client, s3_bucket):
#     """
#     Process files in the 'cw3' folder.

#     Parameters:
#     - s3_client (boto3.client): S3 client.
#     - s3_bucket (boto3.resource.Bucket): S3 bucket resource.
#     """
#     print("Processing 'cw3' folder...")
#     # Add your logic for processing 'cw3' folder here
#     # ...

# def process_eq3(s3_client, s3_bucket):
#     """
#     Process files in the 'eq3' folder.

#     Parameters:
#     - s3_client (boto3.client): S3 client.
#     - s3_bucket (boto3.resource.Bucket): S3 bucket resource.
#     """
#     print("Processing 'eq3' folder...")
#     # Add your logic for processing 'eq3' folder here
#     # ...


def load_qa(keys_qa):
    """
    Loads QA keys to Redis.

    Parameters:
    - keys_qa (boto3.client): S3 client.
    """
    logger.debug("Starting process QA files.")

    # Environment property containing a list of JSON file names
    json_files_env = os.getenv("QA_JSON_FILES")
